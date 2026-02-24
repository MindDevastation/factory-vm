from __future__ import annotations

import json
import os
import socket
from typing import Any, Dict, Optional

from services.common.env import Env
from services.common import db as dbm
from services.common.config import load_policies
from services.common.ffmpeg import ffprobe_json, parse_fps, volumedetect
from services.common.paths import outbox_dir, qa_path, cancel_flag_path
from services.common.logging_setup import get_logger


log = get_logger("qa")


def _safe_float(v: object) -> Optional[float]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if not s or s.lower() == "n/a":
            return None
        return float(s)
    except Exception:
        return None


def qa_cycle(*, env: Env, worker_id: str) -> None:
    conn = dbm.connect(env)
    job_id: Optional[int] = None
    try:
        dbm.migrate(conn)

        dbm.touch_worker(
            conn,
            worker_id=worker_id,
            role="qa",
            pid=os.getpid(),
            hostname=socket.gethostname(),
            details={"state": "idle"},
        )

        job_id = dbm.claim_job(
            conn,
            want_state="QA_RUNNING",
            worker_id=worker_id,
            lock_ttl_sec=env.job_lock_ttl_sec,
        )
        if not job_id:
            return

        job = dbm.get_job(conn, job_id)
        if not job:
            dbm.release_lock(conn, job_id, worker_id)
            return

        if str(job.get("state") or "") == "CANCELLED":
            # Job was cancelled while waiting for QA.
            dbm.release_lock(conn, job_id, worker_id)
            return

        try:
            if cancel_flag_path(env, job_id).exists():
                dbm.cancel_job(conn, job_id, reason="cancelled by user")
                dbm.release_lock(conn, job_id, worker_id)
                return
        except Exception:
            pass

            dbm.release_lock(conn, job_id, worker_id)
            return

        mp4 = outbox_dir(env, job_id) / "render.mp4"
        if not mp4.exists():
            dbm.update_job_state(conn, job_id, state="QA_FAILED", stage="QA", error_reason="missing mp4")
            dbm.release_lock(conn, job_id, worker_id)
            return

        policies = load_policies("configs/policies.yaml").raw
        qa_cfg = policies.get("qa_policy", {})
        warn_blocks = bool(qa_cfg.get("warning_blocks_pipeline", True))

        expected = conn.execute(
            """
            SELECT rp.video_w, rp.video_h, rp.fps, rp.vcodec_required,
                   rp.audio_sr, rp.audio_ch, rp.acodec_required
            FROM jobs j
            JOIN releases r ON r.id = j.release_id
            JOIN channels c ON c.id = r.channel_id
            JOIN render_profiles rp ON rp.name = c.render_profile
            WHERE j.id = ?
            """,
            (job_id,),
        ).fetchone()

        report: Dict[str, Any] = {"hard_ok": True, "warnings": [], "info": []}

        # ffprobe
        try:
            probe = ffprobe_json(mp4)
        except Exception as e:
            report["hard_ok"] = False
            report["warnings"].append(f"ffprobe_failed: {e}")
            _write_report(env, job_id, report)
            dbm.set_qa_report(conn, job_id, report)
            dbm.update_job_state(conn, job_id, state="QA_FAILED", stage="QA", error_reason="ffprobe failed")
            dbm.release_lock(conn, job_id, worker_id)
            return

        v_stream = None
        a_stream = None
        for s in probe.get("streams", []):
            if s.get("codec_type") == "video" and v_stream is None:
                v_stream = s
            if s.get("codec_type") == "audio" and a_stream is None:
                a_stream = s

        if v_stream is None or a_stream is None:
            report["hard_ok"] = False
            report["warnings"].append("missing video or audio stream")

        # durations
        dur_v = _safe_float(v_stream.get("duration")) if v_stream else None
        dur_a = _safe_float(a_stream.get("duration")) if a_stream else None
        report["duration_actual"] = dur_v or dur_a

        if dur_v and dur_a:
            if abs(dur_v - dur_a) > float(qa_cfg.get("duration_diff_hard_fail_sec", 2.0)):
                report["hard_ok"] = False
                report["warnings"].append(f"duration_mismatch: v={dur_v:.2f} a={dur_a:.2f}")

        # video params
        if v_stream:
            fps = parse_fps(v_stream)
            width = int(v_stream.get("width") or 0)
            height = int(v_stream.get("height") or 0)
            vcodec = str(v_stream.get("codec_name") or "")
            report.update({"fps": fps, "width": width, "height": height, "vcodec": vcodec})

            fps_target = float(expected["fps"]) if expected else float(qa_cfg.get("video", {}).get("fps_target", 24))
            fps_tol = float(qa_cfg.get("video", {}).get("fps_tolerance", 0.5))
            if fps is None or abs(fps - fps_target) > fps_tol:
                report["warnings"].append(f"fps_not_{fps_target}: {fps}")

            if expected:
                if width != int(expected["video_w"]) or height != int(expected["video_h"]):
                    report["warnings"].append(f"resolution_mismatch: {width}x{height} expected {expected['video_w']}x{expected['video_h']}")
                if vcodec != str(expected["vcodec_required"]):
                    report["warnings"].append(f"vcodec_not_{expected['vcodec_required']}: {vcodec}")
            else:
                req = qa_cfg.get("video", {}).get("require_codec")
                if req and vcodec != req:
                    report["warnings"].append(f"vcodec_not_{req}: {vcodec}")

        # audio params
        if a_stream:
            acodec = str(a_stream.get("codec_name") or "")
            sr = int(a_stream.get("sample_rate") or 0)
            chn = int(a_stream.get("channels") or 0)
            report.update({"acodec": acodec, "sr": sr, "ch": chn})

            if expected:
                if acodec != str(expected["acodec_required"]):
                    report["warnings"].append(f"acodec_not_{expected['acodec_required']}: {acodec}")
                if sr != int(expected["audio_sr"]):
                    report["warnings"].append(f"sr_not_{expected['audio_sr']}: {sr}")
                if chn != int(expected["audio_ch"]):
                    report["warnings"].append(f"ch_not_{expected['audio_ch']}: {chn}")
            else:
                req_a = qa_cfg.get("audio", {}).get("require_codec")
                if req_a and acodec != req_a:
                    report["warnings"].append(f"acodec_not_{req_a}: {acodec}")
                if sr != int(qa_cfg.get("audio", {}).get("require_sample_rate", 48000)):
                    report["warnings"].append(f"sr_not_{qa_cfg['audio']['require_sample_rate']}: {sr}")
                if chn != int(qa_cfg.get("audio", {}).get("require_channels", 2)):
                    report["warnings"].append(f"ch_not_{qa_cfg['audio']['require_channels']}: {chn}")

        # loudness (limited seconds)
        try:
            mean_db, max_db, _ = volumedetect(mp4, seconds=env.qa_volumedetect_seconds)
        except Exception as e:
            mean_db, max_db = None, None
            report["warnings"].append(f"volumedetect_failed: {e}")
        report["mean_volume_db"] = mean_db
        report["max_volume_db"] = max_db

        loud = qa_cfg.get("loudness", {})
        if max_db is not None and max_db >= float(loud.get("warn_if_max_volume_gte_db", -0.1)):
            report["warnings"].append(f"max_volume_gte: {max_db} dB")
        if mean_db is not None and mean_db > float(loud.get("warn_if_mean_volume_gt_db", -10.0)):
            report["warnings"].append(f"mean_volume_too_high: {mean_db} dB")
        if mean_db is not None and mean_db < float(loud.get("warn_if_mean_volume_lt_db", -55.0)):
            report["warnings"].append(f"mean_volume_too_low: {mean_db} dB")

        _write_report(env, job_id, report)
        dbm.set_qa_report(conn, job_id, report)

        if not report["hard_ok"] or (warn_blocks and report["warnings"]):
            dbm.update_job_state(conn, job_id, state="QA_FAILED", stage="QA", error_reason="QA blocked")
        else:
            dbm.update_job_state(conn, job_id, state="UPLOADING", stage="UPLOAD", progress_text="qa ok")

        dbm.release_lock(conn, job_id, worker_id)

    except Exception as e:
        log.exception("qa_cycle crashed: %s", e)
        if job_id is not None:
            try:
                dbm.update_job_state(conn, job_id, state="QA_FAILED", stage="QA", error_reason=f"qa exception: {e}")
                dbm.release_lock(conn, job_id, worker_id)
            except Exception:
                pass
    finally:
        conn.close()


def _write_report(env: Env, job_id: int, report: Dict[str, Any]) -> None:
    p = qa_path(env, job_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
