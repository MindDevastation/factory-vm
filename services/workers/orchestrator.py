from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

from services.common.env import Env
from services.common import db as dbm
from services.common.config import load_policies
from services.common.logging_setup import append_job_log, get_logger, safe_path_basename
from services.common.paths import workspace_dir, outbox_dir, preview_path, cancel_flag_path
from services.common.utils import safe_slug
from services.common.ffmpeg import make_preview_60s
from services.integrations.gdrive import DriveClient


log = get_logger("orchestrator")


def _parse_progress_pct(line: str) -> Optional[float]:
    """Parse a progress percentage from a renderer log line.

    Expected patterns include:
      - "12%"
      - "12.5 %"
      - "render 12.5 %"

    We intentionally ignore negative values and values > 100.
    """
    s = (line or "").strip()
    if not s.endswith("%"):
        return None
    s = s[:-1].strip()  # drop trailing '%'
    if not s:
        return None

    token = s.split()[-1]
    try:
        v = float(token)
    except Exception:
        return None

    return v if 0.0 <= v <= 100.0 else None


def _fetch_asset_to(*, env: Env, drive: DriveClient | None, asset: dict, dest: Path) -> DriveClient | None:
    """Copy/download asset into dest. Returns DriveClient if instantiated."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    origin = (asset.get("origin") or "").upper()

    if origin == "LOCAL":
        src = Path(str(asset.get("origin_id") or asset.get("path") or "")).resolve()
        if not src.exists():
            raise RuntimeError(f"Local asset missing: {src}")
        shutil.copyfile(src, dest)
        return drive

    if origin == "GDRIVE":
        if drive is None:
            drive = DriveClient(
                service_account_json=env.gdrive_sa_json,
                oauth_client_json=env.gdrive_oauth_client_json,
                oauth_token_json=env.gdrive_oauth_token_json,
            )
        drive.download_to_path(str(asset["origin_id"]), dest)
        return drive

    raise RuntimeError(f"Unsupported asset origin: {origin}")


def orchestrator_cycle(*, env: Env, worker_id: str) -> None:
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)

        dbm.touch_worker(
            conn,
            worker_id=worker_id,
            role="orchestrator",
            pid=os.getpid(),
            hostname=socket.gethostname(),
            details={"origin_backend": env.origin_backend},
        )

        # Recovery for crashed orchestrators: reclaim stale FETCHING_INPUTS/RENDERING back to READY_FOR_RENDER.
        dbm.reclaim_stale_render_jobs(
            conn,
            lock_ttl_sec=env.job_lock_ttl_sec,
            backoff_sec=env.retry_backoff_sec,
            max_attempts=env.max_render_attempts,
        )

        job_id = dbm.claim_job(conn, want_state="READY_FOR_RENDER", worker_id=worker_id, lock_ttl_sec=env.job_lock_ttl_sec)
        if not job_id:
            return

        job = dbm.get_job(conn, job_id)
        if not job:
            dbm.release_lock(conn, job_id, worker_id)
            return

        if str(job.get("state") or "") == "CANCELLED":
            dbm.release_lock(conn, job_id, worker_id)
            return

        dbm.update_job_state(conn, job_id, state="FETCHING_INPUTS", stage="FETCH", progress_pct=0.0, progress_text="fetching inputs")

        inputs = conn.execute(
            """
            SELECT ji.role, ji.order_index, a.*
            FROM job_inputs ji
            JOIN assets a ON a.id = ji.asset_id
            WHERE ji.job_id = ?
            ORDER BY ji.role ASC, ji.order_index ASC
            """,
            (job_id,),
        ).fetchall()

    finally:
        conn.close()

    tracks = [i for i in inputs if i["role"] == "TRACK"]
    backgrounds = [i for i in inputs if i["role"] == "BACKGROUND"]
    covers = [i for i in inputs if i["role"] == "COVER"]

    render_bg = backgrounds[0] if backgrounds else (covers[0] if covers else None)

    if not tracks or not render_bg:
        conn = dbm.connect(env)
        try:
            attempt = dbm.increment_attempt(conn, job_id)
            reason = "missing inputs (tracks/background)"
            if attempt < env.max_render_attempts:
                dbm.schedule_retry(conn, job_id, next_state="READY_FOR_RENDER", stage="FETCH", error_reason=reason, backoff_sec=env.retry_backoff_sec)
            else:
                dbm.update_job_state(conn, job_id, state="RENDER_FAILED", stage="FETCH", error_reason=reason)
                dbm.clear_retry(conn, job_id)
                dbm.release_lock(conn, job_id, worker_id)
        finally:
            conn.close()
        return

    policies = load_policies("configs/policies.yaml").raw
    prev_cfg = policies.get("preview_policy", {})
    pv = prev_cfg.get("video", {})
    pa = prev_cfg.get("audio", {})
    prev_seconds = int(prev_cfg.get("seconds", 60))

    ws = workspace_dir(env, job_id)
    ob = outbox_dir(env, job_id)

    drive: DriveClient | None = None

    try:
        if ws.exists():
            shutil.rmtree(ws, ignore_errors=True)
        if ob.exists():
            shutil.rmtree(ob, ignore_errors=True)

        root_dir = ws / "YouTubeRoot"
        cancel_flag = cancel_flag_path(env, job_id)
        try:
            cancel_flag.unlink(missing_ok=True)
        except Exception:
            pass

        project_name = str(job["channel_name"])
        project_dir = root_dir / project_name
        audio_dir = project_dir / "Audio"
        images_dir = project_dir / "Images"
        release_dir = project_dir / "Release"
        for d in (audio_dir, images_dir, release_dir):
            d.mkdir(parents=True, exist_ok=True)

        # download background image (fallback: cover for legacy jobs)
        bg_name = safe_path_basename(str(render_bg.get("name") or "background.png"), fallback="background.png")
        bg_dst = images_dir / bg_name
        drive = _fetch_asset_to(env=env, drive=drive, asset=render_bg, dest=bg_dst)

        # download tracks
        track_ids: List[str] = []
        for idx, t in enumerate(tracks, start=1):
            orig_name = safe_path_basename(str(t.get("name") or f"track_{idx}.wav"), fallback=f"track_{idx}.wav")
            if not orig_name.lower().endswith(".wav"):
                raise RuntimeError(f"Audio must be WAV for render_worker. Got: {orig_name}")

            tid = f"{idx:03d}"
            track_ids.append(tid)
            new_name = f"{tid}_{safe_slug(Path(orig_name).stem)}.wav"
            dst = audio_dir / new_name
            drive = _fetch_asset_to(env=env, drive=drive, asset=t, dest=dst)

        # PlayLists.txt
        playlists = project_dir / "PlayLists.txt"
        title = " ".join(str(job["release_title"]).split()).replace(":", " -")
        block = [
            f"{title}: " + " ".join(track_ids),
            f"Image: {bg_name}",
            "Status: Not done",
            "",
        ]
        playlists.write_text("\n".join(block), encoding="utf-8")

        # If cancellation happened while fetching inputs, stop early.
        conn_check = dbm.connect(env)
        try:
            jx = dbm.get_job(conn_check, job_id)
            if jx and str(jx.get("state") or "") == "CANCELLED":
                dbm.cancel_job(conn_check, job_id, reason="cancelled by user")
                dbm.clear_retry(conn_check, job_id)
                dbm.release_lock(conn_check, job_id, worker_id)
                return
        finally:
            conn_check.close()

        # render
        conn = dbm.connect(env)
        try:
            dbm.update_job_state(conn, job_id, state="RENDERING", stage="RENDER", progress_pct=0.0, progress_text="rendering")
        finally:
            conn.close()

        cmd = [sys.executable, str(Path("render_worker") / "main.py"), "--root", str(root_dir)]
        append_job_log(env, job_id, "CMD: " + " ".join(cmd))

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

        last_pct = 0.0
        last_update = 0.0
        last_cancel_check = 0.0
        cancelled = False
        fatal_image_invalid: str | None = None

        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                append_job_log(env, job_id, line.rstrip())
                line_text = line.rstrip()
                if line_text.startswith("FATAL_IMAGE_INVALID:"):
                    fatal_image_invalid = line_text.split(":", 1)[1].strip()

                now = time.time()
                if now - last_cancel_check >= 1.0:
                    last_cancel_check = now

                    # cancel via marker file
                    try:
                        if cancel_flag.exists():
                            cancelled = True
                            append_job_log(env, job_id, "CANCEL_REQUESTED: terminating renderer")
                            proc.terminate()
                            break
                    except Exception:
                        pass

                    # cancel via DB state
                    try:
                        connc = dbm.connect(env)
                        try:
                            jx = dbm.get_job(connc, job_id)
                            if jx and str(jx.get("state") or "") == "CANCELLED":
                                cancelled = True
                                append_job_log(env, job_id, "CANCEL_REQUESTED(DB): terminating renderer")
                                proc.terminate()
                                break
                        finally:
                            connc.close()
                    except Exception:
                        pass

                pct = _parse_progress_pct(line)
                if (not cancelled) and pct is not None and (pct >= last_pct + 0.5 or now - last_update >= 2.0):
                    last_pct = max(last_pct, pct)
                    last_update = now
                    connp = dbm.connect(env)
                    try:
                        dbm.update_job_state(connp, job_id, state="RENDERING", stage="RENDER", progress_pct=last_pct, progress_text="rendering")
                    finally:
                        connp.close()
        finally:
            ret = proc.wait()

        if cancelled:
            connx = dbm.connect(env)
            try:
                dbm.cancel_job(connx, job_id, reason="cancelled by user")
                dbm.clear_retry(connx, job_id)
                dbm.release_lock(connx, job_id, worker_id)
            finally:
                connx.close()
            return

        if ret != 0:
            if fatal_image_invalid:
                raise RuntimeError(fatal_image_invalid)
            raise RuntimeError(f"renderer exited {ret}")

        # output mp4
        mp4s = sorted(release_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not mp4s:
            raise RuntimeError("no mp4 produced")

        mp4_src = mp4s[0]
        ob.mkdir(parents=True, exist_ok=True)
        mp4_dst = ob / "render.mp4"
        shutil.move(str(mp4_src), str(mp4_dst))

        # optional cover for thumbnail upload only
        if covers:
            cover = covers[0]
            cover_name = safe_path_basename(str(cover.get("name") or "cover.png"), fallback="cover.png")
            cover_dst = ob / "cover" / cover_name
            cover_dst.parent.mkdir(parents=True, exist_ok=True)
            if cover is render_bg:
                shutil.copyfile(bg_dst, cover_dst)
            else:
                tmp_cover = ws / "tmp_cover" / cover_name
                drive = _fetch_asset_to(env=env, drive=drive, asset=cover, dest=tmp_cover)
                shutil.copyfile(tmp_cover, cover_dst)

        # preview
        preview_dst = preview_path(env, job_id)
        make_preview_60s(
            src_mp4=mp4_dst,
            dst_mp4=preview_dst,
            seconds=prev_seconds,
            width=int(pv.get("width", 1280)),
            height=int(pv.get("height", 720)),
            fps=int(pv.get("fps", 24)),
            v_bitrate=str(pv.get("video_bitrate", "1200k")),
            a_bitrate=str(pa.get("bitrate", "96k")),
        )

        # register outputs
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, str(job["channel_slug"]))
            channel_id = int(ch["id"]) if ch else 0

            mp4_asset = dbm.create_asset(conn, channel_id=channel_id, kind="MP4", origin="VM", origin_id=None, name="render.mp4", path=str(mp4_dst))
            dbm.link_job_output(conn, job_id, mp4_asset, "MP4")

            prev_asset = dbm.create_asset(conn, channel_id=channel_id, kind="PREVIEW_60S", origin="VM", origin_id=None, name=preview_dst.name, path=str(preview_dst))
            dbm.link_job_output(conn, job_id, prev_asset, "PREVIEW_60S")

            dbm.update_job_state(conn, job_id, state="QA_RUNNING", stage="QA", progress_pct=100.0, progress_text="render done")
            dbm.clear_retry(conn, job_id)
            dbm.release_lock(conn, job_id, worker_id)
        finally:
            conn.close()

    except Exception as e:
        conn = dbm.connect(env)
        try:
            attempt = dbm.increment_attempt(conn, job_id)
            if attempt < env.max_render_attempts:
                dbm.schedule_retry(
                    conn,
                    job_id,
                    next_state="READY_FOR_RENDER",
                    stage="FETCH",
                    error_reason=f"attempt={attempt} retry: {e}",
                    backoff_sec=env.retry_backoff_sec,
                )
            else:
                dbm.update_job_state(conn, job_id, state="RENDER_FAILED", stage="RENDER", error_reason=str(e))
                dbm.clear_retry(conn, job_id)
                dbm.release_lock(conn, job_id, worker_id)
        finally:
            conn.close()
    finally:
        shutil.rmtree(ws, ignore_errors=True)
