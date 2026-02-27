from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.common import db as dbm
from services.common import ffmpeg


class AnalyzeError(RuntimeError):
    pass


@dataclass(frozen=True)
class AnalyzeStats:
    selected: int
    processed: int
    failed: int


_TRUE_PEAK_RE = re.compile(r"(?:true\s+peak|peak)\s*[:=]\s*(-?\d+(?:\.\d+)?)\s*dB", re.IGNORECASE)


def analyze_tracks(
    conn: Any,
    drive: Any,
    *,
    channel_slug: str,
    storage_root: str,
    job_id: int,
    scope: str = "pending",
    force: bool = False,
    max_tracks: int = 200,
) -> AnalyzeStats:
    _require_thresholds(conn, channel_slug)

    tracks = _select_tracks(conn, channel_slug=channel_slug, scope=scope, force=force, max_tracks=max_tracks)
    selected = len(tracks)
    processed = 0
    failed = 0

    for row in tracks:
        track_pk = int(row["id"])
        file_id = str(row["gdrive_file_id"])
        track_tmp_dir = Path(storage_root) / "tmp" / "track_analyzer" / str(job_id) / str(track_pk)
        local_path = track_tmp_dir / f"{file_id}.wav"
        try:
            drive.download_to_path(file_id, local_path)
            duration_sec = _extract_duration_sec(local_path)
            true_peak_dbfs = _extract_true_peak_dbfs(local_path)
            spikes_found = _detect_spikes(true_peak_dbfs)

            dominant_texture = "unknown texture"
            prohibited_cues_notes = "No prohibited cues detected by fallback analyzer."
            dsp_score = _derive_dsp_score(true_peak_dbfs, spikes_found)

            missing_fields: list[str] = []
            if not dominant_texture.strip():
                missing_fields.append("dominant_texture")
            if not prohibited_cues_notes.strip():
                missing_fields.append("prohibited_cues_notes")
            if dsp_score is None:
                missing_fields.append("dsp_score")

            analysis_status = "COMPLETE" if not missing_fields else "REVIEW"
            computed_at = dbm.now_ts()

            features_payload = {
                "duration_sec": duration_sec,
                "true_peak_dbfs": true_peak_dbfs,
                "spikes_found": spikes_found,
                "dominant_texture": dominant_texture,
                "analysis_status": analysis_status,
                "missing_fields": missing_fields,
            }
            tags_payload = {
                "prohibited_cues_notes": prohibited_cues_notes,
                "analysis_status": analysis_status,
                "missing_fields": missing_fields,
            }
            scores_payload = {
                "dsp_score": dsp_score,
                "analysis_status": analysis_status,
                "missing_fields": missing_fields,
            }

            conn.execute(
                """
                INSERT INTO track_features(track_pk, payload_json, computed_at)
                VALUES(?,?,?)
                ON CONFLICT(track_pk) DO UPDATE SET payload_json=excluded.payload_json, computed_at=excluded.computed_at
                """,
                (track_pk, dbm.json_dumps(features_payload), computed_at),
            )
            conn.execute(
                """
                INSERT INTO track_tags(track_pk, payload_json, computed_at)
                VALUES(?,?,?)
                ON CONFLICT(track_pk) DO UPDATE SET payload_json=excluded.payload_json, computed_at=excluded.computed_at
                """,
                (track_pk, dbm.json_dumps(tags_payload), computed_at),
            )
            conn.execute(
                """
                INSERT INTO track_scores(track_pk, payload_json, computed_at)
                VALUES(?,?,?)
                ON CONFLICT(track_pk) DO UPDATE SET payload_json=excluded.payload_json, computed_at=excluded.computed_at
                """,
                (track_pk, dbm.json_dumps(scores_payload), computed_at),
            )
            conn.execute(
                "UPDATE tracks SET analyzed_at=?, duration_sec=? WHERE id=?",
                (computed_at, duration_sec, track_pk),
            )
            processed += 1
        except Exception:
            failed += 1
            raise
        finally:
            shutil.rmtree(track_tmp_dir, ignore_errors=True)

    return AnalyzeStats(selected=selected, processed=processed, failed=failed)


def _select_tracks(conn: Any, *, channel_slug: str, scope: str, force: bool, max_tracks: int) -> list[dict[str, Any]]:
    normalized_scope = scope.strip().lower()
    if normalized_scope not in {"pending", "all"}:
        raise AnalyzeError("invalid scope")

    where = ["channel_slug = ?"]
    args: list[Any] = [channel_slug]
    if normalized_scope == "pending" and not force:
        where.append("analyzed_at IS NULL")

    args.append(int(max_tracks))
    return conn.execute(
        f"""
        SELECT id, gdrive_file_id
        FROM tracks
        WHERE {' AND '.join(where)}
        ORDER BY id ASC
        LIMIT ?
        """,
        tuple(args),
    ).fetchall()


def _require_thresholds(conn: Any, channel_slug: str) -> None:
    row = conn.execute("SELECT 1 FROM canon_thresholds WHERE value = ? LIMIT 1", (channel_slug,)).fetchone()
    if row is None:
        raise AnalyzeError("CHANNEL_NOT_IN_CANON")


def _extract_duration_sec(path: Path) -> float | None:
    data = ffmpeg.ffprobe_json(path)
    raw_duration = (data.get("format") or {}).get("duration")
    if raw_duration is None:
        for stream in data.get("streams") or []:
            raw_duration = stream.get("duration")
            if raw_duration is not None:
                break
    if raw_duration is None:
        return None
    try:
        return float(raw_duration)
    except Exception:
        return None


def _extract_true_peak_dbfs(path: Path) -> float | None:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-nostats",
        "-i",
        str(path),
        "-af",
        "ebur128=peak=true",
        "-f",
        "null",
        "-",
    ]
    code, out, err = ffmpeg.run(cmd)
    text = out + "\n" + err
    if code == 0:
        peak = _parse_true_peak(text)
        if peak is not None:
            return peak

    _mean_db, max_db, _warn = ffmpeg.volumedetect(path)
    return max_db


def _parse_true_peak(text: str) -> float | None:
    m = _TRUE_PEAK_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _detect_spikes(true_peak_dbfs: float | None) -> bool:
    if true_peak_dbfs is None:
        return False
    return true_peak_dbfs > -1.0


def _derive_dsp_score(true_peak_dbfs: float | None, spikes_found: bool) -> float:
    if true_peak_dbfs is None:
        return 0.5
    if spikes_found:
        return 0.4
    return 0.9
