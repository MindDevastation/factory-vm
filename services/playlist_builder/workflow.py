from __future__ import annotations

import hashlib
import json
import uuid
from time import monotonic
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from services.common import db as dbm
from services.playlist_builder.core import CuratedModeLimitExceeded, PlaylistBuilder, resolve_effective_brief_for_job
from services.playlist_builder.history import batch_distribution_overlap, list_effective_history, novelty_against_previous
from services.playlist_builder.models import PlaylistBrief, PlaylistPreviewResult

PREVIEW_TTL_HOURS = 72
EFFECTIVE_HISTORY_WINDOW = 20
PREVIEW_TIME_BUDGET_SECONDS = 8.0


class PreviewTimeBudgetExceeded(RuntimeError):
    def __init__(self, stage: str, elapsed_ms: float):
        super().__init__(f"Preview exceeded time budget while {stage}")
        self.stage = stage
        self.elapsed_ms = elapsed_ms


class PreviewTimer:
    def __init__(self, *, budget_seconds: float):
        self._started = monotonic()
        self._budget_seconds = budget_seconds
        self.timings_ms: dict[str, float] = {}

    def stage(self, name: str, started: float) -> None:
        self.timings_ms[f"{name}_ms"] = round((monotonic() - started) * 1000.0, 3)

    def assert_within_budget(self, stage: str) -> None:
        elapsed = monotonic() - self._started
        if elapsed > self._budget_seconds:
            raise PreviewTimeBudgetExceeded(stage=stage, elapsed_ms=round(elapsed * 1000.0, 3))

    def finalize(self) -> None:
        self.timings_ms["preview_total_ms"] = round((monotonic() - self._started) * 1000.0, 3)


class PlaylistBuilderApiError(Exception):
    def __init__(self, code: str, message: str, *, diagnostics: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.diagnostics = diagnostics or {}


@dataclass
class PreviewEnvelope:
    preview_id: str
    brief: PlaylistBrief
    preview_result: PlaylistPreviewResult
    tracks: list[dict[str, Any]]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _fingerprint(parts: list[int]) -> str:
    payload = "|".join(str(int(v)) for v in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _fetch_tracks(conn: object, ordered_track_pks: list[int]) -> list[dict[str, Any]]:
    if not ordered_track_pks:
        return []
    placeholders = ",".join("?" for _ in ordered_track_pks)
    rows = conn.execute(
        f"""
        SELECT t.id AS track_pk, t.track_id, t.channel_slug, COALESCE(t.title, '') AS title,
               t.month_batch, taf.duration_sec, COALESCE(taf.yamnet_top_tags_text, '') AS yamnet_top_tags_text,
               taf.voice_flag
        FROM track_analysis_flat taf
        JOIN tracks t ON t.id = taf.track_pk
        WHERE taf.track_pk IN ({placeholders})
        """,
        tuple(ordered_track_pks),
    ).fetchall()
    by_pk = {int(r["track_pk"]): r for r in rows}
    ordered: list[dict[str, Any]] = []
    for idx, pk in enumerate(ordered_track_pks, 1):
        row = by_pk.get(int(pk))
        if row is None:
            continue
        tags_text = str(row.get("yamnet_top_tags_text") or "")
        tags = [t.strip() for t in tags_text.split(",") if t.strip()]
        ordered.append(
            {
                "position": idx,
                "track_pk": int(row["track_pk"]),
                "track_id": str(row.get("track_id") or ""),
                "title": str(row.get("title") or ""),
                "channel_slug": str(row.get("channel_slug") or ""),
                "month_batch": row.get("month_batch"),
                "duration_sec": float(row.get("duration_sec") or 0.0),
                "tags": tags,
                "voice_flag": row.get("voice_flag"),
            }
        )
    return ordered




def _generate_preview_envelope(
    conn: object,
    *,
    brief: PlaylistBrief,
    preview_job_id: int | None,
    created_by: str | None,
    timer: PreviewTimer,
) -> PreviewEnvelope:
    try:
        builder_started = monotonic()
        result = PlaylistBuilder().generate_preview(conn, brief)
        timer.stage("builder_generation", builder_started)
    except CuratedModeLimitExceeded as exc:
        raise PlaylistBuilderApiError("PLB_CURATED_LIMIT_EXCEEDED", str(exc)) from exc
    timer.assert_within_budget("evaluating candidate pool")
    if result.status == "empty":
        reason = str((result.diagnostics or {}).get("reason") or "")
        if reason:
            code = "PLB_NO_CANDIDATES" if "analyzed eligible tracks" in reason.lower() or "candidates remained" in reason.lower() else "PLB_NO_VALID_PLAYLIST"
            raise PlaylistBuilderApiError(code, reason, diagnostics=result.diagnostics)
        raise PlaylistBuilderApiError("PLB_NO_VALID_PLAYLIST", "No valid playlist could be composed", diagnostics=result.diagnostics)

    preview_id = str(uuid.uuid4())
    created_at = _now_utc()
    expires_at = created_at + timedelta(hours=PREVIEW_TTL_HOURS)

    fetch_started = monotonic()
    tracks = _fetch_tracks(conn, result.ordered_track_pks)
    timer.stage("track_fetch", fetch_started)
    notes_by_pk = {int(item.get("track_pk", -1)): str(item.get("fit_note") or item.get("note") or "") for item in result.per_track_fit_notes}
    for track in tracks:
        track["fit_note"] = notes_by_pk.get(int(track["track_pk"]), "")

    persist_started = monotonic()
    conn.execute(
        """
        INSERT INTO playlist_build_previews(
            id, job_id, channel_slug, effective_brief_json, preview_result_json,
            created_by, created_at, expires_at, status
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            preview_id,
            preview_job_id,
            brief.channel_slug,
            json.dumps(brief.model_dump(), sort_keys=True),
            json.dumps(result.model_dump(), sort_keys=True),
            created_by,
            _iso(created_at),
            _iso(expires_at),
            "PREVIEW",
        ),
    )
    timer.stage("preview_persistence", persist_started)

    diagnostics = result.diagnostics or {}
    diagnostics.update(timer.timings_ms)
    result.diagnostics = diagnostics

    return PreviewEnvelope(preview_id=preview_id, brief=brief, preview_result=result, tracks=tracks)

def create_preview(conn: object, *, job_id: int, override: dict[str, Any] | None, created_by: str | None = None) -> PreviewEnvelope:
    timer = PreviewTimer(budget_seconds=PREVIEW_TIME_BUDGET_SECONDS)
    resolve_job_started = monotonic()
    job = dbm.get_job(conn, job_id)
    draft = dbm.get_ui_job_draft(conn, job_id)
    timer.stage("job_resolution", resolve_job_started)
    if not job or not draft:
        raise PlaylistBuilderApiError("PLB_JOB_NOT_FOUND", "UI job not found")

    try:
        brief_started = monotonic()
        brief = resolve_effective_brief_for_job(conn, job_id=job_id, request_override=override or {})
        timer.stage("effective_brief_resolution", brief_started)
    except Exception as exc:
        raise PlaylistBuilderApiError("PLB_INVALID_BRIEF", str(exc)) from exc
    timer.assert_within_budget("resolving effective brief")

    envelope = _generate_preview_envelope(
        conn,
        brief=brief,
        preview_job_id=job_id,
        created_by=created_by,
        timer=timer,
    )
    timer.finalize()
    envelope.preview_result.diagnostics = envelope.preview_result.diagnostics or {}
    envelope.preview_result.diagnostics.update(timer.timings_ms)
    return envelope


def create_preview_for_brief(conn: object, *, brief: PlaylistBrief, created_by: str | None = None) -> PreviewEnvelope:
    timer = PreviewTimer(budget_seconds=PREVIEW_TIME_BUDGET_SECONDS)
    envelope = _generate_preview_envelope(
        conn,
        brief=brief,
        preview_job_id=brief.job_id,
        created_by=created_by,
        timer=timer,
    )
    timer.finalize()
    envelope.preview_result.diagnostics = envelope.preview_result.diagnostics or {}
    envelope.preview_result.diagnostics.update(timer.timings_ms)
    return envelope


def get_preview_row(conn: object, *, preview_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM playlist_build_previews WHERE id = ?", (preview_id,)).fetchone()
    return row


def build_preview_response(envelope: PreviewEnvelope) -> dict[str, Any]:
    result = envelope.preview_result
    brief = envelope.brief
    target_min = round(brief.target_duration_min, 3)
    achieved_min = round(result.achieved_duration_min, 3)
    return {
        "preview_id": envelope.preview_id,
        "summary": {
            "generation_mode": brief.generation_mode,
            "strictness_mode": brief.strictness_mode,
            "duration": {
                "min": round(brief.min_duration_min, 3),
                "target": target_min,
                "max": round(brief.max_duration_min, 3),
                "tolerance": round(brief.tolerance_min, 3),
                "achieved": achieved_min,
                "deviation_from_target": round(achieved_min - target_min, 3),
            },
            "tracks_count": len(envelope.tracks),
            "batch_ratio": {
                "target_preferred": brief.preferred_batch_ratio / 100.0,
                "achieved_preferred": result.achieved_batch_ratio,
            },
            "novelty": {
                "target_min": brief.novelty_target_min,
                "target_max": brief.novelty_target_max,
                "achieved": result.achieved_novelty,
            },
            "warnings": result.warnings,
            "relaxations": result.relaxations,
            "relaxations_structured": [item.model_dump() for item in result.relaxations_structured],
            "diagnostics": result.diagnostics or {},
        },
        "tracks": envelope.tracks,
    }


def _load_preview_payload(row: dict[str, Any]) -> tuple[PlaylistBrief, PlaylistPreviewResult]:
    brief = PlaylistBrief.model_validate(json.loads(str(row["effective_brief_json"])))
    result = PlaylistPreviewResult.model_validate(json.loads(str(row["preview_result_json"])))
    return brief, result


def _deactivate_existing_draft_history(conn: object, *, channel_slug: str, job_id: int) -> None:
    conn.execute(
        "UPDATE playlist_history SET is_active = 0 WHERE channel_slug = ? AND job_id = ? AND history_stage = 'DRAFT' AND is_active = 1",
        (channel_slug, job_id),
    )


def _find_preview_draft_history(conn: object, *, preview_id: str) -> dict[str, Any] | None:
    return conn.execute(
        "SELECT * FROM playlist_history WHERE source_preview_id = ? AND history_stage = 'DRAFT' ORDER BY id DESC LIMIT 1",
        (preview_id,),
    ).fetchone()


def _insert_draft_history(conn: object, *, preview_id: str, brief: PlaylistBrief, result: PlaylistPreviewResult, tracks: list[dict[str, Any]]) -> int:
    previous = conn.execute(
        "SELECT id FROM playlist_history WHERE channel_slug = ? AND is_active = 1 ORDER BY datetime(created_at) DESC, id DESC LIMIT 1",
        (brief.channel_slug,),
    ).fetchone()
    prev_tracks: list[int] = []
    prev_batches: list[str | None] = []
    if previous:
        rows = conn.execute(
            "SELECT track_pk, month_batch FROM playlist_history_items WHERE history_id = ? ORDER BY position_index ASC",
            (int(previous["id"]),),
        ).fetchall()
        prev_tracks = [int(r["track_pk"]) for r in rows]
        prev_batches = [r["month_batch"] for r in rows]

    ordered = [int(t["track_pk"]) for t in tracks]
    batches = [t.get("month_batch") for t in tracks]
    now = _iso(_now_utc())
    cur = conn.execute(
        """
        INSERT INTO playlist_history(
            channel_slug, job_id, history_stage, source_preview_id, generation_mode,
            strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint,
            ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5,
            novelty_against_prev, batch_overlap_score, is_active, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)
        """,
        (
            brief.channel_slug,
            brief.job_id,
            "DRAFT",
            preview_id,
            brief.generation_mode,
            brief.strictness_mode,
            float(result.achieved_duration_sec),
            len(ordered),
            _fingerprint(sorted(ordered)),
            _fingerprint(ordered),
            _fingerprint(ordered[:3]),
            _fingerprint(ordered[:5]),
            novelty_against_previous(ordered, prev_tracks) if prev_tracks else None,
            batch_distribution_overlap(batches, prev_batches) if prev_batches else None,
            now,
        ),
    )
    history_id = int(cur.lastrowid)
    for pos, track in enumerate(tracks):
        conn.execute(
            """
            INSERT INTO playlist_history_items(history_id, position_index, track_pk, month_batch, duration_sec, channel_slug)
            VALUES(?,?,?,?,?,?)
            """,
            (
                history_id,
                pos,
                int(track["track_pk"]),
                track.get("month_batch"),
                float(track.get("duration_sec") or 0.0),
                str(track.get("channel_slug") or brief.channel_slug),
            ),
        )
    return history_id


def write_committed_history_for_published(conn: object, *, job_id: int) -> int | None:
    draft = dbm.get_ui_job_draft(conn, job_id)
    if draft is None:
        return None

    existing = conn.execute(
        "SELECT id FROM playlist_history WHERE job_id = ? AND history_stage = 'COMMITTED' ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if existing:
        return int(existing["id"])

    draft_history = conn.execute(
        """
        SELECT *
        FROM playlist_history
        WHERE job_id = ? AND history_stage = 'DRAFT' AND is_active = 1
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT 1
        """,
        (job_id,),
    ).fetchone()
    if draft_history is None:
        raise PlaylistBuilderApiError("PLB_COMMITTED_HISTORY_MISSING_DRAFT", f"No active draft playlist history found for job_id={job_id}")

    draft_items = conn.execute(
        """
        SELECT position_index, track_pk, month_batch, duration_sec, channel_slug
        FROM playlist_history_items
        WHERE history_id = ?
        ORDER BY position_index ASC
        """,
        (int(draft_history["id"]),),
    ).fetchall()
    if not draft_items:
        raise PlaylistBuilderApiError("PLB_COMMITTED_HISTORY_MISSING_ITEMS", f"No draft playlist history items found for job_id={job_id}")

    draft_audio_ids = [int(x) for x in str(draft.get("audio_ids_text") or "").replace(",", " ").split() if x.strip()]
    if not draft_audio_ids:
        raise PlaylistBuilderApiError(
            "PLB_COMMITTED_HISTORY_PLAYLIST_MISMATCH",
            f"Draft playlist is empty or invalid for job_id={job_id}",
        )

    current_tracks = _fetch_tracks(conn, draft_audio_ids)
    if len(current_tracks) != len(draft_audio_ids):
        raise PlaylistBuilderApiError(
            "PLB_COMMITTED_HISTORY_PLAYLIST_MISMATCH",
            f"Draft playlist has missing track metadata for job_id={job_id}",
        )

    current_ordered = [int(t["track_pk"]) for t in current_tracks]
    draft_history_ordered = [int(item["track_pk"]) for item in draft_items]
    if current_ordered != draft_history_ordered:
        raise PlaylistBuilderApiError(
            "PLB_COMMITTED_HISTORY_PLAYLIST_MISMATCH",
            f"Draft playlist no longer matches active draft history for job_id={job_id}",
        )

    effective = list_effective_history(conn, channel_slug=str(draft_history["channel_slug"]), window=EFFECTIVE_HISTORY_WINDOW)
    prev = next((entry for entry in effective if int(entry.job_id or -1) != int(job_id)), None)

    current_batches = [t.get("month_batch") for t in current_tracks]
    novelty = novelty_against_previous(current_ordered, prev.tracks) if prev else None
    batch_overlap = batch_distribution_overlap(current_batches, prev.month_batches) if prev else None
    playlist_duration_sec = sum(float(t.get("duration_sec") or 0.0) for t in current_tracks)

    cur = conn.execute(
        """
        INSERT INTO playlist_history(
            channel_slug, job_id, history_stage, source_preview_id, generation_mode,
            strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint,
            ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5,
            novelty_against_prev, batch_overlap_score, is_active, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)
        """,
        (
            draft_history["channel_slug"],
            int(draft_history["job_id"]),
            "COMMITTED",
            draft_history["source_preview_id"],
            draft_history["generation_mode"],
            draft_history["strictness_mode"],
            float(playlist_duration_sec),
            len(current_ordered),
            _fingerprint(sorted(current_ordered)),
            _fingerprint(current_ordered),
            _fingerprint(current_ordered[:3]),
            _fingerprint(current_ordered[:5]),
            novelty,
            batch_overlap,
            _iso(_now_utc()),
        ),
    )
    history_id = int(cur.lastrowid)

    for pos, track in enumerate(current_tracks):
        conn.execute(
            """
            INSERT INTO playlist_history_items(history_id, position_index, track_pk, month_batch, duration_sec, channel_slug)
            VALUES(?,?,?,?,?,?)
            """,
            (
                history_id,
                pos,
                int(track["track_pk"]),
                track.get("month_batch"),
                float(track.get("duration_sec") or 0.0),
                track.get("channel_slug") or draft_history["channel_slug"],
            ),
        )

    return history_id


def apply_preview(conn: object, *, job_id: int, preview_id: str, manage_transaction: bool = True) -> dict[str, Any]:
    job = dbm.get_job(conn, job_id)
    draft = dbm.get_ui_job_draft(conn, job_id)
    if not job or not draft:
        raise PlaylistBuilderApiError("PLB_JOB_NOT_FOUND", "UI job not found")

    post_commit_error: PlaylistBuilderApiError | None = None
    response: dict[str, Any] | None = None

    if manage_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_preview_row(conn, preview_id=preview_id)
        if not row or int(row.get("job_id") or -1) != job_id:
            raise PlaylistBuilderApiError("PLB_PREVIEW_NOT_FOUND", "Preview not found for this job")

        existing_history = _find_preview_draft_history(conn, preview_id=preview_id)
        if str(row.get("status") or "").upper() == "APPLIED" and existing_history:
            response = {
                "job_id": str(job_id),
                "playlist_applied": True,
                "draft_history_id": int(existing_history["id"]),
                "history_written": False,
            }
        else:
            expires_at = _parse_iso(str(row["expires_at"]))
            if _now_utc() > expires_at:
                conn.execute("UPDATE playlist_build_previews SET status = 'EXPIRED' WHERE id = ?", (preview_id,))
                post_commit_error = PlaylistBuilderApiError("PLB_PREVIEW_EXPIRED", "Preview expired")
            elif str(row.get("status") or "").upper() != "PREVIEW":
                raise PlaylistBuilderApiError("PLB_APPLY_CONFLICT", "Preview cannot be applied in current state")
            else:
                brief, result = _load_preview_payload(row)
                tracks = _fetch_tracks(conn, result.ordered_track_pks)
                if len(tracks) != len(result.ordered_track_pks):
                    raise PlaylistBuilderApiError("PLB_APPLY_CONFLICT", "Preview payload tracks are no longer fully available")
                if not tracks:
                    raise PlaylistBuilderApiError("PLB_APPLY_CONFLICT", "Preview payload has no applicable tracks")

                existing_history = _find_preview_draft_history(conn, preview_id=preview_id)
                if existing_history:
                    conn.execute("UPDATE playlist_build_previews SET status = 'APPLIED' WHERE id = ?", (preview_id,))
                    response = {
                        "job_id": str(job_id),
                        "playlist_applied": True,
                        "draft_history_id": int(existing_history["id"]),
                        "history_written": False,
                    }
                else:
                    audio_ids = " ".join(str(t["track_pk"]) for t in tracks)
                    updated = conn.execute(
                        "UPDATE ui_job_drafts SET audio_ids_text = ?, updated_at = ? WHERE job_id = ?",
                        (audio_ids, dbm.now_ts(), job_id),
                    )
                    if int(updated.rowcount or 0) == 0:
                        raise PlaylistBuilderApiError("PLB_JOB_NOT_FOUND", "UI job not found")

                    conn.execute("UPDATE playlist_build_previews SET status = 'APPLIED' WHERE id = ?", (preview_id,))
                    _deactivate_existing_draft_history(conn, channel_slug=brief.channel_slug, job_id=job_id)
                    try:
                        history_id = _insert_draft_history(conn, preview_id=preview_id, brief=brief, result=result, tracks=tracks)
                    except Exception as exc:
                        if "UNIQUE constraint failed" in str(exc):
                            existing_history = _find_preview_draft_history(conn, preview_id=preview_id)
                            if existing_history:
                                response = {
                                    "job_id": str(job_id),
                                    "playlist_applied": True,
                                    "draft_history_id": int(existing_history["id"]),
                                    "history_written": False,
                                }
                            else:
                                raise PlaylistBuilderApiError("PLB_HISTORY_WRITE_FAILED", f"Failed to write draft history: {exc}") from exc
                        else:
                            raise PlaylistBuilderApiError("PLB_HISTORY_WRITE_FAILED", f"Failed to write draft history: {exc}") from exc
                    else:
                        response = {
                            "job_id": str(job_id),
                            "playlist_applied": True,
                            "draft_history_id": history_id,
                            "history_written": True,
                        }

        if manage_transaction:
            conn.commit()
    except Exception:
        if manage_transaction:
            conn.rollback()
        raise

    if post_commit_error is not None:
        raise post_commit_error
    assert response is not None
    return response
