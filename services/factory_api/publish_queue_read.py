from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.security import require_basic_auth
from services.publish_runtime.events import read_publish_lifecycle_events
from services.publish_runtime.queue_summary import assemble_publish_queue_summary


_VIEW_STATE_MAP: dict[str, tuple[str, ...]] = {
    "queue": ("private_uploaded", "waiting_for_schedule", "ready_to_publish", "publish_in_progress", "retry_pending"),
    "blocked": ("policy_blocked",),
    "failed": ("publish_failed_terminal",),
    "manual": ("manual_handoff_pending", "manual_handoff_acknowledged"),
    "health": tuple(),
}


def _to_iso_utc(value: Any) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_optional_ts(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    normalized = text.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).timestamp()


def _effective_decision(row: Any) -> dict[str, Any]:
    if bool(row.get("publish_hold_active") or 0):
        return {
            "decision": "hold",
            "reason_code": row.get("publish_hold_reason_code") or row.get("publish_reason_code"),
            "delivery_mode": row.get("publish_delivery_mode_effective"),
            "resolved_scope": row.get("publish_resolved_scope"),
        }
    mode = str(row.get("publish_delivery_mode_effective") or "")
    if mode == "manual":
        decision = "manual_handoff"
    elif mode == "automatic":
        decision = "auto_publish"
    else:
        decision = "unknown"
    return {
        "decision": decision,
        "reason_code": row.get("publish_reason_code"),
        "delivery_mode": row.get("publish_delivery_mode_effective"),
        "resolved_scope": row.get("publish_resolved_scope"),
    }


def _normalize_event(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_name": item.get("event_name") or item.get("event") or "unknown",
        "occurred_at": item.get("occurred_at") or item.get("ts") or item.get("created_at"),
        "publish_state_before": item.get("publish_state_before"),
        "publish_state_after": item.get("publish_state_after"),
        "changed_fields": list(item.get("changed_fields") or []),
        "actor": item.get("actor") or item.get("actor_identity"),
        "raw": item,
    }


def _build_queue_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "job_id": int(row["id"]),
            "release_id": int(row["release_id"]),
            "channel_slug": row.get("channel_slug"),
            "channel_name": row.get("channel_name"),
            "release_title": row.get("release_title"),
            "state": row.get("state"),
            "stage": row.get("stage"),
            "publish_state": row.get("publish_state"),
            "publish_scheduled_at": _to_iso_utc(row.get("publish_scheduled_at")),
            "publish_attempt_count": int(row.get("publish_attempt_count") or 0),
            "publish_last_error_code": row.get("publish_last_error_code"),
            "publish_last_error_message": row.get("publish_last_error_message"),
            "publish_hold_active": bool(row.get("publish_hold_active") or 0),
            "publish_drift_detected_at": _to_iso_utc(row.get("publish_drift_detected_at")),
            "updated_at": _to_iso_utc(row.get("updated_at")),
        }
        for row in rows
    ]


def _job_not_found(job_id: int) -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": {"code": "PQR_JOB_NOT_FOUND", "message": f"job {job_id} not found"}})


def create_publish_queue_read_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/publish", tags=["publish-queue-read"])

    @router.get("/queue")
    def get_publish_queue(
        channel_slug: str | None = None,
        publish_state: str | None = None,
        view: str = "queue",
        scheduled_before: str | None = None,
        scheduled_after: str | None = None,
        limit: int = Query(default=200, ge=1, le=500),
        _: bool = Depends(require_basic_auth(env)),
    ):
        normalized_view = str(view or "queue").strip().lower()
        if normalized_view not in _VIEW_STATE_MAP:
            return JSONResponse(status_code=422, content={"error": {"code": "PQR_INVALID_VIEW", "message": "view must be queue|blocked|failed|manual|health"}})

        try:
            before_ts = _parse_optional_ts(scheduled_before)
            after_ts = _parse_optional_ts(scheduled_after)
        except ValueError:
            return JSONResponse(status_code=422, content={"error": {"code": "PQR_INVALID_SCHEDULE", "message": "scheduled_before/after must be epoch seconds or ISO datetime"}})

        where_clauses = ["j.publish_state IS NOT NULL"]
        params: list[Any] = []

        if channel_slug:
            where_clauses.append("c.slug = ?")
            params.append(channel_slug)
        if publish_state:
            where_clauses.append("j.publish_state = ?")
            params.append(str(publish_state).strip().lower())
        if before_ts is not None:
            where_clauses.append("j.publish_scheduled_at IS NOT NULL AND j.publish_scheduled_at <= ?")
            params.append(before_ts)
        if after_ts is not None:
            where_clauses.append("j.publish_scheduled_at IS NOT NULL AND j.publish_scheduled_at >= ?")
            params.append(after_ts)

        view_states = _VIEW_STATE_MAP[normalized_view]
        if view_states:
            placeholders = ",".join("?" for _ in view_states)
            where_clauses.append(f"j.publish_state IN ({placeholders})")
            params.extend(view_states)

        where_sql = " AND ".join(where_clauses)

        conn = dbm.connect(env)
        try:
            rows = conn.execute(
                f"""
                SELECT j.*, r.title AS release_title, c.slug AS channel_slug, c.display_name AS channel_name
                FROM jobs j
                JOIN releases r ON r.id = j.release_id
                JOIN channels c ON c.id = r.channel_id
                WHERE {where_sql}
                ORDER BY (j.publish_scheduled_at IS NULL) ASC, j.publish_scheduled_at ASC, j.updated_at DESC, j.id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
            all_rows = conn.execute(
                """
                SELECT j.publish_state, j.publish_hold_active
                FROM jobs j
                WHERE j.publish_state IS NOT NULL
                """
            ).fetchall()
        finally:
            conn.close()

        items = _build_queue_items(rows)
        summary = assemble_publish_queue_summary(all_rows)
        payload: dict[str, Any] = {
            "view": normalized_view,
            "filters": {
                "channel_slug": channel_slug,
                "publish_state": publish_state,
                "scheduled_before": scheduled_before,
                "scheduled_after": scheduled_after,
                "limit": limit,
            },
            "summary": summary,
            "items": items,
        }
        if normalized_view == "health":
            payload = {
                "view": normalized_view,
                "filters": payload["filters"],
                "summary": summary,
                "items": items,
                "health": summary,
            }
        return payload

    @router.get("/jobs/{job_id}")
    def get_publish_job_detail(job_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            row = conn.execute(
                """
                SELECT j.*, r.title AS release_title, c.slug AS channel_slug, c.display_name AS channel_name
                FROM jobs j
                JOIN releases r ON r.id = j.release_id
                JOIN channels c ON c.id = r.channel_id
                WHERE j.id = ?
                """,
                (job_id,),
            ).fetchone()
        finally:
            conn.close()

        if not row:
            return _job_not_found(job_id)

        events = read_publish_lifecycle_events(storage_root=env.storage_root, limit=200)
        matched = [event for event in events if int(event.get("job_id") or -1) == job_id]

        return {
            "job_id": int(row["id"]),
            "release": {
                "release_id": int(row["release_id"]),
                "title": row.get("release_title"),
                "channel_slug": row.get("channel_slug"),
                "channel_name": row.get("channel_name"),
            },
            "global_state_stage_summary": {
                "state": row.get("state"),
                "stage": row.get("stage"),
                "progress_pct": float(row.get("progress_pct") or 0.0),
                "updated_at": _to_iso_utc(row.get("updated_at")),
            },
            "publish_state": row.get("publish_state"),
            "effective_decision": _effective_decision(row),
            "schedule": {
                "scheduled_at": _to_iso_utc(row.get("publish_scheduled_at")),
                "retry_at": _to_iso_utc(row.get("publish_retry_at")),
                "last_transition_at": _to_iso_utc(row.get("publish_last_transition_at")),
            },
            "attempts": {
                "publish_attempt_count": int(row.get("publish_attempt_count") or 0),
                "job_attempt": int(row.get("attempt") or 0),
                "attempt_no": int(row.get("attempt_no") or 1),
            },
            "last_error": {
                "code": row.get("publish_last_error_code"),
                "message": row.get("publish_last_error_message"),
            },
            "audit_trail_summary": {
                "count": len(matched),
                "latest_event_name": matched[0].get("event_name") if matched else None,
                "latest_occurred_at": (matched[0].get("occurred_at") if matched else None),
            },
            "manual_handoff": {
                "ack_at": _to_iso_utc(row.get("publish_manual_ack_at")),
                "completed_at": _to_iso_utc(row.get("publish_manual_completed_at")),
                "published_at": _to_iso_utc(row.get("publish_manual_published_at")),
                "video_id": row.get("publish_manual_video_id"),
                "url": row.get("publish_manual_url"),
            },
            "drift": {
                "detected_at": _to_iso_utc(row.get("publish_drift_detected_at")),
                "observed_visibility": row.get("publish_observed_visibility"),
            },
        }

    @router.get("/jobs/{job_id}/audit")
    def get_publish_job_audit(job_id: int, limit: int = Query(default=50, ge=1, le=200), _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            found = conn.execute("SELECT 1 FROM jobs WHERE id = ?", (job_id,)).fetchone()
        finally:
            conn.close()
        if not found:
            return _job_not_found(job_id)

        events = read_publish_lifecycle_events(storage_root=env.storage_root, limit=2000)
        rows: list[dict[str, Any]] = []
        for item in events:
            raw_job_id = item.get("job_id")
            if raw_job_id is None:
                continue
            try:
                current_job_id = int(raw_job_id)
            except Exception:
                continue
            if current_job_id != job_id:
                continue
            rows.append(_normalize_event(item))
            if len(rows) >= limit:
                break

        return {"job_id": job_id, "items": rows, "limit": limit}

    return router


__all__ = ["create_publish_queue_read_router"]
