from __future__ import annotations

from typing import Any

_ALLOWED_BY_STATE: dict[str, list[str]] = {
    "manual_handoff_pending": ["approve", "reject", "ack_manual_handoff"],
    "manual_handoff_acknowledged": ["reject", "confirm_manual_completion"],
    "ready_to_publish": ["approve", "reject"],
    "retry_pending": ["approve", "reject"],
    "policy_blocked": ["reject"],
    "publish_failed_terminal": ["reject"],
    "publish_state_drift_detected": [],
    "published_public": [],
    "published_unlisted": [],
    "manual_publish_completed": [],
}


def _build_web_link(job_id: int, release_id: int | None = None) -> str:
    if release_id is None:
        return f"/jobs/{int(job_id)}"
    return f"/jobs/{int(job_id)}?release_id={int(release_id)}"


def build_publish_context_summary(*, row: dict[str, Any]) -> dict[str, Any]:
    job_id = int(row["job_id"])
    release_id = int(row["release_id"]) if row.get("release_id") is not None else None
    publish_state = str(row.get("publish_state") or "unknown")
    reason_code = str(row.get("publish_reason_code") or "") or None
    reason_detail = str(row.get("publish_reason_detail") or "") or None
    actions = list(_ALLOWED_BY_STATE.get(publish_state, []))

    blocker = reason_code or reason_detail or ("manual_handoff_required" if publish_state.startswith("manual_handoff") else None)

    return {
        "target": {
            "job_id": job_id,
            "release_id": release_id,
            "channel_slug": row.get("channel_slug"),
            "release_title": row.get("release_title"),
        },
        "publish_state": publish_state,
        "reason": {"code": reason_code, "detail": reason_detail, "blocker": blocker},
        "available_next_actions": actions,
        "web_link": _build_web_link(job_id=job_id, release_id=release_id),
        "compact": True,
    }


def load_publish_decision_context(conn: Any, *, job_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            j.id AS job_id,
            j.release_id AS release_id,
            j.publish_state AS publish_state,
            j.publish_reason_code AS publish_reason_code,
            j.publish_reason_detail AS publish_reason_detail,
            r.title AS release_title,
            c.slug AS channel_slug
        FROM jobs j
        JOIN releases r ON r.id = j.release_id
        JOIN channels c ON c.id = r.channel_id
        WHERE j.id = ?
        LIMIT 1
        """,
        (int(job_id),),
    ).fetchone()
    if row is None:
        return None
    return build_publish_context_summary(row=dict(row))
