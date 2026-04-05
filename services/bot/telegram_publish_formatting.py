from __future__ import annotations

from typing import Any

_STATE_LABELS = {
    "policy_blocked": "Policy blocked",
    "published_public": "Published (public)",
    "published_unlisted": "Published (unlisted)",
    "publish_failed_terminal": "Publish failed",
    "manual_handoff_pending": "Manual handoff pending",
    "waiting_for_schedule": "Waiting for schedule",
    "publish_state_drift_detected": "Drift detected",
}

_REASON_LABELS = {
    "global_pause_active": "Global pause is active",
    "retries_exhausted": "Retries exhausted",
    "missed_schedule_operator_review": "Missed schedule",
    "external_manual_publish_detected": "External manual publish detected",
    "operator_forced_manual": "Forced manual by operator",
    "invalid_configuration": "Invalid configuration",
    "terminal_publish_rejection": "Terminal publish rejection",
}

_NEXT_ACTIONS = {
    "policy_blocked": ["unblock", "hold", "review_policy"],
    "publish_failed_terminal": ["reset-failure", "move-to-manual"],
    "manual_handoff_pending": ["acknowledge", "mark-completed"],
    "waiting_for_schedule": ["reschedule", "move-to-manual"],
    "publish_state_drift_detected": ["move-to-manual", "mark-completed"],
}


def format_publish_state_label(publish_state: str | None) -> str:
    key = str(publish_state or "").strip()
    return _STATE_LABELS.get(key, key or "unknown")


def format_publish_reason_label(reason_code: str | None) -> str:
    key = str(reason_code or "").strip()
    return _REASON_LABELS.get(key, key or "none")


def format_next_actions(publish_state: str | None) -> str:
    key = str(publish_state or "").strip()
    actions = _NEXT_ACTIONS.get(key, [])
    return ", ".join(actions) if actions else "none"


def format_critical_event_message(*, family: str, item: dict[str, Any]) -> str:
    job_id = int(item.get("job_id") or 0)
    state_label = format_publish_state_label(str(item.get("publish_state") or ""))
    reason_label = format_publish_reason_label(str(item.get("publish_reason_code") or ""))
    next_actions = format_next_actions(str(item.get("publish_state") or ""))
    return (
        f"🚨 {family}\n"
        f"job_id={job_id}\n"
        f"state={state_label}\n"
        f"reason={reason_label}\n"
        f"next_actions={next_actions}"
    )


__all__ = [
    "format_publish_state_label",
    "format_publish_reason_label",
    "format_next_actions",
    "format_critical_event_message",
]
