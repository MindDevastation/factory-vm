from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence


def assemble_publish_queue_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Build deterministic summary blocks for publish queue payloads."""

    state_counts = Counter(str(row.get("publish_state") or "unknown") for row in rows)

    blocked_states = {"policy_blocked"}
    failed_states = {"publish_failed_terminal"}
    manual_states = {"manual_handoff_pending", "manual_handoff_acknowledged"}
    queue_states = {
        "private_uploaded",
        "waiting_for_schedule",
        "ready_to_publish",
        "publish_in_progress",
        "retry_pending",
    }

    return {
        "total": len(rows),
        "by_publish_state": {key: state_counts[key] for key in sorted(state_counts)},
        "views": {
            "queue": sum(state_counts[s] for s in sorted(queue_states)),
            "blocked": sum(state_counts[s] for s in sorted(blocked_states)),
            "failed": sum(state_counts[s] for s in sorted(failed_states)),
            "manual": sum(state_counts[s] for s in sorted(manual_states)),
            "health": len(rows),
        },
        "signals": {
            "drift_detected": state_counts["publish_state_drift_detected"],
            "hold_active": sum(1 for row in rows if bool(row.get("publish_hold_active") or 0)),
            "retry_pending": state_counts["retry_pending"],
        },
    }


__all__ = ["assemble_publish_queue_summary"]
