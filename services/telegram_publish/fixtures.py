from __future__ import annotations

from typing import Any


def build_manual_handoff_fixture(*, job_id: int = 1, release_id: int = 10) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "release_id": release_id,
        "publish_state": "manual_handoff_pending",
        "publish_reason_code": "policy_requires_manual",
        "publish_reason_detail": "manual handoff required",
        "allowed_next_actions": ["approve", "reject", "ack_manual_handoff"],
    }
