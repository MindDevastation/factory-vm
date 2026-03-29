from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class PublishRuntimeSnapshot:
    job_id: int
    job_state: str
    publish_state: str | None
    publish_target_visibility: str | None
    publish_delivery_mode_effective: str | None
    publish_resolved_scope: str | None
    publish_reason_code: str | None
    publish_reason_detail: str | None
    publish_attempt_count: int
    publish_retry_at: float | None
    publish_last_transition_at: float | None
    publish_hold_active: bool


def build_publish_runtime_snapshot(job_row: Mapping[str, Any]) -> PublishRuntimeSnapshot:
    return PublishRuntimeSnapshot(
        job_id=int(job_row["id"]),
        job_state=str(job_row.get("state") or ""),
        publish_state=(str(job_row["publish_state"]) if job_row.get("publish_state") is not None else None),
        publish_target_visibility=(
            str(job_row["publish_target_visibility"]) if job_row.get("publish_target_visibility") is not None else None
        ),
        publish_delivery_mode_effective=(
            str(job_row["publish_delivery_mode_effective"]) if job_row.get("publish_delivery_mode_effective") is not None else None
        ),
        publish_resolved_scope=(str(job_row["publish_resolved_scope"]) if job_row.get("publish_resolved_scope") is not None else None),
        publish_reason_code=(str(job_row["publish_reason_code"]) if job_row.get("publish_reason_code") is not None else None),
        publish_reason_detail=(str(job_row["publish_reason_detail"]) if job_row.get("publish_reason_detail") is not None else None),
        publish_attempt_count=int(job_row.get("publish_attempt_count") or 0),
        publish_retry_at=(float(job_row["publish_retry_at"]) if job_row.get("publish_retry_at") is not None else None),
        publish_last_transition_at=(
            float(job_row["publish_last_transition_at"]) if job_row.get("publish_last_transition_at") is not None else None
        ),
        publish_hold_active=bool(job_row.get("publish_hold_active") or 0),
    )


__all__ = ["PublishRuntimeSnapshot", "build_publish_runtime_snapshot"]
