from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from services.planner.time_normalization import normalize_publish_at


@dataclass(frozen=True)
class ScheduleEvaluation:
    normalized_publish_at: str | None
    publish_scheduled_at_ts: float | None
    eligibility: str


def evaluate_publish_schedule(*, planned_at: str | None, now: datetime | None = None) -> ScheduleEvaluation:
    if planned_at is None or not str(planned_at).strip():
        return ScheduleEvaluation(normalized_publish_at=None, publish_scheduled_at_ts=None, eligibility="absent")

    normalized = normalize_publish_at(str(planned_at))
    scheduled_dt = datetime.fromisoformat(normalized).astimezone(timezone.utc)
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if scheduled_dt > now_utc:
        return ScheduleEvaluation(
            normalized_publish_at=normalized,
            publish_scheduled_at_ts=scheduled_dt.timestamp(),
            eligibility="future",
        )
    return ScheduleEvaluation(
        normalized_publish_at=normalized,
        publish_scheduled_at_ts=scheduled_dt.timestamp(),
        eligibility="past_due",
    )


__all__ = ["ScheduleEvaluation", "evaluate_publish_schedule"]
