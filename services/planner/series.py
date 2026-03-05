from __future__ import annotations

from datetime import datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from services.planner.duration import parse_duration
from services.planner.time_normalization import normalize_publish_at

_KYIV_TZ = ZoneInfo("Europe/Kyiv")


class TimeNormalizationService(Protocol):
    def normalize_publish_at(self, publish_at: str) -> str: ...


class DefaultTimeNormalizationService:
    def normalize_publish_at(self, publish_at: str) -> str:
        return normalize_publish_at(publish_at)


def generate_series_publish_at(
    *,
    count: int,
    start_publish_at: str | None,
    step: timedelta | None,
    time_normalization_service: TimeNormalizationService | None = None,
) -> list[str | None]:
    """Build publish_at values for series with Kyiv-local stepping and normalization."""

    if count < 0:
        raise ValueError("count must be >= 0")

    if start_publish_at is None:
        return [None] * count

    if count > 1 and step is None:
        raise ValueError("step is required when count > 1 and start_publish_at is set")
    if count > 1 and step is not None and step <= timedelta(0):
        raise ValueError("step must be > 0 when count > 1 and start_publish_at is set")

    step_value = step or timedelta(0)
    normalizer = time_normalization_service or DefaultTimeNormalizationService()

    normalized_start = normalizer.normalize_publish_at(start_publish_at)
    start_dt_kyiv = datetime.fromisoformat(normalized_start).astimezone(_KYIV_TZ)

    publish_ats: list[str] = []
    for index in range(count):
        scheduled = start_dt_kyiv + (index * step_value)
        publish_ats.append(normalizer.normalize_publish_at(scheduled.isoformat(timespec="seconds")))

    return publish_ats


def generate_bulk_publish_ats(*, count: int, start_publish_at: str | None, step: str | None) -> list[str | None]:
    if count < 1 or count > 5000:
        raise ValueError("count must be between 1 and 5000")

    if start_publish_at is None:
        return generate_series_publish_at(count=count, start_publish_at=None, step=None)

    step_delta = None
    if step is not None:
        step_delta = parse_duration(step)

    return generate_series_publish_at(count=count, start_publish_at=start_publish_at, step=step_delta)
