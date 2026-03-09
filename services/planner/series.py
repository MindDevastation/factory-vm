from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from services.planner.duration import DurationValidationError, parse_duration
from services.planner.time_normalization import normalize_publish_at

_KYIV_TZ = ZoneInfo("Europe/Kyiv")


class TimeNormalizationService(Protocol):
    def normalize_publish_at(self, publish_at: str) -> str: ...


class DefaultTimeNormalizationService:
    def normalize_publish_at(self, publish_at: str) -> str:
        return normalize_publish_at(publish_at)


@dataclass(frozen=True)
class BulkSeriesInput:
    count: int
    start_publish_at: str | None
    step: str | None


class BulkSeriesValidationError(ValueError):
    """Raised when bulk planner series parameters are invalid."""


def build_bulk_publish_ats(payload: BulkSeriesInput) -> list[str | None]:
    if payload.count < 1 or payload.count > 5000:
        raise BulkSeriesValidationError("count must be in range 1..5000")

    if payload.start_publish_at is None:
        return [None] * payload.count

    step_delta: timedelta | None = None
    if payload.step is not None:
        try:
            step_delta = parse_duration(payload.step)
        except DurationValidationError as exc:
            raise BulkSeriesValidationError("step must be a supported ISO8601 duration") from exc

    try:
        return generate_series_publish_at(
            count=payload.count,
            start_publish_at=payload.start_publish_at,
            step=step_delta,
        )
    except ValueError as exc:
        raise BulkSeriesValidationError(str(exc)) from exc


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
