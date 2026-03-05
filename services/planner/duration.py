from __future__ import annotations

import re
from datetime import timedelta

_DURATION_RE = re.compile(
    r"^P(?:"
    r"(?P<days>\d+)D"
    r"|T(?:(?P<hours>\d+)H(?:(?P<hours_minutes>\d+)M)?|(?P<minutes_only>\d+)M)"
    r")$"
)


class DurationValidationError(ValueError):
    """Raised when a duration does not match the supported ISO8601 subset."""


def parse_duration(value: str) -> timedelta:
    """Parse restricted ISO8601 duration subset into timedelta.

    Supported formats:
    - PnD
    - PTnH
    - PTnM
    - PTnHnM
    """

    if not isinstance(value, str):
        raise DurationValidationError("duration must be an ISO8601 string")

    match = _DURATION_RE.fullmatch(value.strip())
    if not match:
        raise DurationValidationError("duration must be one of PnD, PTnH, PTnM, PTnHnM")

    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("hours_minutes") or match.group("minutes_only") or 0)

    try:
        return timedelta(days=days, hours=hours, minutes=minutes)
    except OverflowError as exc:
        raise DurationValidationError("duration value is too large") from exc
