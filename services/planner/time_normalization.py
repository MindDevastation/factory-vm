from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

_KYIV_TZ = ZoneInfo("Europe/Kyiv")
_ISO_PUBLISH_AT_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:\.\d{1,6})?)?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})?$"
)


class PublishAtValidationError(ValueError):
    """Raised when publish_at is not a supported ISO8601 datetime string."""


def normalize_publish_at(publish_at: str) -> str:
    """Normalize publish_at to ISO8601 with explicit offset and seconds.

    Supported input:
    - ISO8601 with offset or Z (e.g. 2025-01-01T10:30Z, 2025-01-01T10:30:45+02:00)
    - ISO8601 without offset, interpreted as Europe/Kyiv local time.

    Output is deterministic ISO8601 with second precision and explicit UTC offset.
    """

    if not isinstance(publish_at, str):
        raise PublishAtValidationError("publish_at must be an ISO8601 string")

    value = publish_at.strip()
    match = _ISO_PUBLISH_AT_RE.fullmatch(value)
    if not match:
        raise PublishAtValidationError("publish_at must be ISO8601 datetime")

    normalized_input = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized_input)
    except ValueError as exc:
        raise PublishAtValidationError("publish_at must be ISO8601 datetime") from exc

    if match.group("offset") is None:
        dt = dt.replace(tzinfo=_KYIV_TZ)

    return dt.isoformat(timespec="seconds")
