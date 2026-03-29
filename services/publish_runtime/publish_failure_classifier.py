from __future__ import annotations

import socket
from typing import Tuple

try:
    from googleapiclient.errors import HttpError
except Exception:  # pragma: no cover - optional dependency
    HttpError = None  # type: ignore[assignment]


RETRIABLE_ERROR_CODES: frozenset[str] = frozenset(
    {
        "timeout",
        "rate_limited",
        "transient_api_error",
        "unknown_external_error",
    }
)

TERMINAL_ERROR_CODES: frozenset[str] = frozenset(
    {
        "invalid_configuration",
        "terminal_publish_rejection",
    }
)


def _extract_http_status(exc: Exception) -> int | None:
    if HttpError is not None and isinstance(exc, HttpError):
        status = getattr(getattr(exc, "resp", None), "status", None)
        if isinstance(status, int):
            return status
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    return None


def classify_publish_failure(exc: Exception) -> Tuple[str, str]:
    if isinstance(exc, TimeoutError | socket.timeout):
        return "timeout", "retriable"

    status = _extract_http_status(exc)
    if status == 429:
        return "rate_limited", "retriable"
    if status in {500, 502, 503, 504}:
        return "transient_api_error", "retriable"
    if status in {400, 401, 403}:
        return "invalid_configuration", "terminal"
    if status in {404, 409, 422}:
        return "terminal_publish_rejection", "terminal"

    msg = str(exc).lower()
    if "token" in msg or "credential" in msg or "auth" in msg or "permission" in msg:
        return "invalid_configuration", "terminal"
    if "invalid" in msg or "forbidden" in msg:
        return "terminal_publish_rejection", "terminal"
    return "unknown_external_error", "retriable"


__all__ = [
    "RETRIABLE_ERROR_CODES",
    "TERMINAL_ERROR_CODES",
    "classify_publish_failure",
]
