from __future__ import annotations

from typing import Final

LOADING: Final[str] = "LOADING"
EMPTY: Final[str] = "EMPTY"
STALE: Final[str] = "STALE"
ERROR: Final[str] = "ERROR"
SUCCESS: Final[str] = "SUCCESS"
PARTIAL: Final[str] = "PARTIAL"
BLOCKED: Final[str] = "BLOCKED"


def state_template_catalog() -> dict[str, dict[str, str]]:
    return {
        LOADING: {"kind": LOADING, "intent": "fetching"},
        EMPTY: {"kind": EMPTY, "intent": "no_data"},
        STALE: {"kind": STALE, "intent": "refresh_required"},
        ERROR: {"kind": ERROR, "intent": "failure"},
        SUCCESS: {"kind": SUCCESS, "intent": "complete"},
        PARTIAL: {"kind": PARTIAL, "intent": "degraded_success"},
        BLOCKED: {"kind": BLOCKED, "intent": "operator_blocked"},
    }


def classify_state_template(*, has_data: bool, has_error: bool, is_stale: bool, is_blocked: bool, is_partial: bool) -> str:
    if has_error:
        return ERROR
    if is_blocked:
        return BLOCKED
    if is_stale:
        return STALE
    if is_partial:
        return PARTIAL
    if has_data:
        return SUCCESS
    return EMPTY
