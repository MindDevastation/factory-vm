from __future__ import annotations

from .literals import INBOX_LIFECYCLE_STATES, ensure_lifecycle_state

_ALLOWED = {
    "ACTIVE": {"SUPERSEDED", "RESOLVED", "EXPIRED", "INFORMATIONAL", "ACTIVE"},
    "SUPERSEDED": {"RESOLVED", "EXPIRED", "SUPERSEDED"},
    "RESOLVED": {"RESOLVED"},
    "EXPIRED": {"EXPIRED"},
    "INFORMATIONAL": {"RESOLVED", "EXPIRED", "INFORMATIONAL"},
}


def can_transition(*, from_state: str, to_state: str) -> bool:
    f = ensure_lifecycle_state(from_state)
    t = ensure_lifecycle_state(to_state)
    return t in _ALLOWED.get(f, set())


def require_transition(*, from_state: str, to_state: str) -> str:
    if not can_transition(from_state=from_state, to_state=to_state):
        raise ValueError(f"invalid lifecycle transition: {from_state} -> {to_state}")
    return ensure_lifecycle_state(to_state)
