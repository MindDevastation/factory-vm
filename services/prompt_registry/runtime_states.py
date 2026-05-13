from __future__ import annotations

RUNTIME_STATES = (
    "PREPARED",
    "PREFLIGHT_REJECTED",
    "CONFIRMATION_REQUIRED",
    "ADMITTED",
    "DISPATCHED",
    "RUNNING",
    "RETRY_PENDING",
    "SUCCEEDED",
    "FAILED_TERMINAL",
    "CANCELLED",
    "STALE_BLOCKED",
    "CONFLICT_BLOCKED",
)

TERMINAL_RUNTIME_STATES = (
    "PREFLIGHT_REJECTED",
    "SUCCEEDED",
    "FAILED_TERMINAL",
    "CANCELLED",
    "STALE_BLOCKED",
    "CONFLICT_BLOCKED",
)

_ALLOWED_TRANSITIONS = {
    "PREPARED": {"PREFLIGHT_REJECTED", "CONFIRMATION_REQUIRED", "STALE_BLOCKED", "CONFLICT_BLOCKED"},
    "CONFIRMATION_REQUIRED": {"ADMITTED", "CANCELLED", "STALE_BLOCKED", "CONFLICT_BLOCKED"},
    "ADMITTED": {"DISPATCHED", "RUNNING", "SUCCEEDED", "FAILED_TERMINAL", "CANCELLED"},
    "DISPATCHED": {"RUNNING", "SUCCEEDED", "RETRY_PENDING", "FAILED_TERMINAL", "CANCELLED"},
    "RUNNING": {"SUCCEEDED", "RETRY_PENDING", "FAILED_TERMINAL", "CANCELLED"},
    "RETRY_PENDING": {"ADMITTED", "FAILED_TERMINAL", "CANCELLED", "STALE_BLOCKED", "CONFLICT_BLOCKED"},
}

def is_runtime_state(value: str) -> bool:
    return str(value or "").strip() in RUNTIME_STATES

def is_terminal_runtime_state(state: str) -> bool:
    normalized = str(state or "").strip()
    if normalized not in RUNTIME_STATES:
        raise ValueError(f"Unknown runtime state: {state}")
    return normalized in TERMINAL_RUNTIME_STATES

def is_allowed_runtime_transition(from_state: str, to_state: str) -> bool:
    source = str(from_state or "").strip()
    target = str(to_state or "").strip()
    if source not in RUNTIME_STATES:
        raise ValueError(f"Unknown runtime state: {from_state}")
    if target not in RUNTIME_STATES:
        raise ValueError(f"Unknown runtime state: {to_state}")
    return target in _ALLOWED_TRANSITIONS.get(source, set())
