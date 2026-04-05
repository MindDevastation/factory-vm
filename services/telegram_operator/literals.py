from __future__ import annotations

from typing import Final

TELEGRAM_ACCESS_STATUSES: Final[tuple[str, ...]] = ("ACTIVE", "INACTIVE", "REVOKED")
CHAT_BINDING_KINDS: Final[tuple[str, ...]] = ("PRIVATE_CHAT", "GROUP_CHAT", "GROUP_THREAD")
BINDING_STATUSES: Final[tuple[str, ...]] = ("PENDING", "ACTIVE", "DISABLED", "REVOKED")
ACTION_TRANSPORT_TYPES: Final[tuple[str, ...]] = ("COMMAND", "CALLBACK")
PERMISSION_ACCESS_CLASSES: Final[tuple[str, ...]] = (
    "READ_ONLY",
    "STANDARD_OPERATOR_MUTATE",
    "GUARDED_OPERATOR_MUTATE",
    "PRIVILEGED_OPERATOR_MUTATE",
)
GATEWAY_RESULTS: Final[tuple[str, ...]] = ("ALLOWED", "DENIED", "STALE", "EXPIRED", "INVALID")


def _ensure_literal(value: str, *, field_name: str, allowed: tuple[str, ...]) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise ValueError(f"{field_name} must be one of {', '.join(allowed)}")
    return normalized


def ensure_telegram_access_status(value: str) -> str:
    return _ensure_literal(value, field_name="telegram_access_status", allowed=TELEGRAM_ACCESS_STATUSES)


def ensure_chat_binding_kind(value: str) -> str:
    return _ensure_literal(value, field_name="chat_binding_kind", allowed=CHAT_BINDING_KINDS)


def ensure_binding_status(value: str) -> str:
    return _ensure_literal(value, field_name="binding_status", allowed=BINDING_STATUSES)


def ensure_action_transport_type(value: str) -> str:
    return _ensure_literal(value, field_name="action_transport_type", allowed=ACTION_TRANSPORT_TYPES)


def ensure_permission_access_class(value: str) -> str:
    return _ensure_literal(value, field_name="permission_access_class", allowed=PERMISSION_ACCESS_CLASSES)


def ensure_gateway_result(value: str) -> str:
    return _ensure_literal(value, field_name="gateway_result", allowed=GATEWAY_RESULTS)
