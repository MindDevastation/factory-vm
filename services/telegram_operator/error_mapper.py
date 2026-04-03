from __future__ import annotations

from .errors import (
    E6A_ACTION_EXPIRED,
    E6A_CHAT_BINDING_DISABLED,
    E6A_CHAT_BINDING_MISSING,
    E6A_CHAT_BINDING_REVOKED,
    E6A_GATEWAY_CONTEXT_INVALID,
    E6A_OPERATOR_IDENTITY_MISMATCH,
    E6A_OPERATOR_INACTIVE,
    E6A_OPERATOR_REVOKED,
    E6A_PERMISSION_DENIED,
    E6A_SYSTEM_UNAVAILABLE,
    E6A_TARGET_NOT_FOUND,
    E6A_TARGET_STALE,
    E6A_TELEGRAM_IDENTITY_UNBOUND,
)

_SAFE_MESSAGES = {
    E6A_TELEGRAM_IDENTITY_UNBOUND: "Your Telegram account is not enrolled.",
    E6A_OPERATOR_INACTIVE: "Operator access is inactive.",
    E6A_OPERATOR_REVOKED: "Operator access is revoked.",
    E6A_CHAT_BINDING_MISSING: "This chat/thread is not trusted.",
    E6A_CHAT_BINDING_DISABLED: "This chat binding is disabled.",
    E6A_CHAT_BINDING_REVOKED: "This chat binding is revoked.",
    E6A_OPERATOR_IDENTITY_MISMATCH: "Telegram identity does not match the requested operator.",
    E6A_PERMISSION_DENIED: "Permission denied for requested action class.",
    E6A_ACTION_EXPIRED: "Action request has expired.",
    E6A_TARGET_STALE: "Target context is stale.",
    E6A_TARGET_NOT_FOUND: "Target entity was not found.",
    E6A_GATEWAY_CONTEXT_INVALID: "Gateway context is invalid.",
    E6A_SYSTEM_UNAVAILABLE: "System temporarily unavailable.",
}


def to_telegram_safe_error(code: str) -> dict[str, str]:
    return {
        "code": str(code),
        "class": "operator_actionable" if code in _SAFE_MESSAGES else "system",
        "message": _SAFE_MESSAGES.get(str(code), "Unable to process request right now."),
    }
