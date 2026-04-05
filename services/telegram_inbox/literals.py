from __future__ import annotations

from typing import Final

INBOX_MESSAGE_FAMILIES: Final[tuple[str, ...]] = (
    "CRITICAL_ALERT",
    "ACTIONABLE_ALERT",
    "INFORMATIONAL",
    "SUMMARY_DIGEST",
    "UNRESOLVED_FOLLOW_UP",
    "RESOLUTION_UPDATE",
)
INBOX_LIFECYCLE_STATES: Final[tuple[str, ...]] = ("ACTIVE", "SUPERSEDED", "RESOLVED", "EXPIRED", "INFORMATIONAL")
INBOX_SEVERITIES: Final[tuple[str, ...]] = ("CRITICAL", "HIGH", "MEDIUM", "LOW", "INFORMATIONAL")
INBOX_CATEGORIES: Final[tuple[str, ...]] = ("PUBLISH", "READINESS", "RECOVERY", "HEALTH", "FOLLOW_UP", "DIGEST", "INFORMATIONAL")
INBOX_ACTIONABILITY_CLASSES: Final[tuple[str, ...]] = ("INFORMATIONAL", "ACTION_REQUIRED", "ACK_REQUIRED", "ESCALATE_ONLY")
DELIVERY_BEHAVIORS: Final[tuple[str, ...]] = ("IMMEDIATE", "DIGEST", "FOLLOW_UP_ONLY", "SUPPRESSED")

_ALIASES: Final[dict[str, dict[str, str]]] = {
    "message_family": {
        "INFO_ONLY": "INFORMATIONAL",
    },
    "lifecycle_state": {
        "INFO_ONLY": "INFORMATIONAL",
    },
    "severity": {
        "INFO": "INFORMATIONAL",
    },
    "category": {
        "SYSTEM": "INFORMATIONAL",
    },
    "actionability_class": {
        "INFO_ONLY": "INFORMATIONAL",
        "ACTIONABLE": "ACTION_REQUIRED",
    },
}


def _ensure(value: str, *, field: str, allowed: tuple[str, ...]) -> str:
    normalized = str(value or "").strip().upper()
    normalized = _ALIASES.get(field, {}).get(normalized, normalized)
    if normalized not in allowed:
        raise ValueError(f"{field} must be one of {', '.join(allowed)}")
    return normalized


def ensure_message_family(value: str) -> str:
    return _ensure(value, field="message_family", allowed=INBOX_MESSAGE_FAMILIES)


def ensure_lifecycle_state(value: str) -> str:
    return _ensure(value, field="lifecycle_state", allowed=INBOX_LIFECYCLE_STATES)


def ensure_severity(value: str) -> str:
    return _ensure(value, field="severity", allowed=INBOX_SEVERITIES)


def ensure_category(value: str) -> str:
    return _ensure(value, field="category", allowed=INBOX_CATEGORIES)


def ensure_actionability_class(value: str) -> str:
    return _ensure(value, field="actionability_class", allowed=INBOX_ACTIONABILITY_CLASSES)


def ensure_delivery_behavior(value: str) -> str:
    return _ensure(value, field="delivery_behavior", allowed=DELIVERY_BEHAVIORS)
