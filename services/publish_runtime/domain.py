from __future__ import annotations

PUBLISH_STATE_VALUES: tuple[str, ...] = (
    "private_uploaded",
    "policy_blocked",
    "waiting_for_schedule",
    "ready_to_publish",
    "publish_in_progress",
    "retry_pending",
    "manual_handoff_pending",
    "manual_handoff_acknowledged",
    "manual_publish_completed",
    "published_public",
    "published_unlisted",
    "publish_failed_terminal",
    "publish_state_drift_detected",
)

PUBLISH_TARGET_VISIBILITY_VALUES: tuple[str, ...] = ("public", "unlisted")
PUBLISH_DELIVERY_MODE_EFFECTIVE_VALUES: tuple[str, ...] = ("automatic", "manual")
PUBLISH_RESOLVED_SCOPE_VALUES: tuple[str, ...] = ("project", "channel", "item")


def _normalize_literal(value: str) -> str:
    return str(value).strip().lower()


def _validate_literal(value: str, *, allowed_values: tuple[str, ...], field_name: str) -> str:
    normalized = _normalize_literal(value)
    if normalized not in allowed_values:
        raise ValueError(f"invalid {field_name}: {value!r}")
    return normalized


def normalize_publish_state(value: str) -> str:
    return _normalize_literal(value)


def normalize_publish_target_visibility(value: str) -> str:
    return _normalize_literal(value)


def normalize_publish_delivery_mode_effective(value: str) -> str:
    return _normalize_literal(value)


def normalize_publish_resolved_scope(value: str) -> str:
    return _normalize_literal(value)


def validate_publish_state(value: str) -> str:
    return _validate_literal(value, allowed_values=PUBLISH_STATE_VALUES, field_name="publish_state")


def validate_publish_target_visibility(value: str) -> str:
    return _validate_literal(
        value,
        allowed_values=PUBLISH_TARGET_VISIBILITY_VALUES,
        field_name="publish_target_visibility",
    )


def validate_publish_delivery_mode_effective(value: str) -> str:
    return _validate_literal(
        value,
        allowed_values=PUBLISH_DELIVERY_MODE_EFFECTIVE_VALUES,
        field_name="publish_delivery_mode_effective",
    )


def validate_publish_resolved_scope(value: str) -> str:
    return _validate_literal(value, allowed_values=PUBLISH_RESOLVED_SCOPE_VALUES, field_name="publish_resolved_scope")
