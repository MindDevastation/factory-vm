from __future__ import annotations

from typing import Any, Final

RECORD_TYPES: Final[tuple[str, ...]] = ("prompt_template", "snippet_block")
RECORD_STATUSES: Final[tuple[str, ...]] = ("draft", "active", "inactive", "archived")
VALIDATION_STATUSES: Final[tuple[str, ...]] = ("VALID", "INVALID", "UNKNOWN")
SAFETY_CLASSES: Final[tuple[str, ...]] = (
    "standard",
    "secret",
    "operator_only",
    "derived_from_context",
    "multiline_longform",
)
BINDING_SCOPES: Final[tuple[str, ...]] = ("global", "workflow", "channel", "item")
BINDING_STATUSES: Final[tuple[str, ...]] = ("active", "inactive")

_ALLOWED_STATUS_TRANSITIONS: Final[dict[str, set[str]]] = {
    "draft": {"active", "inactive", "archived"},
    "active": {"inactive", "archived"},
    "inactive": {"active", "archived"},
    "archived": set(),
}


def ensure_non_empty(value: Any, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    return text


def ensure_record_type(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in RECORD_TYPES:
        raise ValueError(f"record_type must be one of {', '.join(RECORD_TYPES)}")
    return normalized


def ensure_record_status(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in RECORD_STATUSES:
        raise ValueError(f"status must be one of {', '.join(RECORD_STATUSES)}")
    return normalized


def ensure_validation_status(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in VALIDATION_STATUSES:
        raise ValueError(f"validation_status must be one of {', '.join(VALIDATION_STATUSES)}")
    return normalized


def ensure_safety_class(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in SAFETY_CLASSES:
        raise ValueError(f"safety_class must be one of {', '.join(SAFETY_CLASSES)}")
    return normalized


def ensure_lifecycle_transition(*, current_status: str, new_status: str) -> None:
    if current_status == new_status:
        return
    allowed = _ALLOWED_STATUS_TRANSITIONS.get(current_status, set())
    if new_status not in allowed:
        raise ValueError(f"invalid lifecycle transition: {current_status} -> {new_status}")


def ensure_binding_scope(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in BINDING_SCOPES:
        raise ValueError(f"binding_scope must be one of {', '.join(BINDING_SCOPES)}")
    return normalized


def ensure_binding_status(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in BINDING_STATUSES:
        raise ValueError(f"binding_status must be one of {', '.join(BINDING_STATUSES)}")
    return normalized


def contracts_payload() -> dict[str, Any]:
    return {
        "record_type": list(RECORD_TYPES),
        "status": list(RECORD_STATUSES),
        "validation_status": list(VALIDATION_STATUSES),
        "safety_class": list(SAFETY_CLASSES),
        "binding_scope": list(BINDING_SCOPES),
        "binding_status": list(BINDING_STATUSES),
    }


def bridge_policy_payload() -> dict[str, Any]:
    return {
        "mode": "bridge_safe_foundation",
        "runtime_bridge_execution": "not_implemented",
        "authoritative_surfaces": {
            "title_templates": "authoritative",
            "description_templates": "authoritative",
            "video_tag_presets": "authoritative",
            "channel_visual_style_templates": "authoritative",
        },
        "prompt_registry_role": "policy_and_registry_foundation_only",
        "future_work": {
            "bridge_migration_slices_expected": True,
            "runtime_binding_or_resolution_in_this_slice": False,
        },
    }
