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
LINKED_ACTION_TYPES: Final[tuple[str, ...]] = ("ui_action", "api_endpoint", "workflow", "codex_prompt", "external_note")
LINKED_ACTION_STATUSES: Final[tuple[str, ...]] = ("active", "inactive")
LINKED_ACTION_TARGET_KINDS: Final[tuple[str, ...]] = ("route", "endpoint", "workflow", "prompt_template", "note")
USAGE_EVENT_TYPES: Final[tuple[str, ...]] = ("version_preview", "resolved_preview")
USAGE_EVENT_SOURCES: Final[tuple[str, ...]] = ("api",)
USAGE_EVENT_STATUSES: Final[tuple[str, ...]] = ("OK", "INVALID", "ERROR")
IMPORT_MODES: Final[tuple[str, ...]] = ("merge_only",)
EXPORT_SCHEMA_VERSION: Final[str] = "prompt_registry_export_v1"

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


def ensure_linked_action_type(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in LINKED_ACTION_TYPES:
        raise ValueError(f"action_type must be one of {', '.join(LINKED_ACTION_TYPES)}")
    return normalized


def ensure_linked_action_status(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in LINKED_ACTION_STATUSES:
        raise ValueError(f"action_status must be one of {', '.join(LINKED_ACTION_STATUSES)}")
    return normalized


def ensure_linked_action_target_kind(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in LINKED_ACTION_TARGET_KINDS:
        raise ValueError(f"target_kind must be one of {', '.join(LINKED_ACTION_TARGET_KINDS)}")
    return normalized


def ensure_usage_event_type(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in USAGE_EVENT_TYPES:
        raise ValueError(f"event_type must be one of {', '.join(USAGE_EVENT_TYPES)}")
    return normalized


def ensure_usage_event_source(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in USAGE_EVENT_SOURCES:
        raise ValueError(f"source must be one of {', '.join(USAGE_EVENT_SOURCES)}")
    return normalized


def ensure_usage_event_status(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in USAGE_EVENT_STATUSES:
        raise ValueError(f"status must be one of {', '.join(USAGE_EVENT_STATUSES)}")
    return normalized


def ensure_import_mode(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized not in IMPORT_MODES:
        raise ValueError(f"mode must be one of {', '.join(IMPORT_MODES)}")
    return normalized


def contracts_payload() -> dict[str, Any]:
    return {
        "record_type": list(RECORD_TYPES),
        "status": list(RECORD_STATUSES),
        "validation_status": list(VALIDATION_STATUSES),
        "safety_class": list(SAFETY_CLASSES),
        "binding_scope": list(BINDING_SCOPES),
        "binding_status": list(BINDING_STATUSES),
        "linked_action_type": list(LINKED_ACTION_TYPES),
        "linked_action_status": list(LINKED_ACTION_STATUSES),
        "linked_action_target_kind": list(LINKED_ACTION_TARGET_KINDS),
        "usage_event_type": list(USAGE_EVENT_TYPES),
        "usage_event_source": list(USAGE_EVENT_SOURCES),
        "usage_event_status": list(USAGE_EVENT_STATUSES),
        "import_mode": list(IMPORT_MODES),
        "export_schema_version": EXPORT_SCHEMA_VERSION,
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
