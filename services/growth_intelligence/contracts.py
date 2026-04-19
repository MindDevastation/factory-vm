from __future__ import annotations

import json
from typing import Any, Final

SOURCE_CLASSES: Final[tuple[str, ...]] = ("OFFICIAL", "PRACTITIONER", "INTERNAL")
SOURCE_TRUST_LEVELS: Final[tuple[str, ...]] = ("A", "B", "C", "D")
IMPACT_CONFIDENCE_LEVELS: Final[tuple[str, ...]] = ("High", "Medium", "Low")
FEATURE_FLAG_NAMES: Final[tuple[str, ...]] = (
    "growth_intelligence_enabled",
    "planning_digest_enabled",
    "planner_handoff_enabled",
    "export_enabled",
    "assisted_planning_enabled",
)
TYPED_LINKED_ACTIONS: Final[tuple[str, ...]] = (
    "OPEN_ANALYZER",
    "OPEN_PLANNER",
    "OPEN_METADATA_SURFACE",
    "SEND_TELEGRAM_DIGEST",
    "EXPORT_PLAN",
)
_SOURCE_TRUST_BY_CLASS: Final[dict[str, tuple[str, ...]]] = {
    "OFFICIAL": ("A", "B", "C"),
    "PRACTITIONER": ("B", "C", "D"),
    "INTERNAL": ("A", "B", "C", "D"),
}


def ensure_source_class(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in SOURCE_CLASSES:
        raise ValueError(f"source_class must be one of {', '.join(SOURCE_CLASSES)}")
    return normalized


def ensure_source_trust(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in SOURCE_TRUST_LEVELS:
        raise ValueError(f"source_trust must be one of {', '.join(SOURCE_TRUST_LEVELS)}")
    return normalized


def ensure_impact_confidence(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized not in IMPACT_CONFIDENCE_LEVELS:
        raise ValueError(f"impact_confidence must be one of {', '.join(IMPACT_CONFIDENCE_LEVELS)}")
    return normalized


def ensure_source_hierarchy(*, source_class: str, source_trust: str) -> tuple[str, str]:
    normalized_class = ensure_source_class(source_class)
    normalized_trust = ensure_source_trust(source_trust)
    if normalized_trust not in _SOURCE_TRUST_BY_CLASS[normalized_class]:
        raise ValueError(f"source_trust {normalized_trust} is not allowed for source_class {normalized_class}")
    return normalized_class, normalized_trust


def ensure_json_text(value: Any, *, field_name: str, default: Any) -> str:
    if value is None:
        value = default
    if isinstance(value, str):
        json.loads(value)
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def ensure_boolean_flag_map(value: dict[str, Any]) -> dict[str, int]:
    normalized: dict[str, int] = {}
    for key in FEATURE_FLAG_NAMES:
        raw = value.get(key)
        if not isinstance(raw, bool):
            raise ValueError(f"{key} must be boolean")
        normalized[key] = 1 if raw else 0
    extra = sorted(set(value.keys()) - set(FEATURE_FLAG_NAMES))
    if extra:
        raise ValueError(f"unsupported feature flag fields: {', '.join(extra)}")
    return normalized


def contracts_payload() -> dict[str, Any]:
    return {
        "source_class": list(SOURCE_CLASSES),
        "source_trust": list(SOURCE_TRUST_LEVELS),
        "impact_confidence": list(IMPACT_CONFIDENCE_LEVELS),
        "feature_flags": list(FEATURE_FLAG_NAMES),
        "typed_linked_actions": list(TYPED_LINKED_ACTIONS),
    }
