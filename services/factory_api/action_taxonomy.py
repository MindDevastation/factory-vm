from __future__ import annotations

from typing import Any


_ACTION_CLASS_BY_ACTION: dict[str, str] = {
    "refresh": "READ_ONLY",
    "recompute": "READ_ONLY",
    "open": "READ_ONLY",
    "retry": "LOW_RISK_MUTATE",
    "apply": "LOW_RISK_MUTATE",
    "confirm": "GUARDED_MUTATE",
    "approve": "GUARDED_MUTATE",
    "reject": "GUARDED_MUTATE",
    "cancel": "HIGH_RISK_MUTATE",
    "reclaim": "HIGH_RISK_MUTATE",
    "cleanup": "HIGH_RISK_MUTATE",
    "restart": "HIGH_RISK_MUTATE",
    "batch_apply": "BATCH_MUTATE",
    "batch_execute": "BATCH_MUTATE",
}

_PATTERN_FAMILY_BY_ACTION_CLASS: dict[str, str] = {
    "READ_ONLY": "DIRECT_READ_ONLY",
    "LOW_RISK_MUTATE": "PREVIEW_TO_APPLY",
    "GUARDED_MUTATE": "PREVIEW_TO_CONFIRM_TO_EXECUTE",
    "HIGH_RISK_MUTATE": "DIRECT_MUTATE_WITH_CONFIRMATION",
    "BATCH_MUTATE": "PREVIEW_TO_CONFIRM_TO_EXECUTE",
}

_RESULT_CLASS_BY_OUTCOME: dict[str, str] = {
    "ok": "SUCCEEDED",
    "success": "SUCCEEDED",
    "succeeded": "SUCCEEDED",
    "partial": "PARTIAL",
    "blocked": "BLOCKED",
    "stale": "STALE",
    "denied": "DENIED",
    "conflict": "DENIED",
    "error": "FAILED",
    "failed": "FAILED",
}

_REPRESENTATIVE_SURFACES: list[dict[str, str]] = [
    {"surface": "planner", "scope": "representative", "family": "planning"},
    {"surface": "publish", "scope": "representative", "family": "publish_ops"},
    {"surface": "visuals", "scope": "representative", "family": "visual_ops"},
    {"surface": "analytics", "scope": "representative", "family": "analytics_light_actions"},
    {"surface": "ops_recovery", "scope": "representative", "family": "ops_recovery"},
]


def classify_action_class(*, action: str) -> str:
    return _ACTION_CLASS_BY_ACTION.get(str(action or "").strip().lower(), "GUARDED_MUTATE")


def pattern_family_for_action(*, action: str) -> str:
    action_class = classify_action_class(action=action)
    return _PATTERN_FAMILY_BY_ACTION_CLASS.get(action_class, "PREVIEW_TO_CONFIRM_TO_EXECUTE")


def classify_result_class(*, outcome: str) -> str:
    return _RESULT_CLASS_BY_OUTCOME.get(str(outcome or "").strip().lower(), "FAILED")


def classify_stale_conflict(*, expected_version: str | None, actual_version: str | None) -> str:
    if not expected_version or not actual_version:
        return "UNKNOWN_VERSION_STATE"
    if expected_version == actual_version:
        return "CURRENT"
    expected_text = str(expected_version)
    actual_text = str(actual_version)
    if ":" not in expected_text and ":" not in actual_text:
        return "STALE"
    if expected_text.split(":", 1)[0] == actual_text.split(":", 1)[0]:
        return "STALE"
    return "CONFLICT"


def representative_surfaces_matrix() -> list[dict[str, str]]:
    return list(_REPRESENTATIVE_SURFACES)


def action_taxonomy_catalog() -> dict[str, Any]:
    return {
        "action_classes": dict(_ACTION_CLASS_BY_ACTION),
        "pattern_families": dict(_PATTERN_FAMILY_BY_ACTION_CLASS),
        "result_classes": dict(_RESULT_CLASS_BY_OUTCOME),
        "representative_surfaces": representative_surfaces_matrix(),
    }
