from __future__ import annotations

from typing import Any


_ACTION_CLASS_BY_ACTION: dict[str, str] = {
    "refresh": "SAFE_REFRESH",
    "recompute": "SAFE_RECOMPUTE",
    "retry": "MUTATE_RETRY",
    "cancel": "MUTATE_CANCEL",
    "reclaim": "MUTATE_RECLAIM",
    "cleanup": "MUTATE_CLEANUP",
    "restart": "MUTATE_RESTART",
    "approve": "MUTATE_APPROVE",
    "reject": "MUTATE_REJECT",
}

_PATTERN_FAMILY_BY_ACTION_CLASS: dict[str, str] = {
    "SAFE_REFRESH": "INLINE_SAFE_ACTION",
    "SAFE_RECOMPUTE": "INLINE_SAFE_ACTION",
    "MUTATE_RETRY": "PREVIEW_CONFIRM_EXECUTE",
    "MUTATE_CANCEL": "PREVIEW_CONFIRM_EXECUTE",
    "MUTATE_RECLAIM": "PREVIEW_CONFIRM_EXECUTE",
    "MUTATE_CLEANUP": "PREVIEW_CONFIRM_EXECUTE",
    "MUTATE_RESTART": "PREVIEW_CONFIRM_EXECUTE",
    "MUTATE_APPROVE": "PREVIEW_CONFIRM_EXECUTE",
    "MUTATE_REJECT": "PREVIEW_CONFIRM_EXECUTE",
}

_RESULT_CLASS_BY_OUTCOME: dict[str, str] = {
    "ok": "SUCCESS",
    "success": "SUCCESS",
    "partial": "PARTIAL",
    "blocked": "BLOCKED",
    "stale": "STALE",
    "conflict": "CONFLICT",
    "error": "ERROR",
    "failed": "ERROR",
}

_REPRESENTATIVE_SURFACES: list[dict[str, str]] = [
    {"surface": "planner", "scope": "representative", "family": "planning"},
    {"surface": "publish", "scope": "representative", "family": "publish_ops"},
    {"surface": "visuals", "scope": "representative", "family": "visual_ops"},
    {"surface": "analytics", "scope": "representative", "family": "analytics_light_actions"},
    {"surface": "ops_recovery", "scope": "representative", "family": "ops_recovery"},
]


def classify_action_class(*, action: str) -> str:
    return _ACTION_CLASS_BY_ACTION.get(str(action or "").strip().lower(), "UNKNOWN_ACTION_CLASS")


def pattern_family_for_action(*, action: str) -> str:
    action_class = classify_action_class(action=action)
    return _PATTERN_FAMILY_BY_ACTION_CLASS.get(action_class, "UNCLASSIFIED_PATTERN")


def classify_result_class(*, outcome: str) -> str:
    return _RESULT_CLASS_BY_OUTCOME.get(str(outcome or "").strip().lower(), "ERROR")


def classify_stale_conflict(*, expected_version: str | None, actual_version: str | None) -> str:
    if not expected_version or not actual_version:
        return "UNKNOWN_VERSION_STATE"
    if expected_version == actual_version:
        return "CURRENT"
    if expected_version.split(":", 1)[0] == actual_version.split(":", 1)[0]:
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
