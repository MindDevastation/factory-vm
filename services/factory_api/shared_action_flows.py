from __future__ import annotations

from typing import Any

from services.factory_api.action_taxonomy import (
    classify_action_class,
    classify_result_class,
    classify_stale_conflict,
    representative_surfaces_matrix,
)


CANONICAL_ACTION_CLASSES = (
    "READ_ONLY",
    "LOW_RISK_MUTATE",
    "GUARDED_MUTATE",
    "HIGH_RISK_MUTATE",
    "BATCH_MUTATE",
)


def canonical_action_class_for_action(*, action: str) -> str:
    return classify_action_class(action=action)


def preview_to_apply_contract(*, action: str, preview_scope: str) -> dict[str, Any]:
    return {
        "pattern": "PREVIEW_TO_APPLY",
        "action": action,
        "action_class": canonical_action_class_for_action(action=action),
        "preview_scope": preview_scope,
        "apply_step": "APPLY",
    }


def preview_confirm_execute_contract(*, action: str, preview_scope: str) -> dict[str, Any]:
    return {
        "pattern": "PREVIEW_TO_CONFIRM_TO_EXECUTE",
        "action": action,
        "action_class": canonical_action_class_for_action(action=action),
        "preview_scope": preview_scope,
        "confirm_step": "CONFIRM",
        "execute_step": "EXECUTE",
    }


def direct_mutate_with_confirmation_contract(*, action: str, target_scope: str) -> dict[str, Any]:
    return {
        "pattern": "DIRECT_MUTATE_WITH_CONFIRMATION",
        "action": action,
        "action_class": canonical_action_class_for_action(action=action),
        "target_scope": target_scope,
        "confirm_step": "CONFIRM",
        "execute_step": "EXECUTE",
    }


def stale_refusal_or_refresh_contract(*, expected_version: str, actual_version: str) -> dict[str, Any]:
    stale_state = classify_stale_conflict(expected_version=expected_version, actual_version=actual_version)
    return {
        "pattern": "STALE_REFUSAL_OR_REFRESH",
        "status": stale_state,
        "expected_version": expected_version,
        "actual_version": actual_version,
        "next_action": "refresh" if stale_state in {"STALE", "CONFLICT"} else "continue",
    }


def partial_result_summary_contract(*, succeeded: list[str], failed: list[str], unresolved: list[str]) -> dict[str, Any]:
    result_class = "PARTIAL" if failed or unresolved else "SUCCEEDED"
    return {
        "pattern": "PARTIAL_RESULT_SUMMARY",
        "result_class": classify_result_class(outcome=result_class),
        "succeeded": succeeded,
        "failed": failed,
        "unresolved": unresolved,
        "next_step": "review failures" if failed or unresolved else "continue",
    }


def batch_preview_execute_contract(*, targets: list[str], action: str, requires_preview: bool = True) -> dict[str, Any]:
    return {
        "action_class": "BATCH_MUTATE",
        "pattern": "PREVIEW_TO_CONFIRM_TO_EXECUTE",
        "action": str(action),
        "target_count": len(targets),
        "targets": targets,
        "requires_preview": bool(requires_preview),
        "requires_confirm": True,
        "no_silent_overwrite": True,
    }


def result_continuation_contract(*, result_class: str, what_changed: list[str], what_failed: list[str], unresolved: list[str], next_step: str, return_path: str) -> dict[str, Any]:
    return {
        "result_class": classify_result_class(outcome=result_class),
        "what_happened": "partial_or_mixed" if what_failed or unresolved else "completed",
        "what_changed": what_changed,
        "what_failed": what_failed,
        "unresolved": unresolved,
        "next_step": next_step,
        "return_path": return_path,
        "continuation_supported": True,
    }


def cross_domain_consistency_contract() -> dict[str, Any]:
    return {
        "surfaces": representative_surfaces_matrix(),
        "confirmation_affordance": "class_consistent",
        "stale_conflict_treatment": "explicit",
        "result_messaging": "continuation_first",
    }
