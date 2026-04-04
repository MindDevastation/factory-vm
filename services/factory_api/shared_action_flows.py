from __future__ import annotations

from typing import Any


CANONICAL_ACTION_CLASSES = (
    "READ_ONLY",
    "LOW_RISK_MUTATE",
    "GUARDED_MUTATE",
    "HIGH_RISK_MUTATE",
    "BATCH_MUTATE",
)


def canonical_action_class_for_action(*, action: str) -> str:
    normalized = str(action or "").strip().lower()
    if normalized in {"refresh", "recompute", "open"}:
        return "READ_ONLY"
    if normalized in {"retry", "apply"}:
        return "LOW_RISK_MUTATE"
    if normalized in {"confirm", "approve", "reject"}:
        return "GUARDED_MUTATE"
    if normalized in {"cancel", "restart", "cleanup", "reclaim"}:
        return "HIGH_RISK_MUTATE"
    if normalized in {"batch_apply", "batch_execute"}:
        return "BATCH_MUTATE"
    return "GUARDED_MUTATE"


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
