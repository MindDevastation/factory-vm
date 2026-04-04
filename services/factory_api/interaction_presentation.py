from __future__ import annotations


def button_hierarchy_contract() -> dict[str, list[str]]:
    return {
        "priority_order": ["PRIMARY", "SECONDARY", "TERTIARY", "DESTRUCTIVE"],
        "usage": [
            "PRIMARY for main page intent",
            "SECONDARY for alternative safe action",
            "TERTIARY for low-emphasis utility",
            "DESTRUCTIVE requires explicit affordance",
        ],
    }


def interaction_mode_policy(*, action_kind: str, is_destructive: bool) -> str:
    kind = str(action_kind or "").strip().upper()
    if is_destructive:
        return "MODAL_CONFIRM"
    if kind in {"BATCH", "CROSS_SCOPE"}:
        return "MODAL_CONFIRM"
    return "INLINE_ALLOWED"


def destructive_affordance_policy(*, action_name: str) -> dict[str, str | bool]:
    return {
        "action": str(action_name or "").strip(),
        "requires_confirmation": True,
        "requires_explicit_label": True,
        "default_variant": "danger",
    }


def result_presentation_contract(*, success: bool, partial: bool, blocked: bool) -> dict[str, str]:
    if blocked:
        return {"state": "BLOCKED", "presentation": "inline_error_with_next_step"}
    if partial:
        return {"state": "PARTIAL", "presentation": "inline_warning_with_details"}
    if success:
        return {"state": "SUCCESS", "presentation": "inline_success_with_summary"}
    return {"state": "ERROR", "presentation": "inline_error_with_retry_hint"}


def interaction_presentation_contract_catalog() -> dict[str, object]:
    return {
        "button_hierarchy": button_hierarchy_contract(),
        "inline_policy_sample": interaction_mode_policy(action_kind="SINGLE", is_destructive=False),
        "modal_policy_sample": interaction_mode_policy(action_kind="BATCH", is_destructive=False),
        "destructive_policy_sample": destructive_affordance_policy(action_name="delete"),
        "result_presentations": {
            "success": result_presentation_contract(success=True, partial=False, blocked=False),
            "partial": result_presentation_contract(success=False, partial=True, blocked=False),
            "blocked": result_presentation_contract(success=False, partial=False, blocked=True),
            "error": result_presentation_contract(success=False, partial=False, blocked=False),
        },
    }
