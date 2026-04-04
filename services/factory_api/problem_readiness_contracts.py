from __future__ import annotations

from typing import Any


_PROBLEM_FAMILY_MAP = {
    "FAILED": "EXECUTION_FAILURE",
    "BLOCKED": "OPERATOR_BLOCKER",
    "STALE": "STALE_CONTEXT",
    "DEGRADED": "DEGRADED_PIPELINE",
}

_ATTENTION_CLASS_MAP = {
    "CRITICAL": "IMMEDIATE",
    "HIGH": "PRIORITY",
    "MEDIUM": "SCHEDULED",
    "LOW": "BACKGROUND",
}


def problem_family_for_state(*, state: str) -> str:
    return _PROBLEM_FAMILY_MAP.get(str(state or "").strip().upper(), "UNKNOWN_PROBLEM_FAMILY")


def attention_class_for_severity(*, severity: str) -> str:
    return _ATTENTION_CLASS_MAP.get(str(severity or "").strip().upper(), "SCHEDULED")


def explanation_payload_contract(*, primary_reason: str, supporting_signals: list[str], current_status: str, next_direction: str) -> dict[str, Any]:
    return {
        "primary_reason": str(primary_reason or "").strip(),
        "supporting_signals": [str(v) for v in supporting_signals],
        "current_status": str(current_status or "").strip(),
        "next_direction": str(next_direction or "").strip(),
    }


def problem_readiness_item_contract(*, state: str, severity: str, primary_reason: str, supporting_signals: list[str], next_direction: str) -> dict[str, Any]:
    return {
        "problem_family": problem_family_for_state(state=state),
        "attention_class": attention_class_for_severity(severity=severity),
        "readiness_state": str(state or "").strip().upper(),
        "explanation": explanation_payload_contract(
            primary_reason=primary_reason,
            supporting_signals=supporting_signals,
            current_status=str(state or "").strip().upper(),
            next_direction=next_direction,
        ),
        "routing_targets": [],
    }


def problem_readiness_contract_catalog() -> dict[str, Any]:
    return {
        "problem_family_map": dict(_PROBLEM_FAMILY_MAP),
        "attention_class_map": dict(_ATTENTION_CLASS_MAP),
    }
