from __future__ import annotations

from typing import Any


_PROBLEM_FAMILY_MAP = {
    "FAILED": "BLOCKER",
    "BLOCKED": "BLOCKER",
    "DEGRADED": "DEGRADED",
    "STALE": "STALE_CONTEXT",
    "WARNING": "WARNING",
}

_ATTENTION_CLASS_MAP = {
    "CRITICAL": "CRITICAL",
    "BLOCKING": "BLOCKING",
    "DEGRADED_WORKABLE": "DEGRADED_WORKABLE",
    "INFORMATIONAL": "INFORMATIONAL",
}

_READINESS_BY_STATE = {
    "FAILED": "BLOCKED",
    "BLOCKED": "BLOCKED",
    "DEGRADED": "DEGRADED",
    "STALE": "DEGRADED",
}


def problem_family_for_state(*, state: str) -> str:
    return _PROBLEM_FAMILY_MAP.get(str(state or "").strip().upper(), "ATTENTION_ITEM")


def attention_class_for_severity(*, severity: str) -> str:
    normalized = str(severity or "").strip().upper()
    return _ATTENTION_CLASS_MAP.get(normalized, "INFORMATIONAL")


def readiness_indicator_for_state(*, state: str) -> str:
    return _READINESS_BY_STATE.get(str(state or "").strip().upper(), "READY")


def explanation_payload_contract(*, primary_reason: str, supporting_signals: list[str], current_status: str, next_direction: str) -> dict[str, Any]:
    return {
        "primary_reason": str(primary_reason or "").strip(),
        "supporting_signals": [str(v) for v in supporting_signals],
        "current_status": str(current_status or "").strip(),
        "next_direction": str(next_direction or "").strip(),
    }


def problem_readiness_item_contract(*, state: str, severity: str, primary_reason: str, supporting_signals: list[str], next_direction: str) -> dict[str, Any]:
    normalized_state = str(state or "").strip().upper()
    severity_priority = attention_class_for_severity(severity=severity)
    return {
        "problem_family": problem_family_for_state(state=normalized_state),
        "severity_priority": severity_priority,
        "attention_class": severity_priority,
        "readiness_state": normalized_state,
        "readiness_indicator": readiness_indicator_for_state(state=normalized_state),
        "explanation": explanation_payload_contract(
            primary_reason=primary_reason,
            supporting_signals=supporting_signals,
            current_status=normalized_state,
            next_direction=next_direction,
        ),
        "routing_targets": [],
    }


def problem_readiness_contract_catalog() -> dict[str, Any]:
    return {
        "problem_family_map": dict(_PROBLEM_FAMILY_MAP),
        "attention_class_map": dict(_ATTENTION_CLASS_MAP),
        "readiness_indicator_map": dict(_READINESS_BY_STATE),
    }
