from __future__ import annotations

from typing import Any


def status_badge_semantics(*, status: str) -> dict[str, str]:
    value = str(status or "UNKNOWN").strip().upper()
    tone = {
        "FAILED": "danger",
        "BLOCKED": "danger",
        "PUBLISHED": "success",
        "READY": "success",
        "RUNNING": "info",
        "RENDERING": "info",
        "DRAFT": "neutral",
    }.get(value, "neutral")
    return {"component": "status_badge", "status": value, "tone": tone}


def severity_indicator_semantics(*, severity: str) -> dict[str, str]:
    value = str(severity or "UNKNOWN").strip().upper()
    rank = {"CRITICAL": "P0", "HIGH": "P1", "MEDIUM": "P2", "LOW": "P3"}.get(value, "P3")
    return {"component": "severity_indicator", "severity": value, "priority_rank": rank}


def readiness_indicator_semantics(*, readiness: str) -> dict[str, str]:
    value = str(readiness or "UNKNOWN").strip().upper()
    tone = {
        "READY": "success",
        "NOT_READY": "warning",
        "BLOCKED": "danger",
        "STALE": "stale",
    }.get(value, "neutral")
    return {"component": "readiness_indicator", "readiness": value, "tone": tone}


def inline_message_semantics(*, level: str, text: str) -> dict[str, str]:
    normalized = str(level or "INFO").strip().upper()
    if normalized not in {"INFO", "WARNING", "ERROR", "SUCCESS"}:
        normalized = "INFO"
    return {"component": "inline_message", "level": normalized, "text": str(text or "").strip()}


def action_bar_semantics(*, actions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "component": "action_bar",
        "count": len(actions),
        "actions": [{"action": str(a.get("action") or ""), "kind": str(a.get("kind") or "PRIMARY")} for a in actions],
    }


def filter_control_semantics(*, filters: list[str]) -> dict[str, Any]:
    return {"component": "filter_controls", "filters": [str(v) for v in filters]}


def table_list_semantics(*, variant: str) -> dict[str, str]:
    normalized = str(variant or "TABLE").strip().upper()
    if normalized not in {"TABLE", "LIST"}:
        normalized = "TABLE"
    return {"component": "table_list_pattern", "variant": normalized}
