from __future__ import annotations

from typing import Any


def build_control_center_contract_skeleton(*, factory_summary: dict[str, Any], attention_summary: dict[str, Any], channel_summary: dict[str, Any], batch_month_summary: dict[str, Any], task_routing: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "surface": "CONTROL_CENTER_OVERVIEW",
        "contract_version": "MF3_S1_BASELINE",
        "factory_summary": dict(factory_summary),
        "attention_summary": dict(attention_summary),
        "channel_summary": dict(channel_summary),
        "batch_month_summary": dict(batch_month_summary),
        "task_routing": list(task_routing),
    }


def default_task_routing_contract() -> list[dict[str, str]]:
    return [
        {"route": "/ui/planner", "intent": "planning"},
        {"route": "/ui/publish/failed", "intent": "problem_triage"},
        {"route": "/ui/publish/queue", "intent": "publish_workspace"},
        {"route": "/ui/ops/recovery", "intent": "ops_recovery"},
    ]
