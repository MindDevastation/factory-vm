from __future__ import annotations

from typing import Any

from services.factory_api.problem_readiness_contracts import problem_readiness_item_contract


def _severity_for_state(state: str) -> str:
    normalized = str(state or "").strip().upper()
    return {
        "FAILED": "CRITICAL",
        "BLOCKED": "BLOCKING",
        "DEGRADED": "DEGRADED_WORKABLE",
        "STALE": "DEGRADED_WORKABLE",
    }.get(normalized, "INFORMATIONAL")


def _group_for_state(state: str) -> str:
    normalized = str(state or "").strip().upper()
    return {
        "FAILED": "blockers",
        "BLOCKED": "blockers",
        "DEGRADED": "degraded",
        "STALE": "stale",
    }.get(normalized, "warnings")


def build_grouped_problem_surface(*, jobs: list[dict[str, Any]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for job in jobs:
        state = str(job.get("state") or "").strip().upper()
        if state not in {"FAILED", "BLOCKED", "DEGRADED", "STALE"}:
            continue
        severity = _severity_for_state(state)
        item = problem_readiness_item_contract(
            state=state,
            severity=severity,
            primary_reason=str(job.get("error_reason") or f"state={state}"),
            supporting_signals=[f"job_id={job.get('id')}", f"stage={job.get('stage') or ''}"],
            next_direction="open recovery workspace" if state in {"FAILED", "BLOCKED"} else "open related workspace",
        )
        item["job_id"] = int(job.get("id") or 0)
        item["routing_targets"] = [
            {"kind": "open_entity_workspace", "href": f"/v1/workspaces/job/{item['job_id']}"},
            {"kind": "open_related_domain_page", "href": "/ui/publish/queue"},
            {"kind": "open_action_flow", "href": "/ui/ops/recovery" if state in {"FAILED", "BLOCKED"} else "/ui/planner"},
            {"kind": "open_detail_context", "href": f"/ui/publish/jobs/{item['job_id']}"},
        ]
        item["group"] = _group_for_state(state)
        items.append(item)

    priority = {"CRITICAL": 0, "BLOCKING": 1, "DEGRADED_WORKABLE": 2, "INFORMATIONAL": 3}
    items.sort(key=lambda item: (priority.get(str(item.get("severity_priority") or "INFORMATIONAL"), 3), int(item.get("job_id") or 0)))

    grouped = {"blockers": [], "warnings": [], "degraded": [], "stale": []}
    for item in items:
        grouped[str(item["group"])].append(item)

    return {
        "groups": grouped,
        "summary": {
            "blockers": len(grouped["blockers"]),
            "warnings": len(grouped["warnings"]),
            "degraded": len(grouped["degraded"]),
            "stale": len(grouped["stale"]),
        },
    }
