from __future__ import annotations

from typing import Any


WORKSPACE_FAMILIES = (
    "CHANNEL_WORKSPACE",
    "RELEASE_WORKSPACE",
    "JOB_WORKSPACE",
    "BATCH_MONTH_WORKSPACE",
)


def workspace_family_catalog() -> dict[str, Any]:
    return {"workspace_families": list(WORKSPACE_FAMILIES)}


def workspace_summary_contract(*, family: str, entity_id: str, title: str, blockers: list[str], next_actions: list[str], related_contexts: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "workspace_family": family,
        "entity_id": str(entity_id),
        "title": str(title),
        "blockers": [str(v) for v in blockers],
        "next_actions": [str(v) for v in next_actions],
        "related_contexts": related_contexts,
        "is_task_container": True,
    }


def entity_drilldown_contract(*, entry_context: str, scope: str, related_context_links: list[dict[str, str]], return_path: str, open_full_context_path: str) -> dict[str, Any]:
    return {
        "entry_context": str(entry_context),
        "current_entity_scope": str(scope),
        "related_context_links": related_context_links,
        "return_path": str(return_path),
        "open_full_context_path": str(open_full_context_path),
        "preserves_parent_identity": True,
    }
