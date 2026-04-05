from __future__ import annotations

from typing import Any

from services.common import db as dbm


WORKSPACE_FAMILIES = (
    "CHANNEL_WORKSPACE",
    "RELEASE_WORKSPACE",
    "JOB_WORKSPACE",
    "BATCH_MONTH_WORKSPACE",
)


def workspace_family_catalog() -> dict[str, Any]:
    return {"workspace_families": list(WORKSPACE_FAMILIES)}


def _normalized_family(family: str) -> str:
    normalized = str(family or "").strip().upper()
    alias_map = {
        "CHANNEL": "CHANNEL_WORKSPACE",
        "RELEASE": "RELEASE_WORKSPACE",
        "JOB": "JOB_WORKSPACE",
        "BATCH": "BATCH_MONTH_WORKSPACE",
        "BATCH_MONTH": "BATCH_MONTH_WORKSPACE",
    }
    return alias_map.get(normalized, normalized)


def _workspace_title(family: str, entity: dict[str, Any]) -> str:
    if family == "CHANNEL_WORKSPACE":
        return str(entity.get("display_name") or entity.get("slug") or "Channel")
    if family == "RELEASE_WORKSPACE":
        return str(entity.get("title") or f"Release {entity.get('id')}")
    if family == "JOB_WORKSPACE":
        return f"Job {entity.get('id')}"
    if family == "BATCH_MONTH_WORKSPACE":
        return f"Batch {entity.get('batch_month')}"
    return str(entity.get("id") or "Workspace")


def _workspace_blockers(family: str, entity: dict[str, Any]) -> list[str]:
    if family == "JOB_WORKSPACE" and str(entity.get("state") or "").upper() in {"FAILED", "BLOCKED"}:
        return [f"job_state={str(entity.get('state')).upper()}"]
    if family == "RELEASE_WORKSPACE" and not entity.get("planned_at"):
        return ["missing_planned_at"]
    return []


def _workspace_next_actions(family: str, entity: dict[str, Any]) -> list[str]:
    if family == "JOB_WORKSPACE":
        state = str(entity.get("state") or "").upper()
        if state in {"FAILED", "BLOCKED"}:
            return ["open_recovery", "review_stderr"]
        return ["open_publish_queue"]
    if family == "RELEASE_WORKSPACE":
        return ["open_release_detail", "open_related_jobs"]
    if family == "CHANNEL_WORKSPACE":
        return ["open_channel_dashboard", "open_recent_releases"]
    return ["open_batch_scope", "open_blocked_jobs"]


def workspace_summary_contract(*, family: str, entity_id: str, title: str, blockers: list[str], next_actions: list[str], related_contexts: list[dict[str, str]], core_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "workspace_family": family,
        "entity_id": str(entity_id),
        "title": str(title),
        "core_summary": dict(core_summary or {}),
        "blockers": [str(v) for v in blockers],
        "next_actions": [str(v) for v in next_actions],
        "related_contexts": related_contexts,
        "is_task_container": True,
    }


def build_workspace_summary(*, conn: Any, family: str, entity_id: str) -> dict[str, Any]:
    normalized_family = _normalized_family(family)
    entity_text = str(entity_id)
    if normalized_family == "CHANNEL_WORKSPACE":
        entity = dbm.get_channel_by_id(conn, int(entity_text))
        if not entity:
            raise ValueError("channel_not_found")
        related = [{"kind": "recent_jobs", "href": f"/ui/jobs?channel={entity.get('slug')}"}]
    elif normalized_family == "RELEASE_WORKSPACE":
        entity = conn.execute("SELECT id, channel_id, title, planned_at FROM releases WHERE id=?", (int(entity_text),)).fetchone()
        if not entity:
            raise ValueError("release_not_found")
        related = [
            {"kind": "channel_workspace", "href": f"/v1/workspaces/channel/{int(entity['channel_id'])}"},
            {"kind": "release_jobs", "href": f"/ui/jobs?release_id={int(entity['id'])}"},
        ]
    elif normalized_family == "JOB_WORKSPACE":
        entity = dbm.get_job(conn, int(entity_text))
        if not entity:
            raise ValueError("job_not_found")
        related = [
            {"kind": "release_workspace", "href": f"/v1/workspaces/release/{int(entity['release_id'])}"},
            {"kind": "channel_workspace", "href": f"/v1/workspaces/channel/{int(entity['channel_id'])}"},
        ]
    elif normalized_family == "BATCH_MONTH_WORKSPACE":
        month = entity_text
        count_row = conn.execute(
            "SELECT COUNT(*) AS c FROM releases WHERE planned_at IS NOT NULL AND strftime('%Y-%m', planned_at)=?",
            (month,),
        ).fetchone()
        entity = {"batch_month": month, "release_count": int((count_row or {"c": 0})["c"])}
        related = [{"kind": "batch_releases", "href": f"/ui/planner?month={month}"}]
    else:
        raise ValueError("unsupported_workspace_family")

    return workspace_summary_contract(
        family=normalized_family,
        entity_id=entity_text,
        title=_workspace_title(normalized_family, entity),
        blockers=_workspace_blockers(normalized_family, entity),
        next_actions=_workspace_next_actions(normalized_family, entity),
        related_contexts=related,
        core_summary=dict(entity),
    )


def entity_drilldown_contract(*, entry_context: str, scope: str, related_context_links: list[dict[str, str]], return_path: str, open_full_context_path: str) -> dict[str, Any]:
    return {
        "entry_context": str(entry_context),
        "current_entity_scope": str(scope),
        "related_context_links": related_context_links,
        "return_path": str(return_path),
        "open_full_context_path": str(open_full_context_path),
        "preserves_parent_identity": True,
    }


def task_continuity_contract(*, parent_context_ref: str, filters: dict[str, str], scope: str, result_return_path: str) -> dict[str, Any]:
    return {
        "parent_context_ref": str(parent_context_ref),
        "filters": {str(k): str(v) for k, v in filters.items()},
        "current_scope": str(scope),
        "result_return_path": str(result_return_path),
        "restorable": True,
    }


def result_return_contract(*, from_action: str, return_path: str, open_full_context_path: str) -> dict[str, Any]:
    return {
        "from_action": str(from_action),
        "return_path": str(return_path),
        "open_full_context_path": str(open_full_context_path),
        "continuation_supported": True,
    }
