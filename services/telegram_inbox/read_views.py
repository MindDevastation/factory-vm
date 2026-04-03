from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_freshness_summary(*, generated_at: str | None, stale_after_seconds: int = 300, now: datetime | None = None) -> dict[str, Any]:
    ref = now or datetime.now(timezone.utc)
    if not generated_at:
        return {"freshness": "unknown", "is_stale": True, "age_seconds": None}
    ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    age = int((ref - ts).total_seconds())
    return {
        "freshness": "stale" if age > stale_after_seconds else "current",
        "is_stale": age > stale_after_seconds,
        "age_seconds": max(0, age),
    }


def build_compact_read_view(*, summary: str, reason: str, risk: str, actions: list[str] | None, web_link: str, generated_at: str | None) -> dict[str, Any]:
    return {
        "compact": True,
        "summary": str(summary),
        "reason": str(reason),
        "risk": str(risk),
        "actions": list(actions or []),
        "web_link": str(web_link),
        "freshness": build_freshness_summary(generated_at=generated_at),
    }


def build_status_fixture(*, kind: str = "factory_overview") -> dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return build_compact_read_view(
        summary=f"{kind} summary",
        reason="triage required",
        risk="medium",
        actions=["open_web"],
        web_link="/ops/overview",
        generated_at=now,
    )


def group_queue_items(*, rows: list[dict[str, Any]], key: str = "publish_state") -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        k = str(row.get(key) or "unknown")
        grouped.setdefault(k, []).append(row)
    return grouped


def triage_priority(row: dict[str, Any]) -> tuple[int, int]:
    state = str(row.get("publish_state") or "")
    risk_rank = {
        "manual_handoff_pending": 0,
        "policy_blocked": 1,
        "retry_pending": 2,
        "ready_to_publish": 3,
    }.get(state, 9)
    job_id = int(row.get("job_id") or 0)
    return (risk_rank, job_id)


def build_factory_overview(*, rows: list[dict[str, Any]], generated_at: str | None) -> dict[str, Any]:
    grouped = group_queue_items(rows=rows)
    attention = sorted(rows, key=triage_priority)[:10]
    return build_compact_read_view(
        summary=f"{len(rows)} publish items, {len(attention)} need attention",
        reason="remote triage overview",
        risk="mixed",
        actions=["open_web", "open_queue"],
        web_link="/publish/queue",
        generated_at=generated_at,
    ) | {
        "queue_groups": {k: len(v) for k, v in grouped.items()},
        "attention_needed": [int(r.get("job_id") or 0) for r in attention],
    }


def build_readiness_overview(*, blockers: list[dict[str, Any]], generated_at: str | None) -> dict[str, Any]:
    return build_compact_read_view(
        summary=f"{len(blockers)} readiness blockers",
        reason="publish/readiness blockers detected",
        risk="high" if blockers else "low",
        actions=["open_web", "open_blockers"],
        web_link="/publish/health",
        generated_at=generated_at,
    ) | {
        "blocked_items": [int(item.get("job_id") or 0) for item in blockers],
    }


def build_deep_link(*, entity_type: str, entity_id: int) -> str:
    if entity_type == "job":
        return f"/jobs/{int(entity_id)}"
    if entity_type == "release":
        return f"/releases/{int(entity_id)}"
    return "/"


def build_entity_drilldown(*, entity_type: str, entity_id: int, state: str, reason: str, next_action: str | None, generated_at: str | None) -> dict[str, Any]:
    return build_compact_read_view(
        summary=f"{entity_type} {entity_id} is {state}",
        reason=reason,
        risk="high" if "blocked" in state or "manual" in state else "medium",
        actions=([next_action] if next_action else ["open_web"]),
        web_link=build_deep_link(entity_type=entity_type, entity_id=entity_id),
        generated_at=generated_at,
    ) | {
        "entity_type": entity_type,
        "entity_id": int(entity_id),
        "state": state,
    }


def build_problem_list(*, rows: list[dict[str, Any]], generated_at: str | None) -> dict[str, Any]:
    items = sorted(rows, key=triage_priority)
    return build_compact_read_view(
        summary=f"{len(items)} items need attention",
        reason="problem list",
        risk="high" if items else "low",
        actions=["open_web"],
        web_link="/publish/queue?filter=problem",
        generated_at=generated_at,
    ) | {
        "items": [{"job_id": int(r.get("job_id") or 0), "state": str(r.get("publish_state") or "unknown")} for r in items],
    }
