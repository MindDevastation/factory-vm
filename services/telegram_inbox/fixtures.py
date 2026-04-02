from __future__ import annotations

from typing import Any


def build_inbox_event_fixture(
    *,
    message_family: str = "ACTIONABLE_ALERT",
    category: str = "PUBLISH",
    severity: str = "HIGH",
    target_entity_type: str = "release",
    target_entity_ref: str = "rel-1",
    upstream_event_family: str = "publish/manual_handoff",
    upstream_event_ref: str = "evt-1",
) -> dict[str, Any]:
    return {
        "message_family": message_family,
        "category": category,
        "severity": severity,
        "target_entity_type": target_entity_type,
        "target_entity_ref": target_entity_ref,
        "upstream_event_family": upstream_event_family,
        "upstream_event_ref": upstream_event_ref,
        "title": "fixture title",
        "body": "fixture body",
        "attributes": {"channel": "darkwood-reverie"},
    }


def build_routing_context_fixture(*, telegram_user_id: int = 1001, chat_id: int = -2001, thread_id: int | None = None) -> dict[str, Any]:
    return {"telegram_user_id": telegram_user_id, "chat_id": chat_id, "thread_id": thread_id}
