from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_inbox_event(
    conn: Any,
    *,
    event_type: str,
    message_id: int | None,
    telegram_user_id: int | None,
    product_operator_id: str | None,
    chat_id: int | None,
    thread_id: int | None,
    message_family: str | None,
    category: str | None,
    severity: str | None,
    target_context: dict[str, Any] | None,
    lifecycle_state: str | None,
    routing_result: str | None,
    reason_code: str | None,
    payload: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_inbox_events(
            event_type, message_id, telegram_user_id, product_operator_id, chat_id, thread_id,
            message_family, category, severity, target_context_json, lifecycle_state,
            routing_result, reason_code, created_at, payload_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(event_type),
            message_id,
            telegram_user_id,
            product_operator_id,
            chat_id,
            thread_id,
            message_family,
            category,
            severity,
            json.dumps(target_context or {}, ensure_ascii=False, sort_keys=True),
            lifecycle_state,
            routing_result,
            reason_code,
            _now_iso(),
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
        ),
    )
