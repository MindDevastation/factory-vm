from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


MANDATORY_AUDIT_EVENTS = {
    "TELEGRAM_IDENTITY_ENROLLED",
    "TELEGRAM_IDENTITY_ACCESS_CHANGED",
    "TELEGRAM_CHAT_BINDING_CREATED",
    "TELEGRAM_CHAT_BINDING_STATUS_CHANGED",
    "TELEGRAM_GATEWAY_EVALUATED",
    "TELEGRAM_GATEWAY_DENIED",
    "TELEGRAM_ACTION_EXPIRED",
    "TELEGRAM_ACTION_STALE",
    "TELEGRAM_WHOAMI_REQUESTED",
    "TELEGRAM_IDENTITY_MISMATCH_DETECTED",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_audit_event(
    conn: Any,
    *,
    event_type: str,
    telegram_user_id: int,
    resolved_product_operator_id: str | None,
    chat_id: int | None,
    thread_id: int | None,
    binding_id: int | None,
    action_type: str | None,
    action_class: str | None,
    target_entity_type: str | None,
    target_entity_ref: str | None,
    gateway_result: str | None,
    gateway_error_code: str | None,
    correlation_id: str | None,
    idempotency_key: str | None,
    payload: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_operator_audit_events(
            event_type, telegram_user_id, resolved_product_operator_id, chat_id, thread_id,
            binding_id, action_type, action_class, target_entity_type, target_entity_ref,
            gateway_result, gateway_error_code, correlation_id, idempotency_key, payload_json, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(event_type),
            int(telegram_user_id),
            resolved_product_operator_id,
            chat_id,
            thread_id,
            binding_id,
            action_type,
            action_class,
            target_entity_type,
            target_entity_ref,
            gateway_result,
            gateway_error_code,
            correlation_id,
            idempotency_key,
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            _now_iso(),
        ),
    )
