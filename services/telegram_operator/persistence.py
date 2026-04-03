from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def persist_publish_action_context(
    conn: Any,
    *,
    request_id: str,
    action_type: str,
    action_transport_type: str,
    actor_ref: str,
    target_entity_type: str,
    target_entity_ref: str,
    context: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_publish_action_contexts(
            request_id, action_type, action_transport_type, actor_ref,
            target_entity_type, target_entity_ref, context_json, created_at
        ) VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(request_id) DO NOTHING
        """,
        (
            str(request_id),
            str(action_type),
            str(action_transport_type),
            str(actor_ref),
            str(target_entity_type),
            str(target_entity_ref),
            json.dumps(context, ensure_ascii=False, sort_keys=True),
            _utc_now(),
        ),
    )


def persist_publish_action_result(
    conn: Any,
    *,
    request_id: str,
    action_type: str,
    result_status: str,
    error_code: str | None,
    result_payload: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_publish_action_results(
            request_id, action_type, result_status, error_code, result_json, created_at
        ) VALUES(?,?,?,?,?,?)
        """,
        (
            str(request_id),
            str(action_type),
            str(result_status),
            str(error_code) if error_code else None,
            json.dumps(result_payload, ensure_ascii=False, sort_keys=True),
            _utc_now(),
        ),
    )


def persist_read_view_snapshot(
    conn: Any,
    *,
    product_operator_id: str,
    view_name: str,
    view_params: dict[str, Any],
    snapshot: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_read_view_snapshots(
            product_operator_id, view_name, view_params_json, snapshot_json, created_at
        ) VALUES(?,?,?,?,?)
        """,
        (
            str(product_operator_id),
            str(view_name),
            json.dumps(view_params, ensure_ascii=False, sort_keys=True),
            json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
            _utc_now(),
        ),
    )


def persist_read_view_access_event(
    conn: Any,
    *,
    product_operator_id: str,
    telegram_user_id: int,
    view_name: str,
    access_result: str,
    reason_code: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_read_view_access_events(
            product_operator_id, telegram_user_id, view_name, access_result, reason_code, created_at
        ) VALUES(?,?,?,?,?,?)
        """,
        (
            str(product_operator_id),
            int(telegram_user_id),
            str(view_name),
            str(access_result),
            str(reason_code) if reason_code else None,
            _utc_now(),
        ),
    )


def persist_ops_action_context(
    conn: Any,
    *,
    action_ref: str,
    action_type: str,
    product_operator_id: str,
    telegram_user_id: int,
    target_entity_type: str,
    target_entity_ref: str,
    context: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_ops_action_contexts(
            action_ref, action_type, product_operator_id, telegram_user_id,
            target_entity_type, target_entity_ref, context_json, created_at
        ) VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(action_ref) DO NOTHING
        """,
        (
            str(action_ref),
            str(action_type),
            str(product_operator_id),
            int(telegram_user_id),
            str(target_entity_type),
            str(target_entity_ref),
            json.dumps(context, ensure_ascii=False, sort_keys=True),
            _utc_now(),
        ),
    )


def persist_ops_action_confirmation(
    conn: Any,
    *,
    action_ref: str,
    confirmation_token: str,
    confirmation_status: str,
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_ops_action_confirmations(
            action_ref, confirmation_token, confirmation_status, confirmed_at, created_at
        ) VALUES(?,?,?,?,?)
        """,
        (
            str(action_ref),
            str(confirmation_token),
            str(confirmation_status),
            _utc_now() if confirmation_status == "CONFIRMED" else None,
            _utc_now(),
        ),
    )


def persist_ops_action_result(
    conn: Any,
    *,
    action_ref: str,
    result_status: str,
    error_code: str | None,
    result_payload: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_ops_action_results(
            action_ref, result_status, error_code, result_json, created_at
        ) VALUES(?,?,?,?,?)
        """,
        (
            str(action_ref),
            str(result_status),
            str(error_code) if error_code else None,
            json.dumps(result_payload, ensure_ascii=False, sort_keys=True),
            _utc_now(),
        ),
    )


def persist_action_audit_record(
    conn: Any,
    *,
    record_type: str,
    action_ref: str | None,
    request_id: str | None,
    correlation_id: str | None,
    actor_ref: str | None,
    payload: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_action_audit_records(
            record_type, action_ref, request_id, correlation_id, actor_ref, payload_json, created_at
        ) VALUES(?,?,?,?,?,?,?)
        """,
        (
            str(record_type),
            str(action_ref) if action_ref else None,
            str(request_id) if request_id else None,
            str(correlation_id) if correlation_id else None,
            str(actor_ref) if actor_ref else None,
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            _utc_now(),
        ),
    )


def persist_action_idempotency_record(
    conn: Any,
    *,
    idempotency_key: str,
    action_ref: str | None,
    request_id: str | None,
    response_fingerprint: str | None,
) -> None:
    now = _utc_now()
    conn.execute(
        """
        INSERT INTO telegram_action_idempotency_records(
            idempotency_key, action_ref, request_id, first_seen_at, last_seen_at, response_fingerprint
        ) VALUES(?,?,?,?,?,?)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            last_seen_at=excluded.last_seen_at,
            response_fingerprint=excluded.response_fingerprint
        """,
        (
            str(idempotency_key),
            str(action_ref) if action_ref else None,
            str(request_id) if request_id else None,
            now,
            now,
            str(response_fingerprint) if response_fingerprint else None,
        ),
    )


def persist_action_safety_event(
    conn: Any,
    *,
    safety_event_type: str,
    action_ref: str | None,
    request_id: str | None,
    reason_code: str | None,
    details: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO telegram_action_safety_events(
            safety_event_type, action_ref, request_id, reason_code, details_json, created_at
        ) VALUES(?,?,?,?,?,?)
        """,
        (
            str(safety_event_type),
            str(action_ref) if action_ref else None,
            str(request_id) if request_id else None,
            str(reason_code) if reason_code else None,
            json.dumps(details, ensure_ascii=False, sort_keys=True),
            _utc_now(),
        ),
    )
