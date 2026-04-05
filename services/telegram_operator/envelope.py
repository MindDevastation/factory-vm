from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .literals import ensure_action_transport_type, ensure_permission_access_class


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_action_envelope(
    *,
    action_transport_type: str,
    action_transport_id: str,
    telegram_user_id: int,
    chat_id: int,
    thread_id: int | None,
    action_type: str,
    action_class: str,
    target_entity_type: str | None,
    target_entity_ref: str | None,
    freshness_context: dict[str, Any] | None,
    correlation_id: str,
    idempotency_key: str | None = None,
    product_operator_id: str | None = None,
    binding_id: int | None = None,
    created_at: str | None = None,
    expires_at: str | None = None,
) -> dict[str, Any]:
    if not str(correlation_id or "").strip():
        raise ValueError("correlation_id is required")
    if not str(action_type or "").strip():
        raise ValueError("action_type is required")
    if target_entity_type is None or target_entity_ref is None:
        raise ValueError("target_entity_type and target_entity_ref are required")

    created = created_at or _iso_now()
    _parse_iso(created)
    _parse_iso(expires_at)

    return {
        "action_transport_type": ensure_action_transport_type(action_transport_type),
        "action_transport_id": str(action_transport_id),
        "telegram_user_id": int(telegram_user_id),
        "product_operator_id": product_operator_id,
        "chat_id": int(chat_id),
        "thread_id": int(thread_id) if thread_id is not None else None,
        "binding_id": int(binding_id) if binding_id is not None else None,
        "action_type": str(action_type),
        "action_class": ensure_permission_access_class(action_class),
        "target_entity_type": str(target_entity_type),
        "target_entity_ref": str(target_entity_ref),
        "freshness_context": freshness_context or {},
        "correlation_id": str(correlation_id),
        "idempotency_key": str(idempotency_key) if idempotency_key is not None else None,
        "created_at": created,
        "expires_at": expires_at,
    }


def is_envelope_expired(envelope: dict[str, Any], *, now: datetime | None = None) -> bool:
    expiry = _parse_iso(envelope.get("expires_at"))
    if expiry is None:
        return False
    ref = now or datetime.now(timezone.utc)
    return expiry <= ref
