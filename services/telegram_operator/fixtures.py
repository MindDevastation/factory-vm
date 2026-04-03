from __future__ import annotations

from typing import Any


def build_identity_fixture(*, product_operator_id: str = "operator-1", telegram_user_id: int = 1001, telegram_access_status: str = "ACTIVE", max_permission_class: str = "STANDARD_OPERATOR_MUTATE") -> dict[str, Any]:
    return {
        "product_operator_id": product_operator_id,
        "telegram_user_id": telegram_user_id,
        "telegram_access_status": telegram_access_status,
        "max_permission_class": max_permission_class,
    }


def build_binding_fixture(*, product_operator_id: str = "operator-1", telegram_user_id: int = 1001, chat_id: int = -2001, thread_id: int | None = None, chat_binding_kind: str = "PRIVATE_CHAT", binding_status: str = "ACTIVE") -> dict[str, Any]:
    return {
        "product_operator_id": product_operator_id,
        "telegram_user_id": telegram_user_id,
        "chat_id": chat_id,
        "thread_id": thread_id,
        "chat_binding_kind": chat_binding_kind,
        "binding_status": binding_status,
    }


def build_denied_case_fixture(*, code: str) -> dict[str, Any]:
    return {"gateway_result": "DENIED", "error_code": code}


def build_envelope_fixture(*, correlation_id: str = "corr-fixture", action_class: str = "READ_ONLY", expires_at: str | None = None) -> dict[str, Any]:
    return {
        "action_transport_type": "COMMAND",
        "action_transport_id": "fixture-cmd",
        "telegram_user_id": 1001,
        "product_operator_id": None,
        "chat_id": -2001,
        "thread_id": None,
        "binding_id": None,
        "action_type": "FIXTURE_ACTION",
        "action_class": action_class,
        "target_entity_type": "release",
        "target_entity_ref": "rel-fixture",
        "freshness_context": {},
        "correlation_id": correlation_id,
        "idempotency_key": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "expires_at": expires_at,
    }
