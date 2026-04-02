from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from services.telegram_operator import TelegramOperatorRegistry

from .helpers import (
    build_notification_dedupe_key,
    build_target_context_summary,
    classify_quiet_noisy,
    validate_classification,
)
from .observability import emit_inbox_event

_UPSTREAM_TO_CLASSIFICATION = {
    "publish/manual_handoff": ("ACTIONABLE_ALERT", "PUBLISH", "HIGH", "ACK_REQUIRED"),
    "readiness/blocker": ("ACTIONABLE_ALERT", "READINESS", "HIGH", "ACK_REQUIRED"),
    "recovery/ops": ("CRITICAL_ALERT", "RECOVERY", "CRITICAL", "ESCALATE_ONLY"),
    "worker/health": ("CRITICAL_ALERT", "HEALTH", "CRITICAL", "ESCALATE_ONLY"),
    "stale/follow_up": ("UNRESOLVED_FOLLOW_UP", "FOLLOW_UP", "MEDIUM", "ACTIONABLE"),
    "digest/summary": ("SUMMARY_DIGEST", "DIGEST", "INFO", "INFO_ONLY"),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TelegramInboxRouter:
    def __init__(self, conn: Any):
        self._conn = conn
        self._registry = TelegramOperatorRegistry(conn)

    def route_event(
        self,
        *,
        product_operator_id: str,
        telegram_user_id: int,
        chat_id: int,
        thread_id: int | None,
        upstream_event_family: str,
        upstream_event_ref: str | None,
        target_entity_type: str,
        target_entity_ref: str,
        title: str,
        body: str,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        identity = self._registry.get_identity(telegram_user_id=int(telegram_user_id))
        if identity is None or str(identity["product_operator_id"]) != str(product_operator_id):
            emit_inbox_event(self._conn, event_type="MESSAGE_DELIVERY_DENIED", message_id=None, telegram_user_id=int(telegram_user_id), product_operator_id=str(product_operator_id), chat_id=int(chat_id), thread_id=thread_id, message_family=None, category=None, severity=None, target_context=None, lifecycle_state=None, routing_result="DENIED", reason_code="E6A_TELEGRAM_IDENTITY_UNBOUND", payload={"upstream_event_family": upstream_event_family})
            return {"delivery_result": "DENIED", "reason_code": "E6A_TELEGRAM_IDENTITY_UNBOUND"}
        if str(identity["telegram_access_status"]) in {"INACTIVE", "REVOKED"}:
            _code = "E6A_OPERATOR_REVOKED" if str(identity["telegram_access_status"]) == "REVOKED" else "E6A_OPERATOR_INACTIVE"
            emit_inbox_event(self._conn, event_type="MESSAGE_DELIVERY_DENIED", message_id=None, telegram_user_id=int(telegram_user_id), product_operator_id=str(product_operator_id), chat_id=int(chat_id), thread_id=thread_id, message_family=None, category=None, severity=None, target_context=None, lifecycle_state=None, routing_result="DENIED", reason_code=_code, payload={"upstream_event_family": upstream_event_family})
            return {"delivery_result": "DENIED", "reason_code": _code}

        binding = self._registry.read_binding(
            product_operator_id=str(product_operator_id),
            telegram_user_id=int(telegram_user_id),
            chat_id=int(chat_id),
            thread_id=thread_id,
        )
        if binding is None or str(binding["binding_status"]) != "ACTIVE":
            _code = "E6A_CHAT_BINDING_MISSING" if binding is None else f"E6A_CHAT_BINDING_{str(binding['binding_status'])}"
            emit_inbox_event(self._conn, event_type="MESSAGE_DELIVERY_DENIED", message_id=None, telegram_user_id=int(telegram_user_id), product_operator_id=str(product_operator_id), chat_id=int(chat_id), thread_id=thread_id, message_family=None, category=None, severity=None, target_context=None, lifecycle_state=None, routing_result="DENIED", reason_code=_code, payload={"upstream_event_family": upstream_event_family})
            return {"delivery_result": "DENIED", "reason_code": _code}

        mapped = _UPSTREAM_TO_CLASSIFICATION.get(str(upstream_event_family), ("ACTIONABLE_ALERT", "SYSTEM", "LOW", "INFO_ONLY"))
        message_family, category, severity, actionability = mapped
        delivery_behavior = classify_quiet_noisy(message_family=message_family, severity=severity)
        classification = validate_classification(
            category=category,
            severity=severity,
            message_family=message_family,
            actionability_class=actionability,
            delivery_behavior=delivery_behavior,
        )

        target_context = build_target_context_summary(
            target_entity_type=target_entity_type,
            target_entity_ref=target_entity_ref,
            attributes=attributes,
        )
        dedupe_key = build_notification_dedupe_key(
            message_family=message_family,
            category=category,
            target_entity_type=target_entity_type,
            target_entity_ref=target_entity_ref,
            upstream_event_family=upstream_event_family,
            upstream_event_ref=upstream_event_ref,
        )

        existing = self._conn.execute("SELECT id FROM telegram_inbox_messages WHERE dedupe_key = ?", (dedupe_key,)).fetchone()
        if existing is not None:
            self._conn.execute(
                """
                INSERT INTO telegram_inbox_deliveries(
                    message_id, telegram_user_id, chat_id, thread_id, delivery_status, delivery_reason_code, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (int(existing["id"]), int(telegram_user_id), int(chat_id), thread_id, "SUPPRESSED", "DEDUPE_SUPPRESSED", _now_iso(), _now_iso()),
            )
            emit_inbox_event(self._conn, event_type="MESSAGE_DEDUPED_SUPPRESSED", message_id=int(existing["id"]), telegram_user_id=int(telegram_user_id), product_operator_id=str(product_operator_id), chat_id=int(chat_id), thread_id=thread_id, message_family=message_family, category=category, severity=severity, target_context=target_context, lifecycle_state=None, routing_result="SUPPRESSED", reason_code="DEDUPE_SUPPRESSED", payload={})
            return {"delivery_result": "SUPPRESSED", "reason_code": "DEDUPE_SUPPRESSED", "message_id": int(existing["id"])}

        now = _now_iso()
        cur = self._conn.execute(
            """
            INSERT INTO telegram_inbox_messages(
                message_family, category, severity, actionability_class, lifecycle_state,
                stale_behavior, delivery_behavior, telegram_user_id, product_operator_id,
                chat_id, thread_id, binding_id, target_entity_type, target_entity_ref,
                target_context_json, title, body, dedupe_key, followup_key,
                related_message_id, upstream_event_family, upstream_event_ref, created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                classification["message_family"],
                classification["category"],
                classification["severity"],
                classification["actionability_class"],
                "INFO_ONLY" if classification["actionability_class"] == "INFO_ONLY" else "ACTIVE",
                "SUPERSEDE",
                classification["delivery_behavior"],
                int(telegram_user_id),
                str(product_operator_id),
                int(chat_id),
                thread_id,
                int(binding["id"]),
                str(target_entity_type),
                str(target_entity_ref),
                json.dumps(target_context, ensure_ascii=False, sort_keys=True),
                str(title),
                str(body),
                dedupe_key,
                f"{upstream_event_family}:{target_entity_type}:{target_entity_ref}",
                None,
                str(upstream_event_family),
                upstream_event_ref,
                now,
            ),
        )
        message_id = int(cur.lastrowid)

        delivery_status = "DELIVERED" if classification["delivery_behavior"] in {"IMMEDIATE", "DIGEST", "FOLLOW_UP_ONLY"} else "SUPPRESSED"
        self._conn.execute(
            """
            INSERT INTO telegram_inbox_deliveries(
                message_id, telegram_user_id, chat_id, thread_id, delivery_status, delivery_reason_code, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                message_id,
                int(telegram_user_id),
                int(chat_id),
                thread_id,
                delivery_status,
                None if delivery_status == "DELIVERED" else "SUPPRESSED_BY_POLICY",
                now,
                now,
            ),
        )
        emit_inbox_event(self._conn, event_type="MESSAGE_CREATED", message_id=message_id, telegram_user_id=int(telegram_user_id), product_operator_id=str(product_operator_id), chat_id=int(chat_id), thread_id=thread_id, message_family=classification["message_family"], category=classification["category"], severity=classification["severity"], target_context=target_context, lifecycle_state=("INFO_ONLY" if classification["actionability_class"] == "INFO_ONLY" else "ACTIVE"), routing_result=None, reason_code=None, payload={"delivery_behavior": classification["delivery_behavior"]})
        emit_inbox_event(self._conn, event_type="MESSAGE_ROUTED", message_id=message_id, telegram_user_id=int(telegram_user_id), product_operator_id=str(product_operator_id), chat_id=int(chat_id), thread_id=thread_id, message_family=classification["message_family"], category=classification["category"], severity=classification["severity"], target_context=target_context, lifecycle_state=("INFO_ONLY" if classification["actionability_class"] == "INFO_ONLY" else "ACTIVE"), routing_result=delivery_status, reason_code=None if delivery_status == "DELIVERED" else "SUPPRESSED_BY_POLICY", payload={})
        return {
            "delivery_result": delivery_status,
            "message_id": message_id,
            "classification": classification,
            "target_context": target_context,
        }
