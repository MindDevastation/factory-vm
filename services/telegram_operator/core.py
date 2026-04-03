from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .errors import (
    E6A_CHAT_BINDING_DISABLED,
    E6A_CHAT_BINDING_MISSING,
    E6A_CHAT_BINDING_REVOKED,
    E6A_OPERATOR_IDENTITY_MISMATCH,
    E6A_OPERATOR_INACTIVE,
    E6A_OPERATOR_REVOKED,
    E6A_TELEGRAM_IDENTITY_UNBOUND,
    TelegramOperatorError,
)
from .literals import (
    PERMISSION_ACCESS_CLASSES,
    ensure_binding_status,
    ensure_chat_binding_kind,
    ensure_permission_access_class,
    ensure_telegram_access_status,
)
from .normalizer import normalize_binding_context
from .audit import emit_audit_event


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TelegramOperatorRegistry:
    def __init__(self, conn: Any):
        self._conn = conn

    def start_enrollment(self, *, product_operator_id: str, telegram_user_id: int, max_permission_class: str = "READ_ONLY") -> dict[str, Any]:
        now = _now_iso()
        max_class = ensure_permission_access_class(max_permission_class)
        row = self._conn.execute(
            "SELECT id, telegram_access_status FROM telegram_operator_identities WHERE telegram_user_id = ?",
            (int(telegram_user_id),),
        ).fetchone()
        if row:
            self._conn.execute(
                """
                UPDATE telegram_operator_identities
                SET product_operator_id = ?, telegram_access_status = 'INACTIVE', max_permission_class = ?,
                    disabled_at = NULL, revoked_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (str(product_operator_id), max_class, now, int(row["id"])),
            )
        else:
            self._conn.execute(
                """
                INSERT INTO telegram_operator_identities(
                    product_operator_id, telegram_user_id, telegram_access_status, max_permission_class,
                    enrolled_at, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (str(product_operator_id), int(telegram_user_id), "INACTIVE", max_class, now, now, now),
            )
        identity = self.get_identity(telegram_user_id=int(telegram_user_id))
        emit_audit_event(
            self._conn,
            event_type="TELEGRAM_IDENTITY_ENROLLED",
            telegram_user_id=int(telegram_user_id),
            resolved_product_operator_id=str(product_operator_id),
            chat_id=None,
            thread_id=None,
            binding_id=None,
            action_type="ENROLLMENT_START",
            action_class=max_class,
            target_entity_type="telegram_identity",
            target_entity_ref=str(telegram_user_id),
            gateway_result=None,
            gateway_error_code=None,
            correlation_id=None,
            idempotency_key=None,
            payload={"status": "INACTIVE"},
        )
        return {"enrollment_state": "STARTED", "identity": identity}

    def complete_enrollment(self, *, telegram_user_id: int) -> dict[str, Any]:
        identity = self._require_identity(telegram_user_id)
        effective = self._has_active_binding(telegram_user_id=int(telegram_user_id), product_operator_id=str(identity["product_operator_id"]))
        if effective and str(identity["telegram_access_status"]) != "ACTIVE":
            self.set_identity_access(telegram_user_id=int(telegram_user_id), telegram_access_status="ACTIVE")
            identity = self._require_identity(telegram_user_id)
        return {
            "enrollment_state": "ACTIVE" if effective else "PENDING_BINDING",
            "effective": effective,
            "identity": identity,
        }

    def view_enrollment_state(self, *, telegram_user_id: int) -> dict[str, Any]:
        identity = self.get_identity(telegram_user_id=telegram_user_id)
        if identity is None:
            return {"enrollment_state": "UNBOUND", "effective": False, "identity": None}
        effective = self._has_active_binding(telegram_user_id=int(telegram_user_id), product_operator_id=str(identity["product_operator_id"]))
        return {"enrollment_state": "ACTIVE" if effective else "PENDING_BINDING", "effective": effective, "identity": identity}

    def set_identity_access(self, *, telegram_user_id: int, telegram_access_status: str, last_error_code: str | None = None) -> dict[str, Any]:
        status = ensure_telegram_access_status(telegram_access_status)
        identity = self._require_identity(telegram_user_id)
        now = _now_iso()
        disabled_at = now if status == "INACTIVE" else None
        revoked_at = now if status == "REVOKED" else None
        self._conn.execute(
            """
            UPDATE telegram_operator_identities
            SET telegram_access_status = ?, disabled_at = ?, revoked_at = ?, last_error_code = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, disabled_at, revoked_at, last_error_code, now, int(identity["id"])),
        )
        emit_audit_event(
            self._conn,
            event_type="TELEGRAM_IDENTITY_ACCESS_CHANGED",
            telegram_user_id=int(telegram_user_id),
            resolved_product_operator_id=str(identity["product_operator_id"]),
            chat_id=None,
            thread_id=None,
            binding_id=None,
            action_type="ACCESS_CHANGE",
            action_class=str(identity["max_permission_class"]),
            target_entity_type="telegram_identity",
            target_entity_ref=str(telegram_user_id),
            gateway_result=None,
            gateway_error_code=last_error_code,
            correlation_id=None,
            idempotency_key=None,
            payload={"telegram_access_status": status},
        )
        return self._require_identity(telegram_user_id)

    def create_binding(self, *, product_operator_id: str, telegram_user_id: int, chat_id: int, thread_id: int | None, chat_binding_kind: str, binding_status: str = "ACTIVE") -> dict[str, Any]:
        identity = self._require_identity(telegram_user_id)
        if str(identity["product_operator_id"]) != str(product_operator_id):
            emit_audit_event(
                self._conn,
                event_type="TELEGRAM_IDENTITY_MISMATCH_DETECTED",
                telegram_user_id=int(telegram_user_id),
                resolved_product_operator_id=str(identity["product_operator_id"]),
                chat_id=int(chat_id),
                thread_id=thread_id,
                binding_id=None,
                action_type="BINDING_CREATE",
                action_class=str(identity["max_permission_class"]),
                target_entity_type="telegram_binding",
                target_entity_ref=f"{chat_id}:{thread_id}",
                gateway_result=None,
                gateway_error_code=E6A_OPERATOR_IDENTITY_MISMATCH,
                correlation_id=None,
                idempotency_key=None,
                payload={"requested_operator": str(product_operator_id)},
            )
            raise TelegramOperatorError(E6A_OPERATOR_IDENTITY_MISMATCH, "identity operator mismatch")
        context = normalize_binding_context(
            telegram_user_id=telegram_user_id,
            chat_id=chat_id,
            thread_id=thread_id,
            chat_binding_kind=chat_binding_kind,
        )
        status = ensure_binding_status(binding_status)
        now = _now_iso()
        self._conn.execute(
            """
            INSERT INTO telegram_chat_bindings(
                product_operator_id, telegram_user_id, chat_id, thread_id, chat_binding_kind, binding_status,
                created_at, activated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                str(product_operator_id),
                int(context["telegram_user_id"]),
                int(context["chat_id"]),
                context["thread_id"],
                ensure_chat_binding_kind(str(context["chat_binding_kind"])),
                status,
                now,
                now if status == "ACTIVE" else None,
            ),
        )
        if status == "ACTIVE":
            self.set_identity_access(telegram_user_id=int(telegram_user_id), telegram_access_status="ACTIVE")
        row = self.read_binding(
            product_operator_id=str(product_operator_id),
            telegram_user_id=int(context["telegram_user_id"]),
            chat_id=int(context["chat_id"]),
            thread_id=context["thread_id"],
        )
        emit_audit_event(
            self._conn,
            event_type="TELEGRAM_CHAT_BINDING_CREATED",
            telegram_user_id=int(context["telegram_user_id"]),
            resolved_product_operator_id=str(product_operator_id),
            chat_id=int(context["chat_id"]),
            thread_id=context["thread_id"],
            binding_id=int(row["id"]),
            action_type="BINDING_CREATE",
            action_class=str(identity["max_permission_class"]),
            target_entity_type="telegram_binding",
            target_entity_ref=f"{context['chat_id']}:{context['thread_id']}",
            gateway_result=None,
            gateway_error_code=None,
            correlation_id=None,
            idempotency_key=None,
            payload={"binding_status": status, "chat_binding_kind": context["chat_binding_kind"]},
        )
        return row

    def read_binding(self, *, product_operator_id: str, telegram_user_id: int, chat_id: int, thread_id: int | None) -> dict[str, Any] | None:
        return self._conn.execute(
            """
            SELECT * FROM telegram_chat_bindings
            WHERE product_operator_id = ? AND telegram_user_id = ? AND chat_id = ?
              AND COALESCE(thread_id, -1) = COALESCE(?, -1)
            """,
            (str(product_operator_id), int(telegram_user_id), int(chat_id), thread_id),
        ).fetchone()

    def update_binding_status(self, *, binding_id: int, binding_status: str, last_error_code: str | None = None) -> dict[str, Any]:
        status = ensure_binding_status(binding_status)
        now = _now_iso()
        self._conn.execute(
            """
            UPDATE telegram_chat_bindings
            SET binding_status = ?,
                activated_at = CASE WHEN ? = 'ACTIVE' THEN ? ELSE activated_at END,
                disabled_at = CASE WHEN ? = 'DISABLED' THEN ? ELSE disabled_at END,
                revoked_at = CASE WHEN ? = 'REVOKED' THEN ? ELSE revoked_at END,
                last_error_code = ?
            WHERE id = ?
            """,
            (status, status, now, status, now, status, now, last_error_code, int(binding_id)),
        )
        row = self._conn.execute("SELECT * FROM telegram_chat_bindings WHERE id = ?", (int(binding_id),)).fetchone()
        if row is None:
            raise TelegramOperatorError(E6A_CHAT_BINDING_MISSING, "chat binding not found")
        emit_audit_event(
            self._conn,
            event_type="TELEGRAM_CHAT_BINDING_STATUS_CHANGED",
            telegram_user_id=int(row["telegram_user_id"]),
            resolved_product_operator_id=str(row["product_operator_id"]),
            chat_id=int(row["chat_id"]),
            thread_id=row["thread_id"],
            binding_id=int(row["id"]),
            action_type="BINDING_STATUS_UPDATE",
            action_class=None,
            target_entity_type="telegram_binding",
            target_entity_ref=str(row["id"]),
            gateway_result=None,
            gateway_error_code=last_error_code,
            correlation_id=None,
            idempotency_key=None,
            payload={"binding_status": status},
        )
        return row

    def list_bindings_for_operator(self, *, product_operator_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM telegram_chat_bindings WHERE product_operator_id = ? ORDER BY id ASC",
            (str(product_operator_id),),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_bindings_for_telegram_identity(self, *, telegram_user_id: int, requester_product_operator_id: str | None = None) -> list[dict[str, Any]]:
        identity = self._require_identity(telegram_user_id)
        if requester_product_operator_id and str(identity["product_operator_id"]) != str(requester_product_operator_id):
            raise TelegramOperatorError(E6A_OPERATOR_IDENTITY_MISMATCH, "identity read unauthorized")
        rows = self._conn.execute(
            "SELECT * FROM telegram_chat_bindings WHERE telegram_user_id = ? ORDER BY id ASC",
            (int(telegram_user_id),),
        ).fetchall()
        return [dict(r) for r in rows]

    def whoami(self, *, telegram_user_id: int, chat_id: int, thread_id: int | None) -> dict[str, Any]:
        identity = self.get_identity(telegram_user_id=telegram_user_id)
        if identity is None:
            emit_audit_event(self._conn, event_type="TELEGRAM_WHOAMI_REQUESTED", telegram_user_id=int(telegram_user_id), resolved_product_operator_id=None, chat_id=int(chat_id), thread_id=thread_id, binding_id=None, action_type="WHOAMI", action_class="READ_ONLY", target_entity_type="telegram_identity", target_entity_ref=str(telegram_user_id), gateway_result=None, gateway_error_code=E6A_TELEGRAM_IDENTITY_UNBOUND, correlation_id=None, idempotency_key=None, payload={"bound": False})
            return {
                "telegram_identity": {"telegram_user_id": int(telegram_user_id), "status": "UNBOUND"},
                "resolved_product_operator_id": None,
                "current_access_status": None,
                "binding_summary": {"matched": False, "binding_status": None},
                "effective_max_allowed_action_class": "READ_ONLY",
                "error": {"code": E6A_TELEGRAM_IDENTITY_UNBOUND, "message": "telegram identity is not enrolled"},
            }

        access_status = str(identity["telegram_access_status"])
        bindings = self.list_bindings_for_telegram_identity(telegram_user_id=telegram_user_id)
        matched = next((b for b in bindings if int(b["chat_id"]) == int(chat_id) and int(b["thread_id"] or -1) == int(thread_id or -1)), None)

        effective_class = "READ_ONLY"
        error = None
        if access_status == "REVOKED":
            error = {"code": E6A_OPERATOR_REVOKED, "message": "operator access revoked"}
        elif access_status == "INACTIVE":
            error = {"code": E6A_OPERATOR_INACTIVE, "message": "operator access inactive"}
        elif matched is None:
            error = {"code": E6A_CHAT_BINDING_MISSING, "message": "trusted binding missing for chat/thread"}
        elif str(matched["binding_status"]) == "DISABLED":
            error = {"code": E6A_CHAT_BINDING_DISABLED, "message": "binding disabled"}
        elif str(matched["binding_status"]) == "REVOKED":
            error = {"code": E6A_CHAT_BINDING_REVOKED, "message": "binding revoked"}
        else:
            effective_class = str(identity["max_permission_class"])

        emit_audit_event(self._conn, event_type="TELEGRAM_WHOAMI_REQUESTED", telegram_user_id=int(telegram_user_id), resolved_product_operator_id=str(identity["product_operator_id"]), chat_id=int(chat_id), thread_id=thread_id, binding_id=(int(matched["id"]) if matched else None), action_type="WHOAMI", action_class="READ_ONLY", target_entity_type="telegram_identity", target_entity_ref=str(telegram_user_id), gateway_result=None, gateway_error_code=(error["code"] if error else None), correlation_id=None, idempotency_key=None, payload={"bound": True, "matched": matched is not None})
        return {
            "telegram_identity": {
                "telegram_user_id": int(telegram_user_id),
                "status": access_status,
                "enrolled_at": identity["enrolled_at"],
            },
            "resolved_product_operator_id": str(identity["product_operator_id"]),
            "current_access_status": access_status,
            "binding_summary": {
                "matched": matched is not None,
                "binding_status": (str(matched["binding_status"]) if matched is not None else None),
                "total_bindings": len(bindings),
            },
            "effective_max_allowed_action_class": effective_class,
            "error": error,
        }

    def binding_health(self, *, telegram_user_id: int, chat_id: int, thread_id: int | None) -> dict[str, Any]:
        who = self.whoami(telegram_user_id=telegram_user_id, chat_id=chat_id, thread_id=thread_id)
        return {
            "ok": who["error"] is None,
            "telegram_user_id": int(telegram_user_id),
            "chat_id": int(chat_id),
            "thread_id": thread_id,
            "current_access_status": who["current_access_status"],
            "binding_summary": who["binding_summary"],
            "last_error": who["error"],
        }

    def gateway_health_style(self, *, telegram_user_id: int, chat_id: int, thread_id: int | None) -> dict[str, Any]:
        who = self.whoami(telegram_user_id=telegram_user_id, chat_id=chat_id, thread_id=thread_id)
        return {
            "gateway_ready": who["error"] is None,
            "resolved_operator": who["resolved_product_operator_id"],
            "effective_action_class": who["effective_max_allowed_action_class"],
            "denial_reason": who["error"],
        }

    def _has_active_binding(self, *, telegram_user_id: int, product_operator_id: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM telegram_chat_bindings
            WHERE telegram_user_id = ? AND product_operator_id = ? AND binding_status = 'ACTIVE'
            LIMIT 1
            """,
            (int(telegram_user_id), str(product_operator_id)),
        ).fetchone()
        return row is not None

    def _require_identity(self, telegram_user_id: int) -> Any:
        row = self.get_identity(telegram_user_id=telegram_user_id)
        if row is None:
            raise TelegramOperatorError(E6A_TELEGRAM_IDENTITY_UNBOUND, "telegram identity is not enrolled")
        return row

    def get_identity(self, *, telegram_user_id: int) -> Any:
        return self._conn.execute(
            "SELECT * FROM telegram_operator_identities WHERE telegram_user_id = ?",
            (int(telegram_user_id),),
        ).fetchone()


def dump_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
