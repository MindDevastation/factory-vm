from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .digest import assemble_digest
from .lifecycle import require_transition
from .observability import emit_inbox_event


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TelegramInboxRuntime:
    def __init__(self, conn: Any):
        self._conn = conn

    def list_current(self, *, telegram_user_id: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT * FROM telegram_inbox_messages
            WHERE telegram_user_id = ? AND lifecycle_state IN ('ACTIVE','INFORMATIONAL','INFO_ONLY')
            ORDER BY created_at DESC, id DESC
            """,
            (int(telegram_user_id),),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_history(self, *, telegram_user_id: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM telegram_inbox_messages WHERE telegram_user_id = ? ORDER BY created_at DESC, id DESC",
            (int(telegram_user_id),),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_detail(self, *, message_id: int) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM telegram_inbox_messages WHERE id = ?", (int(message_id),)).fetchone()
        return dict(row) if row else None

    def acknowledge(self, *, message_id: int, telegram_user_id: int, ack_note: str | None = None) -> dict[str, Any]:
        now = _now_iso()
        self._conn.execute(
            """
            INSERT OR REPLACE INTO telegram_inbox_acknowledgments(
                message_id, telegram_user_id, acknowledged_at, ack_note, open_context_ref, escalation_ref
            ) VALUES(?,?,?,?,?,?)
            """,
            (int(message_id), int(telegram_user_id), now, ack_note, None, None),
        )
        self._conn.execute(
            """
            INSERT INTO telegram_inbox_lifecycle_events(message_id, from_state, to_state, reason_code, actor_type, actor_ref, created_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (int(message_id), None, "ACTIVE", "ACKNOWLEDGED", "operator", str(telegram_user_id), now),
        )
        msg = self.get_detail(message_id=int(message_id))
        emit_inbox_event(self._conn, event_type="MESSAGE_ACKNOWLEDGED", message_id=int(message_id), telegram_user_id=int(telegram_user_id), product_operator_id=(str(msg["product_operator_id"]) if msg else None), chat_id=(int(msg["chat_id"]) if msg and msg["chat_id"] is not None else None), thread_id=(msg["thread_id"] if msg else None), message_family=(str(msg["message_family"]) if msg else None), category=(str(msg["category"]) if msg else None), severity=(str(msg["severity"]) if msg else None), target_context=None, lifecycle_state=(str(msg["lifecycle_state"]) if msg else None), routing_result="ACK", reason_code=None, payload={"ack_note": ack_note})
        return {"acknowledged": True, "message_id": int(message_id), "telegram_user_id": int(telegram_user_id)}

    def transition_message(self, *, message_id: int, to_state: str, reason_code: str, actor_ref: str) -> dict[str, Any]:
        row = self._conn.execute("SELECT lifecycle_state FROM telegram_inbox_messages WHERE id = ?", (int(message_id),)).fetchone()
        if row is None:
            raise ValueError("message not found")
        from_state = str(row["lifecycle_state"])
        resolved = require_transition(from_state=from_state, to_state=to_state)
        now = _now_iso()
        self._conn.execute(
            "UPDATE telegram_inbox_messages SET lifecycle_state = ?, resolved_at = CASE WHEN ? IN ('RESOLVED','EXPIRED') THEN ? ELSE resolved_at END WHERE id = ?",
            (resolved, resolved, now, int(message_id)),
        )
        self._conn.execute(
            "INSERT INTO telegram_inbox_lifecycle_events(message_id, from_state, to_state, reason_code, actor_type, actor_ref, created_at) VALUES(?,?,?,?,?,?,?)",
            (int(message_id), from_state, resolved, str(reason_code), "system", str(actor_ref), now),
        )
        msg = self.get_detail(message_id=int(message_id))
        evt_type = "MESSAGE_SUPERSEDED" if resolved == "SUPERSEDED" else "MESSAGE_RESOLVED" if resolved == "RESOLVED" else "MESSAGE_EXPIRED" if resolved == "EXPIRED" else "MESSAGE_LIFECYCLE_UPDATED"
        emit_inbox_event(self._conn, event_type=evt_type, message_id=int(message_id), telegram_user_id=(int(msg["telegram_user_id"]) if msg else None), product_operator_id=(str(msg["product_operator_id"]) if msg else None), chat_id=(int(msg["chat_id"]) if msg and msg["chat_id"] is not None else None), thread_id=(msg["thread_id"] if msg else None), message_family=(str(msg["message_family"]) if msg else None), category=(str(msg["category"]) if msg else None), severity=(str(msg["severity"]) if msg else None), target_context=None, lifecycle_state=resolved, routing_result="LIFECYCLE", reason_code=str(reason_code), payload={"from_state": from_state, "actor_ref": actor_ref})
        return {"message_id": int(message_id), "from_state": from_state, "to_state": resolved}

    def emit_follow_up(self, *, source_message_id: int, title: str, body: str) -> dict[str, Any]:
        src = self._conn.execute("SELECT * FROM telegram_inbox_messages WHERE id = ?", (int(source_message_id),)).fetchone()
        if src is None:
            raise ValueError("source message not found")
        now = _now_iso()
        dedupe_key = f"followup:{source_message_id}:{now}"
        cur = self._conn.execute(
            """
            INSERT INTO telegram_inbox_messages(
                message_family, category, severity, actionability_class, lifecycle_state, stale_behavior, delivery_behavior,
                telegram_user_id, product_operator_id, chat_id, thread_id, binding_id,
                target_entity_type, target_entity_ref, target_context_json, title, body, dedupe_key,
                followup_key, related_message_id, upstream_event_family, upstream_event_ref, created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "UNRESOLVED_FOLLOW_UP",
                "FOLLOW_UP",
                "MEDIUM",
                "ACTIONABLE",
                "ACTIVE",
                "SUPERSEDE",
                "FOLLOW_UP_ONLY",
                int(src["telegram_user_id"]),
                src["product_operator_id"],
                src["chat_id"],
                src["thread_id"],
                src["binding_id"],
                src["target_entity_type"],
                src["target_entity_ref"],
                src["target_context_json"],
                str(title),
                str(body),
                dedupe_key,
                src["followup_key"],
                int(source_message_id),
                "stale/follow_up",
                f"followup:{source_message_id}",
                now,
            ),
        )
        new_id = int(cur.lastrowid)
        emit_inbox_event(self._conn, event_type="FOLLOW_UP_EMITTED", message_id=new_id, telegram_user_id=int(src["telegram_user_id"]), product_operator_id=(str(src["product_operator_id"]) if src["product_operator_id"] is not None else None), chat_id=(int(src["chat_id"]) if src["chat_id"] is not None else None), thread_id=src["thread_id"], message_family="UNRESOLVED_FOLLOW_UP", category="FOLLOW_UP", severity="MEDIUM", target_context=None, lifecycle_state="ACTIVE", routing_result="FOLLOW_UP", reason_code=None, payload={"related_message_id": int(source_message_id)})
        return {"message_id": new_id, "related_message_id": int(source_message_id)}

    def open_related_context(self, *, message_id: int) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT target_entity_type, target_entity_ref FROM telegram_inbox_messages WHERE id = ?",
            (int(message_id),),
        ).fetchone()
        if row is None:
            raise ValueError("message not found")
        return {
            "navigation_only": True,
            "target": {
                "entity_type": str(row["target_entity_type"]),
                "entity_ref": str(row["target_entity_ref"]),
            },
        }

    def build_digest_for_user(self, *, telegram_user_id: int) -> dict[str, Any]:
        return assemble_digest(self.list_current(telegram_user_id=int(telegram_user_id)))
