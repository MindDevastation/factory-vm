from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_inbox import TelegramInboxRouter, TelegramInboxRuntime
from services.telegram_operator import TelegramOperatorRegistry
from tests._helpers import temp_env


class TestE6AMf2Slice4Hardening(unittest.TestCase):
    def test_observability_events_and_fields(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                reg = TelegramOperatorRegistry(conn)
                reg.start_enrollment(product_operator_id="op-h", telegram_user_id=6601)
                reg.create_binding(
                    product_operator_id="op-h",
                    telegram_user_id=6601,
                    chat_id=-96601,
                    thread_id=None,
                    chat_binding_kind="PRIVATE_CHAT",
                    binding_status="ACTIVE",
                )
                router = TelegramInboxRouter(conn)
                out = router.route_event(
                    product_operator_id="op-h",
                    telegram_user_id=6601,
                    chat_id=-96601,
                    thread_id=None,
                    upstream_event_family="publish/manual_handoff",
                    upstream_event_ref="evt-hard",
                    target_entity_type="release",
                    target_entity_ref="rel-hard",
                    title="hard",
                    body="hard",
                )
                rt = TelegramInboxRuntime(conn)
                rt.acknowledge(message_id=int(out["message_id"]), telegram_user_id=6601, ack_note="seen")
                rt.transition_message(message_id=int(out["message_id"]), to_state="SUPERSEDED", reason_code="newer", actor_ref="system")
                follow = rt.emit_follow_up(source_message_id=int(out["message_id"]), title="f", body="f")
                rt.transition_message(message_id=int(follow["message_id"]), to_state="RESOLVED", reason_code="resolved", actor_ref="system")

                denied = router.route_event(
                    product_operator_id="op-h",
                    telegram_user_id=9999,
                    chat_id=-1,
                    thread_id=None,
                    upstream_event_family="worker/health",
                    upstream_event_ref="evt-denied",
                    target_entity_type="worker",
                    target_entity_ref="w1",
                    title="d",
                    body="d",
                )
                self.assertEqual(denied["delivery_result"], "DENIED")

                rows = conn.execute("SELECT * FROM telegram_inbox_events ORDER BY id ASC").fetchall()
                self.assertGreaterEqual(len(rows), 6)
                required = {"telegram_user_id", "product_operator_id", "chat_id", "thread_id", "message_family", "category", "severity", "lifecycle_state", "routing_result"}
                for row in rows:
                    self.assertTrue(required.issubset(set(row.keys())))
                event_types = {str(r["event_type"]) for r in rows}
                self.assertIn("MESSAGE_CREATED", event_types)
                self.assertIn("MESSAGE_ROUTED", event_types)
                self.assertIn("MESSAGE_ACKNOWLEDGED", event_types)
                self.assertIn("MESSAGE_SUPERSEDED", event_types)
                self.assertIn("MESSAGE_RESOLVED", event_types)
                self.assertIn("FOLLOW_UP_EMITTED", event_types)
                self.assertIn("MESSAGE_DELIVERY_DENIED", event_types)
            finally:
                conn.close()

    def test_stale_messages_not_current_and_mf1_intact(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                reg = TelegramOperatorRegistry(conn)
                reg.start_enrollment(product_operator_id="op-h2", telegram_user_id=6602)
                reg.create_binding(
                    product_operator_id="op-h2",
                    telegram_user_id=6602,
                    chat_id=-96602,
                    thread_id=None,
                    chat_binding_kind="PRIVATE_CHAT",
                    binding_status="ACTIVE",
                )
                router = TelegramInboxRouter(conn)
                out = router.route_event(
                    product_operator_id="op-h2",
                    telegram_user_id=6602,
                    chat_id=-96602,
                    thread_id=None,
                    upstream_event_family="stale/follow_up",
                    upstream_event_ref="evt-s",
                    target_entity_type="release",
                    target_entity_ref="rel-s",
                    title="s",
                    body="s",
                )
                rt = TelegramInboxRuntime(conn)
                rt.transition_message(message_id=int(out["message_id"]), to_state="EXPIRED", reason_code="ttl", actor_ref="system")
                current_ids = {int(r["id"]) for r in rt.list_current(telegram_user_id=6602)}
                self.assertNotIn(int(out["message_id"]), current_ids)

                tables = {str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertIn("telegram_operator_identities", tables)
                self.assertIn("telegram_chat_bindings", tables)
                self.assertIn("telegram_action_gateway_events", tables)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
