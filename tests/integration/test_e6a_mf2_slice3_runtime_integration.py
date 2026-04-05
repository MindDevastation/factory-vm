from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_inbox import TelegramInboxRouter, TelegramInboxRuntime
from services.telegram_operator import TelegramOperatorRegistry
from tests._helpers import temp_env


class TestE6AMf2Slice3RuntimeIntegration(unittest.TestCase):
    def _seed_message(self, conn) -> int:
        reg = TelegramOperatorRegistry(conn)
        reg.start_enrollment(product_operator_id="op-r", telegram_user_id=5501)
        reg.create_binding(
            product_operator_id="op-r",
            telegram_user_id=5501,
            chat_id=-95501,
            thread_id=None,
            chat_binding_kind="PRIVATE_CHAT",
            binding_status="ACTIVE",
        )
        router = TelegramInboxRouter(conn)
        out = router.route_event(
            product_operator_id="op-r",
            telegram_user_id=5501,
            chat_id=-95501,
            thread_id=None,
            upstream_event_family="publish/manual_handoff",
            upstream_event_ref="evt-runtime",
            target_entity_type="release",
            target_entity_ref="rel-runtime",
            title="runtime",
            body="runtime",
        )
        return int(out["message_id"])

    def test_acknowledge_open_context_and_unresolved(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                message_id = self._seed_message(conn)
                rt = TelegramInboxRuntime(conn)
                ack = rt.acknowledge(message_id=message_id, telegram_user_id=5501, ack_note="seen")
                self.assertTrue(bool(ack["acknowledged"]))

                nav = rt.open_related_context(message_id=message_id)
                self.assertTrue(bool(nav["navigation_only"]))

                current = rt.list_current(telegram_user_id=5501)
                self.assertTrue(any(int(i["id"]) == message_id for i in current))
            finally:
                conn.close()

    def test_supersede_resolve_expire_and_followup_continuity(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                message_id = self._seed_message(conn)
                rt = TelegramInboxRuntime(conn)

                sup = rt.transition_message(message_id=message_id, to_state="SUPERSEDED", reason_code="newer_context", actor_ref="system")
                self.assertEqual(sup["to_state"], "SUPERSEDED")

                follow = rt.emit_follow_up(source_message_id=message_id, title="follow-up", body="follow-up")
                self.assertEqual(follow["related_message_id"], message_id)

                rt.transition_message(message_id=follow["message_id"], to_state="RESOLVED", reason_code="done", actor_ref="operator")
                detail = rt.get_detail(message_id=follow["message_id"])
                self.assertEqual(str(detail["lifecycle_state"]), "RESOLVED")

                rt.transition_message(message_id=message_id, to_state="EXPIRED", reason_code="timeout", actor_ref="system")
                current_ids = {int(r["id"]) for r in rt.list_current(telegram_user_id=5501)}
                self.assertNotIn(message_id, current_ids)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
