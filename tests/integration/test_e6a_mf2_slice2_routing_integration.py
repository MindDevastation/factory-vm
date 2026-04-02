from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_inbox import TelegramInboxRouter
from services.telegram_operator import TelegramOperatorRegistry
from tests._helpers import temp_env


class TestE6AMf2Slice2RoutingIntegration(unittest.TestCase):
    def _bind(self, conn):
        reg = TelegramOperatorRegistry(conn)
        reg.start_enrollment(product_operator_id="op-a", telegram_user_id=4401, max_permission_class="STANDARD_OPERATOR_MUTATE")
        reg.create_binding(
            product_operator_id="op-a",
            telegram_user_id=4401,
            chat_id=-84401,
            thread_id=None,
            chat_binding_kind="PRIVATE_CHAT",
            binding_status="ACTIVE",
        )

    def test_routes_multiple_upstream_families_to_bound_context(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                self._bind(conn)
                router = TelegramInboxRouter(conn)
                for fam in ("publish/manual_handoff", "readiness/blocker", "recovery/ops", "worker/health"):
                    out = router.route_event(
                        product_operator_id="op-a",
                        telegram_user_id=4401,
                        chat_id=-84401,
                        thread_id=None,
                        upstream_event_family=fam,
                        upstream_event_ref=f"evt-{fam}",
                        target_entity_type="release",
                        target_entity_ref="rel-x",
                        title="title",
                        body="body",
                        attributes={"scope": fam},
                    )
                    self.assertIn(out["delivery_result"], {"DELIVERED", "SUPPRESSED"})
            finally:
                conn.close()

    def test_immediate_vs_digest_vs_followup_and_dedupe(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                self._bind(conn)
                router = TelegramInboxRouter(conn)

                digest = router.route_event(
                    product_operator_id="op-a",
                    telegram_user_id=4401,
                    chat_id=-84401,
                    thread_id=None,
                    upstream_event_family="digest/summary",
                    upstream_event_ref="evt-digest",
                    target_entity_type="channel",
                    target_entity_ref="darkwood-reverie",
                    title="digest",
                    body="digest-body",
                )
                self.assertEqual(digest["classification"]["delivery_behavior"], "DIGEST")

                follow = router.route_event(
                    product_operator_id="op-a",
                    telegram_user_id=4401,
                    chat_id=-84401,
                    thread_id=None,
                    upstream_event_family="stale/follow_up",
                    upstream_event_ref="evt-follow",
                    target_entity_type="release",
                    target_entity_ref="rel-follow",
                    title="follow",
                    body="follow-body",
                )
                self.assertEqual(follow["classification"]["delivery_behavior"], "FOLLOW_UP_ONLY")

                first = router.route_event(
                    product_operator_id="op-a",
                    telegram_user_id=4401,
                    chat_id=-84401,
                    thread_id=None,
                    upstream_event_family="publish/manual_handoff",
                    upstream_event_ref="evt-same",
                    target_entity_type="release",
                    target_entity_ref="rel-same",
                    title="same",
                    body="same",
                )
                second = router.route_event(
                    product_operator_id="op-a",
                    telegram_user_id=4401,
                    chat_id=-84401,
                    thread_id=None,
                    upstream_event_family="publish/manual_handoff",
                    upstream_event_ref="evt-same",
                    target_entity_type="release",
                    target_entity_ref="rel-same",
                    title="same",
                    body="same",
                )
                self.assertEqual(first["delivery_result"], "DELIVERED")
                self.assertEqual(second["delivery_result"], "SUPPRESSED")
            finally:
                conn.close()

    def test_fail_closed_when_binding_invalid(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                router = TelegramInboxRouter(conn)
                denied = router.route_event(
                    product_operator_id="op-a",
                    telegram_user_id=4401,
                    chat_id=-84401,
                    thread_id=None,
                    upstream_event_family="publish/manual_handoff",
                    upstream_event_ref="evt-denied",
                    target_entity_type="release",
                    target_entity_ref="rel-denied",
                    title="denied",
                    body="denied",
                )
                self.assertEqual(denied["delivery_result"], "DENIED")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
