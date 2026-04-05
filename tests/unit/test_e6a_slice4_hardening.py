from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_operator import TelegramActionGateway, TelegramOperatorRegistry, build_action_envelope
from tests._helpers import temp_env


class TestE6ASlice4Hardening(unittest.TestCase):
    def test_audit_events_include_required_fields_for_enroll_binding_whoami_gateway(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                reg = TelegramOperatorRegistry(conn)
                reg.start_enrollment(product_operator_id="operator-9", telegram_user_id=9001, max_permission_class="STANDARD_OPERATOR_MUTATE")
                binding = reg.create_binding(
                    product_operator_id="operator-9",
                    telegram_user_id=9001,
                    chat_id=-9901,
                    thread_id=None,
                    chat_binding_kind="PRIVATE_CHAT",
                    binding_status="ACTIVE",
                )
                reg.whoami(telegram_user_id=9001, chat_id=-9901, thread_id=None)

                gw = TelegramActionGateway(conn)
                envlp = build_action_envelope(
                    action_transport_type="COMMAND",
                    action_transport_id="cmd-hard-1",
                    telegram_user_id=9001,
                    chat_id=-9901,
                    thread_id=None,
                    action_type="READ_X",
                    action_class="READ_ONLY",
                    target_entity_type="release",
                    target_entity_ref="rel-hard-1",
                    freshness_context={},
                    correlation_id="corr-hard-1",
                )
                gw.evaluate(
                    envlp,
                    target_resolver=lambda _e: {"result": "FOUND"},
                    stale_precheck_hook=lambda _e: {"result": "FRESH"},
                    idempotency_hook=lambda _e: {"result": "OK"},
                )

                rows = conn.execute(
                    "SELECT * FROM telegram_operator_audit_events WHERE telegram_user_id = ? ORDER BY id ASC",
                    (9001,),
                ).fetchall()
                self.assertGreaterEqual(len(rows), 4)
                for row in rows:
                    self.assertIn("event_type", row.keys())
                    self.assertIn("telegram_user_id", row.keys())
                    self.assertIn("gateway_result", row.keys())
                    self.assertIn("correlation_id", row.keys())
                    self.assertIn("payload_json", row.keys())
                self.assertEqual(int(binding["telegram_user_id"]), 9001)
            finally:
                conn.close()

    def test_denied_expired_stale_and_mismatch_auditable(self) -> None:
        from datetime import datetime, timedelta, timezone

        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                reg = TelegramOperatorRegistry(conn)
                reg.start_enrollment(product_operator_id="operator-10", telegram_user_id=9002)
                reg.create_binding(
                    product_operator_id="operator-10",
                    telegram_user_id=9002,
                    chat_id=-9902,
                    thread_id=None,
                    chat_binding_kind="PRIVATE_CHAT",
                    binding_status="ACTIVE",
                )

                gw = TelegramActionGateway(conn)
                denied = build_action_envelope(
                    action_transport_type="COMMAND",
                    action_transport_id="cmd-denied",
                    telegram_user_id=9002,
                    chat_id=-9902,
                    thread_id=None,
                    action_type="MUTATE_X",
                    action_class="PRIVILEGED_OPERATOR_MUTATE",
                    target_entity_type="release",
                    target_entity_ref="rel-hard-2",
                    freshness_context={},
                    correlation_id="corr-denied",
                )
                gw.evaluate(denied, target_resolver=lambda _e: {"result": "FOUND"}, stale_precheck_hook=lambda _e: {"result": "FRESH"}, idempotency_hook=lambda _e: {"result": "OK"})

                stale = build_action_envelope(
                    action_transport_type="CALLBACK",
                    action_transport_id="cb-stale",
                    telegram_user_id=9002,
                    chat_id=-9902,
                    thread_id=None,
                    action_type="READ_X",
                    action_class="READ_ONLY",
                    target_entity_type="release",
                    target_entity_ref="rel-hard-3",
                    freshness_context={},
                    correlation_id="corr-stale",
                )
                gw.evaluate(stale, target_resolver=lambda _e: {"result": "FOUND"}, stale_precheck_hook=lambda _e: {"result": "STALE"}, idempotency_hook=lambda _e: {"result": "OK"})

                expired = build_action_envelope(
                    action_transport_type="CALLBACK",
                    action_transport_id="cb-expired",
                    telegram_user_id=9002,
                    chat_id=-9902,
                    thread_id=None,
                    action_type="READ_X",
                    action_class="READ_ONLY",
                    target_entity_type="release",
                    target_entity_ref="rel-hard-4",
                    freshness_context={},
                    correlation_id="corr-expired",
                    expires_at=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
                )
                gw.evaluate(expired, target_resolver=lambda _e: {"result": "FOUND"}, stale_precheck_hook=lambda _e: {"result": "FRESH"}, idempotency_hook=lambda _e: {"result": "OK"})

                with self.assertRaises(Exception):
                    reg.create_binding(
                        product_operator_id="operator-999",
                        telegram_user_id=9002,
                        chat_id=-9999,
                        thread_id=None,
                        chat_binding_kind="PRIVATE_CHAT",
                        binding_status="ACTIVE",
                    )

                event_types = {
                    str(r["event_type"])
                    for r in conn.execute("SELECT event_type FROM telegram_operator_audit_events WHERE telegram_user_id = ?", (9002,)).fetchall()
                }
                self.assertIn("TELEGRAM_GATEWAY_DENIED", event_types)
                self.assertIn("TELEGRAM_ACTION_STALE", event_types)
                self.assertIn("TELEGRAM_ACTION_EXPIRED", event_types)
                self.assertIn("TELEGRAM_IDENTITY_MISMATCH_DETECTED", event_types)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
