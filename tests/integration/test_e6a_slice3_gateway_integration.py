from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from services.common import db as dbm
from services.telegram_operator import TelegramActionGateway, TelegramOperatorRegistry, build_action_envelope, downstream_mutation_stub
from tests._helpers import temp_env


class TestE6ASlice3GatewayIntegration(unittest.TestCase):
    def _setup_identity_binding(self, conn):
        svc = TelegramOperatorRegistry(conn)
        svc.start_enrollment(product_operator_id="operator-1", telegram_user_id=3001, max_permission_class="STANDARD_OPERATOR_MUTATE")
        svc.create_binding(
            product_operator_id="operator-1",
            telegram_user_id=3001,
            chat_id=-9001,
            thread_id=None,
            chat_binding_kind="PRIVATE_CHAT",
            binding_status="ACTIVE",
        )

    def test_permission_denial_and_event_persisted(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                self._setup_identity_binding(conn)
                gw = TelegramActionGateway(conn)
                envelope = build_action_envelope(
                    action_transport_type="COMMAND",
                    action_transport_id="cmd-99",
                    telegram_user_id=3001,
                    chat_id=-9001,
                    thread_id=None,
                    action_type="MUTATE_X",
                    action_class="PRIVILEGED_OPERATOR_MUTATE",
                    target_entity_type="release",
                    target_entity_ref="rel-9",
                    freshness_context={},
                    correlation_id="corr-99",
                )
                result = gw.evaluate(
                    envelope,
                    target_resolver=lambda _e: {"result": "FOUND"},
                    stale_precheck_hook=lambda _e: {"result": "FRESH"},
                    idempotency_hook=lambda _e: {"result": "OK"},
                )
                self.assertEqual(result["gateway_result"], "DENIED")
                ev = conn.execute("SELECT gateway_result FROM telegram_action_gateway_events WHERE correlation_id=?", ("corr-99",)).fetchone()
                self.assertEqual(str(ev["gateway_result"]), "DENIED")
            finally:
                conn.close()

    def test_allowed_and_downstream_stub_guard(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                self._setup_identity_binding(conn)
                gw = TelegramActionGateway(conn)
                envelope = build_action_envelope(
                    action_transport_type="CALLBACK",
                    action_transport_id="cb-1",
                    telegram_user_id=3001,
                    chat_id=-9001,
                    thread_id=None,
                    action_type="READ_X",
                    action_class="READ_ONLY",
                    target_entity_type="release",
                    target_entity_ref="rel-10",
                    freshness_context={},
                    correlation_id="corr-allowed",
                )
                result = gw.evaluate(
                    envelope,
                    target_resolver=lambda _e: {"result": "FOUND"},
                    stale_precheck_hook=lambda _e: {"result": "FRESH"},
                    idempotency_hook=lambda _e: {"result": "OK"},
                )
                self.assertEqual(result["gateway_result"], "ALLOWED")
                self.assertTrue(bool(downstream_mutation_stub(gateway_result=result)["executed"]))
            finally:
                conn.close()

    def test_expired_action_and_hook_contracts(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                self._setup_identity_binding(conn)
                gw = TelegramActionGateway(conn)
                envelope = build_action_envelope(
                    action_transport_type="CALLBACK",
                    action_transport_id="cb-expired",
                    telegram_user_id=3001,
                    chat_id=-9001,
                    thread_id=None,
                    action_type="READ_X",
                    action_class="READ_ONLY",
                    target_entity_type="release",
                    target_entity_ref="rel-10",
                    freshness_context={},
                    correlation_id="corr-expired",
                    expires_at=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
                )
                result = gw.evaluate(
                    envelope,
                    target_resolver=lambda _e: {"result": "FOUND"},
                    stale_precheck_hook=lambda _e: {"result": "FRESH"},
                    idempotency_hook=lambda _e: {"result": "OK"},
                )
                self.assertEqual(result["gateway_result"], "EXPIRED")
                self.assertIn("stale_precheck_hook_ref", result)
                self.assertIn("idempotency_hook_ref", result)
                ev = conn.execute("SELECT gateway_result FROM telegram_action_gateway_events WHERE correlation_id=?", ("corr-expired",)).fetchone()
                self.assertEqual(str(ev["gateway_result"]), "EXPIRED")
            finally:
                conn.close()

    def test_revoked_binding_rejected(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                self._setup_identity_binding(conn)
                reg = TelegramOperatorRegistry(conn)
                binding = reg.list_bindings_for_operator(product_operator_id="operator-1")[0]
                reg.update_binding_status(binding_id=int(binding["id"]), binding_status="REVOKED")

                gw = TelegramActionGateway(conn)
                envelope = build_action_envelope(
                    action_transport_type="COMMAND",
                    action_transport_id="cmd-revoked",
                    telegram_user_id=3001,
                    chat_id=-9001,
                    thread_id=None,
                    action_type="READ_X",
                    action_class="READ_ONLY",
                    target_entity_type="release",
                    target_entity_ref="rel-11",
                    freshness_context={},
                    correlation_id="corr-revoked",
                )
                result = gw.evaluate(
                    envelope,
                    target_resolver=lambda _e: {"result": "FOUND"},
                    stale_precheck_hook=lambda _e: {"result": "FRESH"},
                    idempotency_hook=lambda _e: {"result": "OK"},
                )
                self.assertEqual(result["error"]["code"], "E6A_CHAT_BINDING_REVOKED")
                self.assertFalse(bool(downstream_mutation_stub(gateway_result=result)["executed"]))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
