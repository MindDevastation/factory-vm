from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from services.telegram_operator import (
    build_action_envelope,
    build_gateway_decision,
    is_envelope_expired,
    permission_allows,
    permission_rank,
    to_telegram_safe_error,
)


class TestE6ASlice3GatewayUnit(unittest.TestCase):
    def test_permission_class_comparison_behavior(self) -> None:
        self.assertLess(permission_rank("READ_ONLY"), permission_rank("STANDARD_OPERATOR_MUTATE"))
        self.assertLess(permission_rank("STANDARD_OPERATOR_MUTATE"), permission_rank("GUARDED_OPERATOR_MUTATE"))
        self.assertLess(permission_rank("GUARDED_OPERATOR_MUTATE"), permission_rank("PRIVILEGED_OPERATOR_MUTATE"))
        self.assertTrue(permission_allows(granted="PRIVILEGED_OPERATOR_MUTATE", requested="READ_ONLY"))
        self.assertFalse(permission_allows(granted="READ_ONLY", requested="STANDARD_OPERATOR_MUTATE"))

    def test_action_envelope_validation(self) -> None:
        env = build_action_envelope(
            action_transport_type="COMMAND",
            action_transport_id="cmd-1",
            telegram_user_id=1001,
            chat_id=-2001,
            thread_id=None,
            action_type="TEST_ACTION",
            action_class="READ_ONLY",
            target_entity_type="release",
            target_entity_ref="rel-1",
            freshness_context={"v": 1},
            correlation_id="corr-1",
        )
        self.assertEqual(env["action_transport_type"], "COMMAND")
        with self.assertRaises(ValueError):
            build_action_envelope(
                action_transport_type="COMMAND",
                action_transport_id="cmd-1",
                telegram_user_id=1001,
                chat_id=-2001,
                thread_id=None,
                action_type="TEST_ACTION",
                action_class="READ_ONLY",
                target_entity_type=None,
                target_entity_ref="rel-1",
                freshness_context={},
                correlation_id="corr-1",
            )

    def test_expired_action_rejection_logic(self) -> None:
        expired = build_action_envelope(
            action_transport_type="CALLBACK",
            action_transport_id="cb-1",
            telegram_user_id=1001,
            chat_id=-2001,
            thread_id=None,
            action_type="TEST_ACTION",
            action_class="READ_ONLY",
            target_entity_type="release",
            target_entity_ref="rel-1",
            freshness_context={},
            correlation_id="corr-1",
            expires_at=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
        )
        self.assertTrue(is_envelope_expired(expired))

    def test_gateway_result_and_error_mapping(self) -> None:
        decision = build_gateway_decision(
            gateway_result="DENIED",
            resolved_operator_status="ACTIVE",
            resolved_binding_status="ACTIVE",
            permission_result="DENIED",
            action_class="GUARDED_OPERATOR_MUTATE",
            target_resolution_result="FOUND",
            stale_precheck_hook_ref="stale_hook",
            idempotency_hook_ref="idem_hook",
            error_code="E6A_PERMISSION_DENIED",
        )
        self.assertEqual(decision["gateway_result"], "DENIED")
        mapped = to_telegram_safe_error("E6A_PERMISSION_DENIED")
        self.assertEqual(mapped["code"], "E6A_PERMISSION_DENIED")


if __name__ == "__main__":
    unittest.main()
