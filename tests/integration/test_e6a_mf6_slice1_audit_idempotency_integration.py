from __future__ import annotations

import unittest

from services.telegram_operator import build_audit_correlation, build_idempotency_fingerprint


class TestE6AMf6Slice1AuditIdempotencyIntegration(unittest.TestCase):
    def test_repeated_interaction_has_stable_fingerprint(self) -> None:
        fp1 = build_idempotency_fingerprint(action_type="ACK", target_entity_type="inbox_message", target_entity_ref="10", request_id="req")
        fp2 = build_idempotency_fingerprint(action_type="ACK", target_entity_type="inbox_message", target_entity_ref="10", request_id="req")
        corr = build_audit_correlation(telegram_user_id=9, chat_id=-9, correlation_id="corr", action_type="ACK", result="ALLOWED")
        self.assertEqual(fp1, fp2)
        self.assertEqual(corr["action_type"], "ACK")


if __name__ == "__main__":
    unittest.main()
