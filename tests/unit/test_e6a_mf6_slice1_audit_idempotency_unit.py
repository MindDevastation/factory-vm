from __future__ import annotations

import unittest

from services.telegram_operator import build_audit_correlation, build_idempotency_fingerprint


class TestE6AMf6Slice1AuditIdempotencyUnit(unittest.TestCase):
    def test_idempotency_fingerprint_is_deterministic(self) -> None:
        a = build_idempotency_fingerprint(action_type="PUBLISH_REJECT", target_entity_type="publish_job", target_entity_ref="1", request_id="r1")
        b = build_idempotency_fingerprint(action_type="PUBLISH_REJECT", target_entity_type="publish_job", target_entity_ref="1", request_id="r1")
        self.assertEqual(a, b)

    def test_audit_correlation_contract(self) -> None:
        payload = build_audit_correlation(telegram_user_id=1, chat_id=-1, correlation_id="c1", action_type="X", result="ALLOWED")
        self.assertEqual(payload["correlation_id"], "c1")


if __name__ == "__main__":
    unittest.main()
