from __future__ import annotations

import unittest

from services.telegram_operator import build_idempotency_fingerprint, classify_stale_conflict, render_operator_safe_result


class TestE6AMf6Slice4FinalHardening(unittest.TestCase):
    def test_fail_closed_unknown_or_stale_paths(self) -> None:
        self.assertEqual(classify_stale_conflict(expected_state=None, current_state="x"), "UNKNOWN")
        safe = render_operator_safe_result(code="E6A_UNSAFE_CURRENT_STATE", message="action not safe")
        self.assertEqual(safe["code"], "E6A_UNSAFE_CURRENT_STATE")

    def test_no_silent_duplicate_behavior(self) -> None:
        a = build_idempotency_fingerprint(action_type="ACK", target_entity_type="msg", target_entity_ref="1", request_id="dup")
        b = build_idempotency_fingerprint(action_type="ACK", target_entity_type="msg", target_entity_ref="1", request_id="dup")
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
