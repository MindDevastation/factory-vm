from __future__ import annotations

import unittest

from services.telegram_publish import build_publish_confirmation_payload, compare_publish_staleness, map_publish_action_policy


class TestE6AMf3Slice2PublishRoutingUnit(unittest.TestCase):
    def test_publish_action_policy_mapping(self) -> None:
        p = map_publish_action_policy(telegram_action="approve")
        self.assertEqual(p["canonical_action_type"], "unblock")
        self.assertTrue(bool(p["confirm_required"]))

    def test_stale_publish_context_comparator(self) -> None:
        stale = compare_publish_staleness(expected_publish_state="ready_to_publish", current_publish_state="manual_handoff_pending")
        self.assertEqual(stale["result"], "STALE")
        current = compare_publish_staleness(expected_publish_state="ready_to_publish", current_publish_state="ready_to_publish")
        self.assertEqual(current["result"], "CURRENT")

    def test_confirmation_payload_validation(self) -> None:
        payload = build_publish_confirmation_payload(
            telegram_action="reject",
            confirm=True,
            reason="policy reject",
            request_id="req-m3-s2",
        )
        self.assertEqual(payload["request_id"], "req-m3-s2")
        with self.assertRaises(ValueError):
            build_publish_confirmation_payload(telegram_action="reject", confirm=False, reason="x", request_id="req")


if __name__ == "__main__":
    unittest.main()
