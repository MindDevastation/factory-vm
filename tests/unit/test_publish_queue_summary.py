from __future__ import annotations

import unittest

from services.publish_runtime.queue_summary import assemble_publish_queue_summary


class TestPublishQueueSummary(unittest.TestCase):
    def test_assemble_summary_is_deterministic(self) -> None:
        rows = [
            {"publish_state": "ready_to_publish", "publish_hold_active": 0},
            {"publish_state": "policy_blocked", "publish_hold_active": 1},
            {"publish_state": "publish_failed_terminal", "publish_hold_active": 0},
            {"publish_state": "manual_handoff_pending", "publish_hold_active": 0},
            {"publish_state": "retry_pending", "publish_hold_active": 0},
            {"publish_state": "publish_state_drift_detected", "publish_hold_active": 0},
        ]

        summary = assemble_publish_queue_summary(rows)
        self.assertEqual(summary["total"], 6)
        self.assertEqual(summary["views"]["queue"], 2)
        self.assertEqual(summary["views"]["blocked"], 1)
        self.assertEqual(summary["views"]["failed"], 1)
        self.assertEqual(summary["views"]["manual"], 1)
        self.assertEqual(summary["views"]["health"], 6)
        self.assertEqual(summary["signals"]["drift_detected"], 1)
        self.assertEqual(summary["signals"]["hold_active"], 1)
        self.assertEqual(summary["signals"]["retry_pending"], 1)


if __name__ == "__main__":
    unittest.main()
