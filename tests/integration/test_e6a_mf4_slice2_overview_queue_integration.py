from __future__ import annotations

import unittest

from services.telegram_inbox import build_factory_overview, build_readiness_overview


class TestE6AMf4Slice2OverviewQueueIntegration(unittest.TestCase):
    def test_factory_overview_and_attention_queue(self) -> None:
        rows = [
            {"job_id": 3, "publish_state": "ready_to_publish"},
            {"job_id": 1, "publish_state": "manual_handoff_pending"},
            {"job_id": 2, "publish_state": "policy_blocked"},
        ]
        view = build_factory_overview(rows=rows, generated_at="2026-04-02T00:00:00Z")
        self.assertIn("queue_groups", view)
        self.assertEqual(view["attention_needed"][0], 1)
        self.assertIn("freshness", view)

    def test_readiness_blockers_overview(self) -> None:
        blockers = [{"job_id": 11}, {"job_id": 12}]
        view = build_readiness_overview(blockers=blockers, generated_at="2026-04-02T00:00:00Z")
        self.assertEqual(view["blocked_items"], [11, 12])
        self.assertEqual(view["risk"], "high")


if __name__ == "__main__":
    unittest.main()
