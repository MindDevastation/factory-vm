from __future__ import annotations

import unittest

from services.telegram_inbox import build_deep_link, build_entity_drilldown, build_problem_list


class TestE6AMf4Slice3DrilldownDeeplinkIntegration(unittest.TestCase):
    def test_job_and_release_drilldown(self) -> None:
        job = build_entity_drilldown(
            entity_type="job",
            entity_id=44,
            state="manual_handoff_pending",
            reason="needs operator",
            next_action="ack_manual_handoff",
            generated_at="2026-04-02T00:00:00Z",
        )
        rel = build_entity_drilldown(
            entity_type="release",
            entity_id=9,
            state="blocked",
            reason="policy",
            next_action=None,
            generated_at="2026-04-02T00:00:00Z",
        )
        self.assertEqual(job["web_link"], "/jobs/44")
        self.assertEqual(rel["web_link"], "/releases/9")

    def test_problem_list_and_deep_link(self) -> None:
        rows = [{"job_id": 8, "publish_state": "policy_blocked"}, {"job_id": 7, "publish_state": "manual_handoff_pending"}]
        payload = build_problem_list(rows=rows, generated_at="2026-04-02T00:00:00Z")
        self.assertEqual(payload["items"][0]["job_id"], 7)
        self.assertEqual(build_deep_link(entity_type="job", entity_id=7), "/jobs/7")


if __name__ == "__main__":
    unittest.main()
