from __future__ import annotations

import unittest

from services.telegram_publish import build_manual_handoff_fixture, build_publish_context_summary


class TestE6AMf3Slice1PublishContextUnit(unittest.TestCase):
    def test_publish_context_summary_contract_shape(self) -> None:
        summary = build_publish_context_summary(
            row={
                "job_id": 1,
                "release_id": 11,
                "publish_state": "ready_to_publish",
                "publish_reason_code": None,
                "publish_reason_detail": None,
                "channel_slug": "darkwood-reverie",
                "release_title": "Demo",
            }
        )
        self.assertEqual(summary["target"]["job_id"], 1)
        self.assertIn("available_next_actions", summary)
        self.assertTrue(bool(summary["web_link"]))
        self.assertTrue(bool(summary["compact"]))

    def test_allowed_next_actions_surface(self) -> None:
        handoff = build_publish_context_summary(
            row={
                "job_id": 2,
                "release_id": 12,
                "publish_state": "manual_handoff_pending",
                "publish_reason_code": "policy_requires_manual",
                "publish_reason_detail": "manual needed",
                "channel_slug": "darkwood-reverie",
                "release_title": "Demo",
            }
        )
        self.assertIn("ack_manual_handoff", handoff["available_next_actions"])
        drift = build_publish_context_summary(
            row={
                "job_id": 3,
                "release_id": 13,
                "publish_state": "publish_state_drift_detected",
                "publish_reason_code": "drift",
                "publish_reason_detail": "changed externally",
                "channel_slug": "darkwood-reverie",
                "release_title": "Demo",
            }
        )
        self.assertEqual(drift["available_next_actions"], [])

    def test_manual_handoff_fixture_factory(self) -> None:
        fx = build_manual_handoff_fixture(job_id=7, release_id=8)
        self.assertEqual(fx["publish_state"], "manual_handoff_pending")
        self.assertIn("ack_manual_handoff", fx["allowed_next_actions"])


if __name__ == "__main__":
    unittest.main()
