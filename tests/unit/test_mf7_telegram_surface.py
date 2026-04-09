from __future__ import annotations

import unittest

from services.analytics_center.telegram_surface import build_telegram_analyzer_surface


class TestMf7TelegramSurface(unittest.TestCase):
    def test_full_surface_contract(self) -> None:
        payload = build_telegram_analyzer_surface(
            channel_slug="darkwood-reverie",
            release_id="42",
            recommendation_items=[
                {"recommendation_family": "ANOMALY_RISK_ALERT", "severity_class": "CRITICAL", "title_text": "Risk", "summary_text": "Drop detected"},
            ],
            planning_summary={"scenario": "WEEK", "status": "READY"},
        )
        self.assertIn("summaries", payload)
        self.assertIn("alerts", payload)
        self.assertIn("channel_snapshots", payload)
        self.assertIn("release_video_snapshots", payload)
        self.assertIn("recommendation_summaries", payload)
        self.assertIn("planning_summaries", payload)
        self.assertIn("linked_actions", payload)
        self.assertIn("deep_links", payload)
        self.assertIn("operator_runtime", payload)
        self.assertEqual(payload["operator_runtime"]["surface_kind"], "TELEGRAM_ANALYZER_OPERATOR")
        self.assertTrue(payload["interface_role"]["operator_surface"])
        self.assertTrue(all(action.get("auto_apply") is False for action in payload["linked_actions"]))
        self.assertTrue(all(action.get("mutation") is False for action in payload["linked_actions"]))
        self.assertFalse(payload["default_behavior"]["auto_apply"])
        self.assertFalse(payload["interface_role"]["replaces_web_ui"])


if __name__ == "__main__":
    unittest.main()
