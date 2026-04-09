from __future__ import annotations

import unittest

from services.analytics_center.planning_assistant import build_planning_assistant_summary


class TestMf7PlanningAssistant(unittest.TestCase):
    def test_week_month_quarter_scenarios_supported(self) -> None:
        expected_slots = {"WEEK": 3, "MONTH": 8, "QUARTER": 12}
        for scenario in ("WEEK", "MONTH", "QUARTER"):
            payload = build_planning_assistant_summary(
                scenario=scenario,
                channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
                format_profile="LONG_FORM",
                publish_windows=["weekday_evening"],
                risk_signals=[{"risk": "drop"}],
            )
            self.assertEqual(payload["scenario"], scenario)
            self.assertFalse(payload["default_behavior"]["auto_apply"])
            self.assertFalse(payload["default_behavior"]["mutation"])
            self.assertIn("recommended_release_schedule", payload["outputs"])
            self.assertEqual(len(payload["outputs"]["recommended_release_schedule"]), expected_slots[scenario])
            self.assertIn("planning_horizon_days", payload["outputs"])
            self.assertIn("execution_checklist", payload["outputs"])
            self.assertTrue(all(a["auto_apply"] is False for a in payload["outputs"]["linked_actions"]))
            self.assertTrue(all(a["mutation"] is False for a in payload["outputs"]["linked_actions"]))

    def test_invalid_scenario_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_planning_assistant_summary(
                scenario="YEAR",
                channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
                format_profile="LONG_FORM",
            )


if __name__ == "__main__":
    unittest.main()
