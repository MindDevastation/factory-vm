from __future__ import annotations

import unittest

from services.analytics_center.planning_assistant import build_planning_assistant_summary
from services.analytics_center.mf4_runtime import recompute_mf4
from services.analytics_center.recommendation_core import persist_recommendation_snapshot, synthesize_recommendations
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot


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

    def test_grounded_inputs_override_conflicting_request_shaping(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_mf4_mixed_input_snapshots(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                seed_mf4_operational_kpi_snapshot(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                recompute_mf4(
                    conn,
                    run_kind="FULL_STACK_RECOMPUTE",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
                recs = synthesize_recommendations(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                for rec in recs[:2]:
                    persist_recommendation_snapshot(conn, recommendation=rec)

                payload = build_planning_assistant_summary(
                    scenario="WEEK",
                    channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
                    format_profile="LONG_FORM",
                    conn=conn,
                    scope_type="CHANNEL",
                    scope_ref="darkwood-reverie",
                    historical_performance={"source": "REQUEST", "history_points": 0},
                    audience_behavior={"source": "REQUEST_ONLY"},
                    publish_windows=["request_only_window"],
                    cadence_patterns={"source": "REQUEST_ONLY"},
                    risk_signals=[{"source": "REQUEST_ONLY"}],
                )
            finally:
                conn.close()

            self.assertEqual(payload["inputs"]["historical_performance"]["source"], "ANALYZER_PERSISTED_HISTORY")
            self.assertNotEqual(payload["inputs"]["publish_windows"], ["request_only_window"])
            self.assertTrue(any(str(item.get("source")) == "RECOMMENDATION" for item in payload["inputs"]["risk_signals"]))
            self.assertEqual(payload["inputs"]["audience_behavior"]["source"], "ANALYZER_PERSISTED_STATE")


if __name__ == "__main__":
    unittest.main()
