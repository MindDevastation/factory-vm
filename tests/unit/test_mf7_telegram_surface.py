from __future__ import annotations

import unittest

from services.analytics_center.mf4_runtime import recompute_mf4
from services.analytics_center.recommendation_core import persist_recommendation_snapshot, synthesize_recommendations
from services.analytics_center.telegram_surface import build_telegram_analyzer_surface
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot


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

    def test_uses_grounded_analyzer_state_over_request_payload(self) -> None:
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
                for rec in synthesize_recommendations(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")[:2]:
                    persist_recommendation_snapshot(conn, recommendation=rec)

                payload = build_telegram_analyzer_surface(
                    conn=conn,
                    scope_type="CHANNEL",
                    scope_ref="darkwood-reverie",
                    recommendation_items=[
                        {"recommendation_family": "REQUEST_ONLY", "severity_class": "INFO", "summary_text": "request"},
                    ],
                    planning_summary={"scenario": "MONTH", "status": "REQUEST_ONLY"},
                )
            finally:
                conn.close()
            self.assertEqual(payload["planning_summaries"][0]["source"], "ANALYZER_PERSISTED_STATE")
            self.assertEqual(payload["summaries"]["history_summary"]["source"], "ANALYZER_PERSISTED_HISTORY")
            families = [str(item.get("recommendation_family") or "") for item in payload["recommendation_summaries"]]
            self.assertNotIn("REQUEST_ONLY", families)


if __name__ == "__main__":
    unittest.main()
