from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf7PlanningTelegramApi(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_planning_assistant_endpoint_contract(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                "/v1/analytics/planning-assistant",
                headers=h,
                json={
                    "scenario": "MONTH",
                    "channel_strategy_profile": "LONG_FORM_BACKGROUND_MUSIC",
                    "format_profile": "LONG_FORM",
                    "publish_windows": ["weekday_evening", "weekend_morning"],
                    "risk_signals": [{"signal": "drop"}],
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["scenario"], "MONTH")
            self.assertFalse(body["default_behavior"]["auto_apply"])
            self.assertIn("recommended_release_schedule", body["outputs"])
            self.assertEqual(body["outputs"]["planning_horizon_days"], 30)
            self.assertGreaterEqual(len(body["outputs"]["execution_checklist"]), 1)
            self.assertTrue(all(action.get("auto_apply") is False for action in body["outputs"]["linked_actions"]))
            self.assertTrue(all(action.get("mutation") is False for action in body["outputs"]["linked_actions"]))

    def test_planning_assistant_supports_week_month_quarter_behavior(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            expected = {"WEEK": 3, "MONTH": 8, "QUARTER": 12}
            for scenario, slot_count in expected.items():
                resp = client.post(
                    "/v1/analytics/planning-assistant",
                    headers=h,
                    json={
                        "scenario": scenario,
                        "channel_strategy_profile": "LONG_FORM_BACKGROUND_MUSIC",
                        "format_profile": "LONG_FORM",
                        "publish_windows": ["weekday_evening", "weekend_morning"],
                        "risk_signals": [{"signal": "cadence_drop"}],
                    },
                )
                self.assertEqual(resp.status_code, 200)
                body = resp.json()
                self.assertEqual(body["scenario"], scenario)
                self.assertEqual(len(body["outputs"]["recommended_release_schedule"]), slot_count)

    def test_telegram_surface_endpoint_contract(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                "/v1/analytics/telegram/surface",
                headers=h,
                json={
                    "channel_slug": "darkwood-reverie",
                    "release_id": 101,
                    "recommendation_items": [
                        {
                            "recommendation_family": "ANOMALY_RISK_ALERT",
                            "severity_class": "CRITICAL",
                            "title_text": "Risk",
                            "summary_text": "Drop detected",
                        }
                    ],
                    "planning_summary": {"scenario": "WEEK", "status": "READY"},
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertIn("summaries", body)
            self.assertIn("alerts", body)
            self.assertIn("channel_snapshots", body)
            self.assertIn("release_video_snapshots", body)
            self.assertIn("recommendation_summaries", body)
            self.assertIn("planning_summaries", body)
            self.assertIn("deep_links", body)
            self.assertIn("operator_runtime", body)
            self.assertEqual(body["operator_runtime"]["surface_kind"], "TELEGRAM_ANALYZER_OPERATOR")
            self.assertGreaterEqual(len(body["linked_actions"]), 3)
            self.assertTrue(all(action.get("auto_apply") is False for action in body["linked_actions"]))
            self.assertTrue(all(action.get("mutation") is False for action in body["linked_actions"]))
            self.assertIn("deep_link", body["alerts"][0])
            self.assertFalse(body["default_behavior"]["auto_apply"])


if __name__ == "__main__":
    unittest.main()
