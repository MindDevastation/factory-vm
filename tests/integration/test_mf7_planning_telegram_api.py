from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.analytics_center.mf4_runtime import recompute_mf4
from services.analytics_center.recommendation_core import persist_recommendation_snapshot, synthesize_recommendations
from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env
from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot


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

    def test_planning_assistant_endpoint_uses_analyzer_grounding_over_request(self) -> None:
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
            finally:
                conn.close()

            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                "/v1/analytics/planning-assistant",
                headers=h,
                json={
                    "scenario": "MONTH",
                    "scope_type": "CHANNEL",
                    "scope_ref": "darkwood-reverie",
                    "publish_windows": ["request_only_window"],
                    "risk_signals": [{"source": "REQUEST_ONLY"}],
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["inputs"]["historical_performance"]["source"], "ANALYZER_PERSISTED_HISTORY")
            self.assertNotEqual(body["inputs"]["publish_windows"], ["request_only_window"])
            self.assertTrue(any(str(item.get("source")) != "REQUEST_ONLY" for item in body["inputs"]["risk_signals"]))

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

    def test_telegram_dispatch_operator_workflow_dry_run_and_live(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            dry = client.post(
                "/v1/analytics/telegram/dispatch",
                headers=h,
                json={
                    "channel_slug": "darkwood-reverie",
                    "release_id": 101,
                    "dry_run": True,
                    "recommendation_items": [{"recommendation_family": "ANOMALY_RISK_ALERT", "severity_class": "CRITICAL"}],
                    "planning_summary": {"scenario": "WEEK", "status": "READY"},
                },
            )
            self.assertEqual(dry.status_code, 200)
            dry_body = dry.json()
            self.assertEqual(dry_body["delivery"]["delivery_mode"], "DRY_RUN")
            self.assertFalse(dry_body["delivery"]["delivered"])
            self.assertIn("message_preview", dry_body["delivery"])
            self.assertFalse(dry_body["default_behavior"]["auto_apply"])

            with patch("services.analytics_center.telegram_delivery._telegram_send_message_http", return_value={"ok": True, "result": {"message_id": 1}}):
                live = client.post(
                    "/v1/analytics/telegram/dispatch",
                    headers=h,
                    json={
                        "channel_slug": "darkwood-reverie",
                        "release_id": 101,
                        "dry_run": False,
                        "bot_token": "unit-test-token",
                        "chat_id": 999123,
                        "recommendation_items": [{"recommendation_family": "ANOMALY_RISK_ALERT", "severity_class": "CRITICAL"}],
                        "planning_summary": {"scenario": "WEEK", "status": "READY"},
                    },
                )
            self.assertEqual(live.status_code, 200)
            live_body = live.json()
            self.assertEqual(live_body["delivery"]["delivery_mode"], "LIVE")
            self.assertTrue(live_body["delivery"]["delivered"])


if __name__ == "__main__":
    unittest.main()
