from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env
from tests.analytics_ui_fixtures import seed_mf6_page_data


class TestMf6AnalyticsPagesIntegration(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_all_required_page_families_and_contract_fields(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            conn = dbm.connect(env)
            try:
                seeded = seed_mf6_page_data(conn)
            finally:
                conn.close()
            pages = {
                "/v1/analytics/overview": "OVERVIEW",
                f"/v1/analytics/channels/{seeded['channel_slug']}": "CHANNEL",
                f"/v1/analytics/releases/{seeded['release_id']}": "RELEASE",
                f"/v1/analytics/batches/{seeded['batch_month']}": "BATCH_MONTH",
                "/v1/analytics/anomalies": "ANOMALIES",
                "/v1/analytics/recommendations": "RECOMMENDATIONS",
                "/v1/analytics/reports": "REPORTS_EXPORTS",
            }
            for path, scope in pages.items():
                r = client.get(path, headers=h)
                self.assertEqual(r.status_code, 200)
                body = r.json()
                self.assertEqual(body["page_scope"], scope)
                for req in (
                    "applied_filters",
                    "freshness_summary",
                    "source_coverage_summary",
                    "summary_cards",
                    "detail_blocks",
                    "anomaly_risk_markers",
                    "recommendation_summary",
                    "available_actions",
                    "export_report_actions",
                    "navigation",
                    "filter_state_token",
                ):
                    self.assertIn(req, body)
                if scope in {"OVERVIEW", "CHANNEL", "RELEASE", "BATCH_MONTH", "ANOMALIES", "RECOMMENDATIONS"}:
                    self.assertGreaterEqual(len(body["detail_blocks"]), 1)

    def test_filter_restorable_behavior(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            conn = dbm.connect(env)
            try:
                seeded = seed_mf6_page_data(conn)
            finally:
                conn.close()
            r = client.get(f"/v1/analytics/channels/{seeded['channel_slug']}", params={"severity": "WARNING"}, headers=h)
            self.assertEqual(r.status_code, 200)
            applied = r.json()["applied_filters"]
            self.assertEqual(applied["channel"], "darkwood-reverie")
            self.assertEqual(applied["severity"], "WARNING")


if __name__ == "__main__":
    unittest.main()
