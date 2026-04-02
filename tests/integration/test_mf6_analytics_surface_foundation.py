from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf6AnalyticsSurfaceFoundation(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_navigation_and_filter_contract_surface(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/analytics/filter-contract", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertIn("shared_filters", body)
            nav_keys = [item["key"] for item in body["navigation"]]
            self.assertEqual(
                nav_keys,
                ["OVERVIEW", "CHANNELS", "RELEASES_VIDEOS", "BATCH_MONTH", "ANOMALIES", "RECOMMENDATIONS", "REPORTS_EXPORTS"],
            )

    def test_page_skeletons_render_and_show_missing_or_partial_completeness(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            paths = [
                "/v1/analytics/overview",
                "/v1/analytics/channels/darkwood-reverie",
                "/v1/analytics/releases/1",
                "/v1/analytics/batches/2026-04",
                "/v1/analytics/anomalies",
                "/v1/analytics/recommendations",
                "/v1/analytics/reports",
            ]
            for path in paths:
                r = client.get(path, headers=headers)
                self.assertEqual(r.status_code, 200)
                body = r.json()
                self.assertIn("page_scope", body)
                self.assertIn("applied_filters", body)
                self.assertIn("freshness_summary", body)
                self.assertIn("source_coverage_summary", body)
                self.assertIn(body["freshness_summary"]["status"], {"MISSING", "PARTIAL", "STALE", "FRESH"})
                self.assertIn(body["source_coverage_summary"]["status"], {"NO_DATA", "PARTIAL", "FULL"})
                self.assertEqual(body["data_completeness"], "PARTIAL")


if __name__ == "__main__":
    unittest.main()
