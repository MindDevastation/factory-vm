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
                ["OVERVIEW", "CHANNELS", "RELEASES_VIDEOS", "BATCH_MONTH", "PORTFOLIO", "ANOMALIES", "RECOMMENDATIONS", "REPORTS_EXPORTS"],
            )
            for item in body["navigation"]:
                r = client.get(item["path"], headers=headers)
                self.assertEqual(r.status_code, 200)

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
                "/v1/analytics/portfolio",
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

    def test_ui_analyzer_surface_family_is_reachable(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            paths = [
                "/ui/analyzer",
                "/ui/analyzer/overview",
                "/ui/analyzer/channels",
                "/ui/analyzer/releases",
                "/ui/analyzer/batches",
                "/ui/analyzer/portfolio",
                "/ui/analyzer/anomalies",
                "/ui/analyzer/recommendations",
                "/ui/analyzer/reports",
            ]
            for path in paths:
                r = client.get(path, headers=headers)
                self.assertEqual(r.status_code, 200)
                self.assertIn("Analyzer · Surface Family", r.text)
                self.assertIn("data-analyzer-nav=", r.text)
                self.assertIn("id=\"analyzer-status\"", r.text)
                self.assertIn("id=\"analyzer-line-chart\"", r.text)
                self.assertIn("id=\"analyzer-bar-chart\"", r.text)
                self.assertIn("id=\"analyzer-refresh-panel\"", r.text)
                self.assertIn("id=\"manual-refresh-trigger\"", r.text)
                self.assertIn("id=\"scheduled-refresh-selector\"", r.text)
                self.assertIn("id=\"scheduled-refresh-trigger\"", r.text)
                self.assertIn("/v1/analytics/external/manual-refresh", r.text)
                self.assertIn("/v1/analytics/external/scheduled-refresh", r.text)
                self.assertIn('value="HOURLY"', r.text)
                self.assertIn('value="EVERY_12_HOURS"', r.text)
                self.assertIn('value="DAILY"', r.text)
                self.assertNotIn('value="EVERY_6_HOURS"', r.text)
                self.assertIn("animateLinePath", r.text)
                self.assertIn("requestAnimationFrame", r.text)

    def test_ui_analyzer_backfill_binds_to_selected_scope_context(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get(
                "/ui/analyzer/channels?target_scope_type=CHANNEL&target_scope_ref=darkwood-reverie",
                headers=headers,
            )
            self.assertEqual(r.status_code, 200)
            self.assertIn("/v1/analytics/channels/darkwood-reverie", r.text)
            self.assertIn("resolveBackfillContext", r.text)
            self.assertIn("setBackfillContext", r.text)
            self.assertIn("id=\"backfill-context\"", r.text)
            self.assertNotIn("target_scope_ref: 'darkwood-reverie'", r.text)


if __name__ == "__main__":
    unittest.main()
