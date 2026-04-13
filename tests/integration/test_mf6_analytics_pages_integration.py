from __future__ import annotations

import importlib
import unittest
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from services.analytics_center.mf4_runtime import recompute_mf4
from services.analytics_center.recommendation_core import persist_recommendation_snapshot, synthesize_recommendations
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
                "/v1/analytics/portfolio": "PORTFOLIO",
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
                    "period_semantics",
                    "semantic_filter_contract",
                    "chart_blocks",
                ):
                    self.assertIn(req, body)
                self.assertIn(body["freshness_summary"]["status"], {"FRESH", "PARTIAL", "STALE", "MISSING"})
                self.assertIn(body["source_coverage_summary"]["status"], {"FULL", "PARTIAL", "NO_DATA"})
                if scope in {"OVERVIEW", "CHANNEL", "RELEASE", "BATCH_MONTH", "PORTFOLIO", "ANOMALIES", "RECOMMENDATIONS"}:
                    self.assertGreaterEqual(len(body["detail_blocks"]), 1)
                self.assertTrue(all(bool(c.get("animated")) for c in body["chart_blocks"]))
                self.assertIn("current_period", body["period_semantics"])
                self.assertIn("baseline_comparison", body["period_semantics"])

    def test_stale_persisted_source_data_is_reported_as_stale(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            conn = dbm.connect(env)
            try:
                seeded = seed_mf6_page_data(conn)
                stale_ts = dbm.now_ts() - (10 * 86400.0)
                conn.execute("UPDATE analytics_snapshots SET captured_at = ?, freshness_status = 'STALE' WHERE is_current = 1", (stale_ts,))
                conn.execute("UPDATE analytics_operational_kpi_snapshots SET created_at = ? WHERE is_current = 1", (stale_ts,))
                conn.execute("UPDATE analytics_comparison_snapshots SET created_at = ? WHERE is_current = 1", (stale_ts,))
                conn.execute("UPDATE analytics_prediction_snapshots SET created_at = ? WHERE is_current = 1", (stale_ts,))
                conn.execute("UPDATE analytics_recommendation_snapshots SET created_at = ? WHERE is_current = 1", (stale_ts,))
            finally:
                conn.close()
            r = client.get(f"/v1/analytics/channels/{seeded['channel_slug']}", headers=h)
            self.assertEqual(r.status_code, 200)
            body = r.json()
            self.assertEqual(body["freshness_summary"]["status"], "STALE")
            self.assertGreaterEqual(len(body["source_coverage_summary"]["stale_sources"]), 1)

    def test_overview_freshness_stale_filter_uses_created_at_not_variance_class(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            conn = dbm.connect(env)
            try:
                seed_mf6_page_data(conn)
                stale_ts = dbm.now_ts() - (10 * 86400.0)
                conn.execute("UPDATE analytics_comparison_snapshots SET created_at = ? WHERE is_current = 1", (stale_ts,))
                conn.execute("UPDATE analytics_recommendation_snapshots SET created_at = ? WHERE is_current = 1", (stale_ts,))
            finally:
                conn.close()

            r = client.get("/v1/analytics/overview", params={"freshness": "STALE"}, headers=h)
            self.assertEqual(r.status_code, 200)
            body = r.json()
            self.assertGreater(len(body["anomaly_risk_markers"]), 0)
            self.assertTrue(
                all(str(row["variance_class"]).upper() in {"ANOMALY", "RISK"} for row in body["anomaly_risk_markers"])
            )
            self.assertGreater(len(body["recommendation_summary"]), 0)

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
            base = client.get(f"/v1/analytics/channels/{seeded['channel_slug']}", headers=h)
            filtered = client.get(
                f"/v1/analytics/channels/{seeded['channel_slug']}",
                params={"severity": "WARNING", "recommendation_family": "WEAK_RELEASE_ATTENTION"},
                headers=h,
            )
            self.assertEqual(base.status_code, 200)
            self.assertEqual(filtered.status_code, 200)
            applied = filtered.json()["applied_filters"]
            self.assertEqual(applied["channel"], "darkwood-reverie")
            self.assertEqual(applied["severity"], "WARNING")
            self.assertEqual(applied["recommendation_family"], "WEAK_RELEASE_ATTENTION")
            self.assertLessEqual(len(filtered.json()["recommendation_summary"]), len(base.json()["recommendation_summary"]))

    def test_release_source_family_unsupported_returns_422(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            conn = dbm.connect(env)
            try:
                seeded = seed_mf6_page_data(conn)
            finally:
                conn.close()
            r = client.get(
                f"/v1/analytics/releases/{seeded['release_id']}",
                params={"source_family": "EXTERNAL_YOUTUBE"},
                headers=h,
            )
            self.assertEqual(r.status_code, 422)
            body = r.json()
            self.assertEqual(body["error"]["code"], "E5A_INVALID_ANALYTICS_FILTER_COMBINATION")
            self.assertIn("source_family", body["error"]["message"])

    def test_date_range_and_period_compare_affect_real_data_loading_for_required_surfaces(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            conn = dbm.connect(env)
            try:
                seeded = seed_mf6_page_data(conn)
                scope_ref = "GLOBAL_PORTFOLIO"
                from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot

                seed_mf4_mixed_input_snapshots(conn, scope_type="PORTFOLIO", scope_ref=scope_ref)
                seed_mf4_operational_kpi_snapshot(conn, scope_type="PORTFOLIO", scope_ref=scope_ref)
                recompute_mf4(conn, run_kind="FULL_STACK_RECOMPUTE", target_scope_type="PORTFOLIO", target_scope_ref=scope_ref, recompute_mode="FULL_RECOMPUTE")
                for rec in synthesize_recommendations(conn, scope_type="PORTFOLIO", scope_ref=scope_ref)[:2]:
                    persist_recommendation_snapshot(conn, recommendation=rec)

                now = dbm.now_ts()
                old = now - (2 * 86400.0)
                conn.execute("UPDATE analytics_comparison_snapshots SET created_at = ? WHERE is_current = 1", (old,))
                conn.execute("UPDATE analytics_recommendation_snapshots SET created_at = ? WHERE is_current = 1", (old,))
                conn.execute("UPDATE analytics_prediction_snapshots SET created_at = ? WHERE is_current = 1", (old,))
                conn.execute("UPDATE analytics_operational_kpi_snapshots SET created_at = ? WHERE is_current = 1", (old,))
            finally:
                conn.close()

            current_start = datetime.fromtimestamp(now, tz=timezone.utc).date()
            current_end = current_start + timedelta(days=1)
            date_from = current_start.isoformat()
            date_to = current_end.isoformat()
            current_params = {"date_from": date_from, "date_to": date_to}
            previous_params = {"date_from": date_from, "date_to": date_to, "period_compare": "PREVIOUS_PERIOD"}

            overview_current = client.get("/v1/analytics/overview", params=current_params, headers=h).json()
            overview_previous = client.get("/v1/analytics/overview", params=previous_params, headers=h).json()
            self.assertEqual(overview_current["summary_cards"][1]["value"], 0)
            self.assertGreaterEqual(overview_previous["summary_cards"][1]["value"], 1)

            channel_current = client.get(f"/v1/analytics/channels/{seeded['channel_slug']}", params=current_params, headers=h).json()
            channel_previous = client.get(
                f"/v1/analytics/channels/{seeded['channel_slug']}",
                params=previous_params,
                headers=h,
            ).json()
            self.assertEqual(channel_current["summary_cards"][0]["value"], 0)
            self.assertGreaterEqual(channel_previous["summary_cards"][0]["value"], 1)

            release_current = client.get(f"/v1/analytics/releases/{seeded['release_id']}", params=current_params, headers=h).json()
            release_previous = client.get(
                f"/v1/analytics/releases/{seeded['release_id']}",
                params=previous_params,
                headers=h,
            ).json()
            self.assertEqual(release_current["summary_cards"][0]["value"], 0)
            self.assertGreaterEqual(release_previous["summary_cards"][0]["value"], 1)

            batch_current = client.get(f"/v1/analytics/batches/{seeded['batch_month']}", params=current_params, headers=h).json()
            batch_previous = client.get(
                f"/v1/analytics/batches/{seeded['batch_month']}",
                params=previous_params,
                headers=h,
            ).json()
            self.assertEqual(batch_current["summary_cards"][0]["value"], 0)
            self.assertGreaterEqual(batch_previous["summary_cards"][0]["value"], 1)

            portfolio_current = client.get("/v1/analytics/portfolio", params={"portfolio_project": scope_ref, **current_params}, headers=h).json()
            portfolio_previous = client.get(
                "/v1/analytics/portfolio",
                params={"portfolio_project": scope_ref, **previous_params},
                headers=h,
            ).json()
            self.assertEqual(portfolio_current["summary_cards"][0]["value"], 0)
            self.assertGreaterEqual(portfolio_previous["summary_cards"][0]["value"], 1)


if __name__ == "__main__":
    unittest.main()
