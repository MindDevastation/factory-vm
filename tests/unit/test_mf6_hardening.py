from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.analytics_center.recommendation_runtime import recompute_recommendations
from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env
from tests.recommendation_fixtures import seed_recommendation_inputs


class TestMf6Hardening(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_mf6_required_ui_events_emitted(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                seed_recommendation_inputs(conn, scope_type="PORTFOLIO", scope_ref="portfolio-global")
                recompute_recommendations(
                    conn,
                    recommendation_scope_type="CHANNEL",
                    recommendation_scope_ref="darkwood-reverie",
                    recommendation_family="WEAK_RELEASE_ATTENTION",
                    recompute_mode="FULL_RECOMPUTE",
                )
            finally:
                conn.close()
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            client.get("/v1/analytics/overview", headers=h)
            good = {
                "report_scope_type": "OVERVIEW",
                "report_scope_ref": None,
                "report_family": "SUMMARY",
                "filter_payload": {"time_window": "30d"},
                "artifact_type": "XLSX",
            }
            client.post("/v1/analytics/reports/request", headers=h, json=good)
            bad = dict(good)
            bad["artifact_type"] = "BAD"
            client.post("/v1/analytics/reports/request", headers=h, json=bad)
            rec = client.get("/v1/analytics/reports/records", headers=h).json()["items"][0]
            client.get(f"/v1/analytics/reports/{rec['id']}/download", headers=h)
            client.post("/v1/analytics/actions/refresh", headers=h, json={"scope": "OVERVIEW"})
            client.post("/v1/analytics/actions/recompute", headers=h, json={"scope": "OVERVIEW"})
            client.get("/v1/analytics/recommendations", headers=h)
            client.post("/v1/analytics/actions/anomaly/inspect", headers=h, json={"id": "a1"})
            client.get("/v1/analytics/actions/related-domain-jump", headers=h, params={"target_domain": "PUBLISH", "scope_ref": "x", "next_action": "open"})

            conn = dbm.connect(env)
            try:
                events = {str(r["event_type"]) for r in conn.execute("SELECT event_type FROM analytics_ui_events").fetchall()}
                freshness_payloads = [
                    str(r["freshness_summary_json"])
                    for r in conn.execute("SELECT freshness_summary_json FROM analytics_ui_events WHERE event_type IN ('ANALYTICS_REPORT_REQUESTED', 'ANALYTICS_REPORT_GENERATED')").fetchall()
                ]
            finally:
                conn.close()
            required = {
                "ANALYTICS_PAGE_VIEWED",
                "ANALYTICS_REPORT_REQUESTED",
                "ANALYTICS_REPORT_GENERATED",
                "ANALYTICS_REPORT_FAILED",
                "EXPORT_DOWNLOADED",
                "REFRESH_TRIGGERED",
                "RECOMPUTE_TRIGGERED",
                "RECOMMENDATION_OPENED",
                "ANOMALY_INSPECTED",
                "RELATED_DOMAIN_JUMP_OPENED",
            }
            self.assertTrue(required.issubset(events))
            self.assertTrue(any('"status": "FRESH"' in payload for payload in freshness_payloads))


if __name__ == "__main__":
    unittest.main()
