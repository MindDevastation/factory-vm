from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env
from tests.recommendation_fixtures import seed_recommendation_inputs
from services.analytics_center.recommendation_runtime import recompute_recommendations, read_recommendations


class TestMf6ReportsActionsIntegration(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_generate_list_download_and_dedupe_reports(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                recompute_recommendations(
                    conn,
                    recommendation_scope_type="CHANNEL",
                    recommendation_scope_ref="darkwood-reverie",
                    recommendation_family="WEAK_RELEASE_ATTENTION",
                    recompute_mode="FULL_RECOMPUTE",
                )
                self.assertGreaterEqual(len(read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", current_only=True)), 1)
            finally:
                conn.close()
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = {
                "report_scope_type": "CHANNEL",
                "report_scope_ref": "darkwood-reverie",
                "report_family": "ANALYTICS_SUMMARY",
                "filter_payload": {"channel": "darkwood-reverie"},
                "artifact_type": "XLSX",
            }
            a = client.post("/v1/analytics/reports/request", headers=h, json=payload)
            self.assertEqual(a.status_code, 200)
            first_id = a.json()["deduped_or_created_id"]
            b = client.post("/v1/analytics/reports/request", headers=h, json=payload)
            self.assertEqual(b.status_code, 200)
            self.assertEqual(first_id, b.json()["deduped_or_created_id"])
            created = a.json()["report_record"]
            self.assertEqual(created["generation_status"], "READY")
            self.assertTrue(str(created["artifact_ref"]).endswith(".xlsx"))
            listed = client.get("/v1/analytics/reports/records", headers=h)
            self.assertEqual(listed.status_code, 200)
            self.assertGreaterEqual(len(listed.json()["items"]), 1)
            dl = client.get(f"/v1/analytics/reports/{first_id}/download", headers=h)
            self.assertEqual(dl.status_code, 200)
            self.assertTrue(dl.json()["download"])
            self.assertTrue(str(dl.json()["artifact_ref"]).endswith(".xlsx"))
            structured_payload = dict(payload)
            structured_payload["artifact_type"] = "STRUCTURED_REPORT"
            structured = client.post("/v1/analytics/reports/request", headers=h, json=structured_payload)
            self.assertEqual(structured.status_code, 200)
            self.assertTrue(str(structured.json()["report_record"]["artifact_ref"]).endswith("_structured.json"))
            api_payload = dict(payload)
            api_payload["artifact_type"] = "API_REPORT"
            api_generated = client.post("/v1/analytics/reports/request", headers=h, json=api_payload)
            self.assertEqual(api_generated.status_code, 200)
            self.assertTrue(str(api_generated.json()["report_record"]["artifact_ref"]).endswith("_api_payload.json"))
            bad = dict(payload)
            bad["artifact_type"] = "BAD_ARTIFACT"
            bad_resp = client.post("/v1/analytics/reports/request", headers=h, json=bad)
            self.assertEqual(bad_resp.status_code, 422)
            self.assertIn("report generation failed", bad_resp.text)

    def test_report_request_fails_when_required_source_data_missing(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = {
                "report_scope_type": "CHANNEL",
                "report_scope_ref": "darkwood-reverie",
                "report_family": "ANALYTICS_SUMMARY",
                "filter_payload": {"channel": "darkwood-reverie"},
                "artifact_type": "XLSX",
            }
            resp = client.post("/v1/analytics/reports/request", headers=h, json=payload)
            self.assertEqual(resp.status_code, 422)
            self.assertIn("missing required source data", resp.text)

    def test_actions_delegate_without_adjacent_domain_mutation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="FULL_RECOMPUTE")
                rec = read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", current_only=True)[0]
                rec_id = int(rec["id"])
            finally:
                conn.close()

            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            rf = client.post("/v1/analytics/actions/refresh", headers=h, json={"scope": "CHANNEL"})
            rc = client.post("/v1/analytics/actions/recompute", headers=h, json={"scope": "CHANNEL"})
            ins = client.post("/v1/analytics/actions/anomaly/inspect", headers=h, json={"id": "anomaly-1"})
            j = client.get("/v1/analytics/actions/related-domain-jump", headers=h, params={"target_domain": "PUBLISH", "scope_ref": "darkwood-reverie", "next_action": "open"})
            ack = client.post(f"/v1/analytics/actions/recommendations/{rec_id}/acknowledge", headers=h)

            self.assertEqual(rf.status_code, 200)
            self.assertEqual(rc.status_code, 200)
            self.assertEqual(ins.status_code, 200)
            self.assertEqual(j.status_code, 200)
            self.assertEqual(ack.status_code, 200)
            self.assertFalse(rf.json()["mutation"])
            self.assertFalse(rc.json()["mutation"])
            self.assertFalse(j.json()["mutation"])


if __name__ == "__main__":
    unittest.main()
