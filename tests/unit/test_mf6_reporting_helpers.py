from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.reporting import build_related_domain_jump, create_report_record, validate_report_request
from services.analytics_center.recommendation_runtime import recompute_recommendations
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.recommendation_fixtures import seed_recommendation_inputs


class TestMf6ReportingHelpers(unittest.TestCase):
    def test_report_scope_and_artifact_validation(self) -> None:
        validate_report_request(report_scope_type="OVERVIEW", artifact_type="XLSX")
        with self.assertRaises(AnalyticsDomainError):
            validate_report_request(report_scope_type="BAD", artifact_type="XLSX")
        with self.assertRaises(AnalyticsDomainError):
            validate_report_request(report_scope_type="OVERVIEW", artifact_type="BAD")

    def test_related_domain_jump_builder(self) -> None:
        jump = build_related_domain_jump(target_domain="PUBLISH", scope_ref="darkwood", next_action="open")
        self.assertIn("path", jump)
        with self.assertRaises(AnalyticsDomainError):
            build_related_domain_jump(target_domain="UNKNOWN", scope_ref="x", next_action="open")

    def test_create_report_record_generates_artifacts_by_type(self) -> None:
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
                for artifact_type, suffix in (
                    ("XLSX", ".xlsx"),
                    ("STRUCTURED_REPORT", "_structured.json"),
                    ("API_REPORT", "_api_payload.json"),
                ):
                    report_id = create_report_record(
                        conn,
                        report_scope_type="CHANNEL",
                        report_scope_ref="darkwood-reverie",
                        report_family="ANALYTICS_SUMMARY",
                        filter_payload={"channel": "darkwood-reverie"},
                        artifact_type=artifact_type,
                        created_by="test",
                    )
                    row = conn.execute("SELECT generation_status, artifact_ref FROM analytics_report_records WHERE id = ?", (int(report_id),)).fetchone()
                    self.assertEqual(str(row["generation_status"]), "READY")
                    self.assertTrue(str(row["artifact_ref"]).endswith(suffix))
            finally:
                conn.close()

    def test_create_report_record_fails_when_source_tables_are_missing(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                before = int(conn.execute("SELECT COUNT(*) AS c FROM analytics_report_records").fetchone()["c"])
                with self.assertRaises(AnalyticsDomainError):
                    create_report_record(
                        conn,
                        report_scope_type="CHANNEL",
                        report_scope_ref="darkwood-reverie",
                        report_family="ANALYTICS_SUMMARY",
                        filter_payload={"channel": "darkwood-reverie"},
                        artifact_type="XLSX",
                        created_by="test",
                    )
                after = int(conn.execute("SELECT COUNT(*) AS c FROM analytics_report_records").fetchone()["c"])
                self.assertEqual(after, before + 1)
                latest = conn.execute("SELECT generation_status, artifact_ref FROM analytics_report_records ORDER BY id DESC LIMIT 1").fetchone()
                self.assertEqual(str(latest["generation_status"]), "FAILED")
                self.assertIsNone(latest["artifact_ref"])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
