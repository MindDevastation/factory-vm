from __future__ import annotations

import logging
import unittest

from services.analytics_center.external_sync import (
    create_sync_run,
    get_coverage_report,
    get_sync_run_detail,
    get_sync_status,
    list_sync_runs,
    request_manual_refresh,
    run_external_youtube_ingestion,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.analytics_fixtures import make_sync_run_payload


class _Provider:
    def __init__(self, payload: dict):
        self._payload = payload

    def fetch_channel_metrics(self, **_: object) -> dict:
        return dict(self._payload)

    def fetch_video_metrics(self, **_: object) -> dict:
        return dict(self._payload)


class TestAnalyticsExternalInterfacesHardening(unittest.TestCase):
    def test_manual_refresh_interface_and_status_coverage_reads(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                run_id = request_manual_refresh(
                    conn,
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    refresh_mode="MANUAL_REFRESH",
                    metrics_subset=["views", "impressions", "ctr", "monetization"],
                    force=True,
                )
                provider = _Provider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {"views": 10},
                        "metric_families_returned": ["views"],
                        "metric_families_unavailable": ["monetization"],
                        "freshness_status": "PARTIAL",
                        "freshness_basis": "window_end",
                        "incomplete_backfill": True,
                        "permission_limited": True,
                    }
                )
                run_external_youtube_ingestion(
                    conn,
                    run_id=run_id,
                    provider=provider,
                    channel_slug="darkwood-reverie",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                )
                status = get_sync_status(conn, target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                self.assertEqual(status["sync_state"], "PARTIAL")
                self.assertTrue(status["incomplete_backfill"])
                self.assertIn("monetization", status["missing_metric_families"])
                self.assertEqual(status["source_availability_status"], "PERMISSION_LIMITED")

                coverage = get_coverage_report(conn, target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                self.assertIn("metric_family_coverage", coverage)
                self.assertIn("historical_range_coverage", coverage)
                self.assertFalse(coverage["not_yet_synced"])

                detail = get_sync_run_detail(conn, run_id=run_id)
                assert detail is not None
                self.assertIn("missing_metric_families", detail)
                self.assertTrue(detail["incomplete_backfill"])

                runs = list_sync_runs(conn, target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                self.assertGreaterEqual(len(runs), 1)
            finally:
                conn.close()

    def test_no_external_data_fallback_and_audit_log_fields(self) -> None:
        with temp_env() as (_td, env), self.assertLogs("services.analytics_center.external_sync", level=logging.INFO) as logs:
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                status = get_sync_status(conn, target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                self.assertEqual(status["source_availability_status"], "NOT_YET_SYNCED")

                coverage = get_coverage_report(conn, target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                self.assertTrue(coverage["not_yet_synced"])

                run_id = create_sync_run(conn, **make_sync_run_payload())
                provider = _Provider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {},
                        "metric_families_returned": [],
                        "metric_families_unavailable": ["views", "impressions", "ctr"],
                        "freshness_status": "UNKNOWN",
                        "freshness_basis": "source_unavailable",
                        "source_unavailable": True,
                    }
                )
                run_external_youtube_ingestion(
                    conn,
                    run_id=run_id,
                    provider=provider,
                    channel_slug="darkwood-reverie",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                )
                count = conn.execute("SELECT COUNT(*) AS c FROM analytics_external_audit_events").fetchone()
                self.assertGreater(int(count["c"]), 0)
            finally:
                conn.close()

            merged = "\n".join(logs.output)
            self.assertIn("provider_name=YOUTUBE", merged)
            self.assertIn("target_scope_type=CHANNEL", merged)
            self.assertIn("target_scope_ref=darkwood-reverie", merged)
            self.assertIn("run_mode=", merged)
            self.assertIn("sync_state=", merged)
            self.assertIn("observed_from=", merged)
            self.assertIn("observed_to=", merged)
            self.assertIn("created_snapshots_count=", merged)
            self.assertIn("partial_snapshots_count=", merged)
            self.assertIn("failed_snapshots_count=", merged)
            self.assertIn("missing_metric_families=", merged)
            self.assertIn("incomplete_backfill=", merged)
            self.assertIn("freshness_status=", merged)


if __name__ == "__main__":
    unittest.main()
