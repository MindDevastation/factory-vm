from __future__ import annotations

import json
import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.external_sync import (
    build_coverage_payload,
    classify_external_availability,
    normalize_metric_families,
    plan_fetch_targets,
)


class TestAnalyticsExternalSyncUnit(unittest.TestCase):
    def test_required_metric_family_recognition(self) -> None:
        self.assertEqual(
            normalize_metric_families(["views", "impressions", "CTR", "watch_time", "average_view_duration", "retention", "subscribers", "monetization"]),
            ("views", "impressions", "ctr", "watch_time", "average_view_duration", "retention", "subscribers", "monetization"),
        )

    def test_invalid_metric_family_validation(self) -> None:
        with self.assertRaises(AnalyticsDomainError):
            normalize_metric_families(["unknown_metric"])

    def test_coverage_payload_validation(self) -> None:
        payload = build_coverage_payload(
            metric_families_requested=["views", "impressions", "ctr"],
            metric_families_returned=["views"],
            metric_families_unavailable=["ctr"],
            covered_window={"from": 1.0, "to": 2.0},
            incomplete_backfill=True,
            freshness_basis="window_end",
        )
        body = json.loads(payload)
        self.assertIn("metric_families_requested", body)
        self.assertIn("metric_families_returned", body)
        self.assertIn("metric_families_unavailable", body)
        self.assertIn("covered_window", body)
        self.assertIn("incomplete_backfill", body)
        self.assertIn("freshness_basis", body)

    def test_run_mode_and_scope_validation_via_planner(self) -> None:
        targets = plan_fetch_targets(
            run_mode="INITIAL_BACKFILL",
            channel_slug="darkwood-reverie",
            release_video_refs=["rel-1"],
            now_ts_value=1000.0,
            stale_before_ts=None,
            backfill_days=10,
            metric_families=["views", "ctr"],
        )
        self.assertEqual(targets[0].target_scope_type, "CHANNEL")
        self.assertEqual(targets[1].target_scope_type, "RELEASE_VIDEO")
        with self.assertRaises(AnalyticsDomainError):
            plan_fetch_targets(
                run_mode="BAD_MODE",
                channel_slug="darkwood-reverie",
                release_video_refs=None,
                now_ts_value=1000.0,
                stale_before_ts=None,
                backfill_days=10,
                metric_families=["views"],
            )

    def test_stale_incomplete_status_calculation(self) -> None:
        self.assertEqual(
            classify_external_availability(
                has_sync_history=True,
                source_unavailable=False,
                permission_limited=False,
                stale=True,
                partial=False,
            ),
            "STALE",
        )
        self.assertEqual(
            classify_external_availability(
                has_sync_history=True,
                source_unavailable=False,
                permission_limited=False,
                stale=False,
                partial=True,
            ),
            "PARTIAL",
        )


if __name__ == "__main__":
    unittest.main()
