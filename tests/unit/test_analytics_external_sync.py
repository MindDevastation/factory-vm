from __future__ import annotations

import json
import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.external_sync import (
    DEFAULT_REQUIRED_METRIC_FAMILIES,
    build_backfill_action_contract,
    build_backfill_runtime_contract,
    build_manual_refresh_action_contract,
    build_manual_refresh_runtime_contract,
    build_scheduled_refresh_control_contract,
    build_coverage_payload,
    classify_external_availability,
    normalize_metric_families,
    normalize_refresh_selector,
    plan_fetch_targets,
)


class TestAnalyticsExternalSyncUnit(unittest.TestCase):
    def test_required_metric_family_recognition(self) -> None:
        self.assertEqual(
            normalize_metric_families(
                [
                    "views",
                    "impressions",
                    "CTR",
                    "watch_time",
                    "average_view_duration",
                    "retention",
                    "subscribers",
                    "monetization",
                    "unique_viewers",
                    "new_casual_regular_returning_viewers",
                    "traffic_sources",
                    "youtube_search_terms",
                    "viewers_when_on_youtube",
                    "retention_key_moments",
                    "retention_typical_benchmark",
                    "top_geographies",
                    "subscriber_conversion_context",
                ]
            ),
            (
                "views",
                "impressions",
                "ctr",
                "watch_time",
                "average_view_duration",
                "retention",
                "subscribers_gained_lost",
                "revenue_rpm",
                "unique_viewers",
                "viewer_segments_new_casual_regular_returning",
                "traffic_sources",
                "youtube_search_terms",
                "viewers_when_on_youtube",
                "retention_key_moments",
                "retention_typical_benchmark",
                "top_geographies",
                "subscriber_conversion_context",
            ),
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
        self.assertIn("missing_metric_families", body)
        self.assertIn("permission_limited_metric_families", body)
        self.assertIn("availability_limited_metric_families", body)
        self.assertIn("stale_metric_families", body)
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

    def test_default_required_metric_breadth_is_full(self) -> None:
        self.assertEqual(
            DEFAULT_REQUIRED_METRIC_FAMILIES,
            (
                "views",
                "impressions",
                "ctr",
                "watch_time",
                "average_view_duration",
                "retention",
                "subscribers_gained_lost",
                "revenue_rpm",
                "unique_viewers",
                "viewer_segments_new_casual_regular_returning",
                "traffic_sources",
                "youtube_search_terms",
                "viewers_when_on_youtube",
                "retention_key_moments",
                "retention_typical_benchmark",
                "top_geographies",
                "subscriber_conversion_context",
            ),
        )

    def test_scheduled_refresh_selector_contract_is_closed_exact_set(self) -> None:
        contract = build_scheduled_refresh_control_contract()
        self.assertEqual(contract["run_mode"], "SCHEDULED_SYNC")
        self.assertEqual(contract["allowed_refresh_selectors"], ["HOURLY", "EVERY_12_HOURS", "DAILY"])
        self.assertEqual(contract["selector_to_interval_seconds"]["HOURLY"], 3600)
        self.assertEqual(contract["selector_to_interval_seconds"]["EVERY_12_HOURS"], 43200)
        self.assertEqual(contract["selector_to_interval_seconds"]["DAILY"], 86400)

    def test_scheduled_refresh_selector_rejects_unsupported_intervals(self) -> None:
        self.assertEqual(normalize_refresh_selector("hourly"), "HOURLY")
        with self.assertRaises(AnalyticsDomainError):
            normalize_refresh_selector("every_6_hours")

    def test_manual_refresh_action_runtime_contract_is_explicit(self) -> None:
        control = build_manual_refresh_action_contract()
        self.assertEqual(control["action"], "MANUAL_REFRESH")
        self.assertEqual(
            control["allowed_run_modes"],
            ["MANUAL_REFRESH", "PARTIAL_REFRESH", "STALE_RESYNC", "INITIAL_BACKFILL"],
        )
        self.assertIn("not_scheduled_selector_alias", control["invariants"])
        self.assertIn("freshness_sync_coverage_visible", control["invariants"])

        runtime = build_manual_refresh_runtime_contract(
            refresh_mode="MANUAL_REFRESH",
            force=True,
            observed_from=10.0,
            observed_to=30.0,
        )
        self.assertEqual(runtime["action"], "MANUAL_REFRESH")
        self.assertEqual(runtime["run_mode"], "MANUAL_REFRESH")
        self.assertTrue(runtime["force"])
        self.assertEqual(runtime["window_seconds"], 20)
        self.assertEqual(runtime["freshness_basis"], "manual_refresh_force")

    def test_manual_refresh_runtime_rejects_scheduled_sync_alias(self) -> None:
        with self.assertRaises(AnalyticsDomainError):
            build_manual_refresh_runtime_contract(
                refresh_mode="SCHEDULED_SYNC",
                force=False,
                observed_from=None,
                observed_to=None,
            )

    def test_backfill_action_runtime_contract_is_explicit(self) -> None:
        contract = build_backfill_action_contract()
        self.assertEqual(contract["action"], "HISTORICAL_BACKFILL")
        self.assertEqual(contract["run_mode"], "INITIAL_BACKFILL")
        self.assertIn("historical_truth_preserved", contract["invariants"])
        self.assertIn("freshness_sync_coverage_visible", contract["invariants"])

        runtime = build_backfill_runtime_contract(backfill_days=21, observed_to=200.0)
        self.assertEqual(runtime["action"], "HISTORICAL_BACKFILL")
        self.assertEqual(runtime["run_mode"], "INITIAL_BACKFILL")
        self.assertEqual(runtime["backfill_days"], 21)
        self.assertEqual(runtime["freshness_basis"], "historical_backfill_window")
        self.assertIn("backfill_contract", runtime)

    def test_backfill_runtime_contract_rejects_invalid_days(self) -> None:
        with self.assertRaises(AnalyticsDomainError):
            build_backfill_runtime_contract(backfill_days=0, observed_to=None)
        with self.assertRaises(AnalyticsDomainError):
            build_backfill_runtime_contract(backfill_days=366, observed_to=None)


if __name__ == "__main__":
    unittest.main()
