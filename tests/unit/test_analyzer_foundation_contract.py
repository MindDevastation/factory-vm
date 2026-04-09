from __future__ import annotations

import unittest

from services.analytics_center.analyzer_foundation import build_analyzer_foundation_contract


class TestAnalyzerFoundationContract(unittest.TestCase):
    def test_contract_exposes_foundation_and_gap_boundaries(self) -> None:
        contract = build_analyzer_foundation_contract()

        self.assertEqual(contract["contract_version"], "MF1-S1")
        self.assertEqual(contract["completeness"], "FOUNDATION_ONLY")

        model = contract["analyzer_model"]
        self.assertEqual(model["core_mode"], "ONE_ANALYZER_MANY_PROFILES")
        self.assertEqual(model["profile_axes"], ["CHANNEL_STRATEGY_PROFILE", "FORMAT_PROFILE"])
        self.assertEqual(model["default_mutation_policy"], "NO_AUTO_APPLY")
        self.assertEqual(model["refresh_selector_values"], ["HOURLY", "EVERY_12_HOURS", "DAILY"])

        registry = contract["profile_registry_contract"]
        self.assertEqual(registry["core_analyzer_mode"], "ONE_ANALYZER_MANY_PROFILES")
        self.assertEqual(registry["foundations_affected"], ["weighting", "baseline", "prediction", "recommendation", "planning"])

        sample = contract["sample_profile_effects"]
        self.assertEqual(sample["channel_strategy_profile"], "LONG_FORM_BACKGROUND_MUSIC")
        self.assertEqual(sample["format_profile"], "LONG_FORM")
        self.assertIn("retention", sample["weighting_hooks"])
        self.assertIn("window_bias", sample["baseline_hooks"])
        self.assertIn("primary_target", sample["prediction_hooks"])
        self.assertIn("priority_theme", sample["recommendation_hooks"])
        self.assertIn("cadence_mode", sample["planning_hooks"])
        self.assertTrue(sample["hook_fingerprint"])

        ingestion = contract["ingestion_interface_contract"]
        self.assertEqual(ingestion["execution_scope"], "INTERFACE_FOUNDATION_ONLY")
        self.assertIn("RELEASE_VIDEO", ingestion["supported_scope_types"])
        self.assertIn("coverage_state_explicit", ingestion["invariants"])
        self.assertIn("permission_limited_visibility_explicit", ingestion["invariants"])

        service_boundary = contract["service_boundary_contract"]
        self.assertEqual(service_boundary["write_service"], "write_analyzer_snapshot")
        self.assertEqual(service_boundary["read_service"], "read_analyzer_snapshots")
        self.assertIn("NOT_YET_SYNCED", service_boundary["sync_states"])
        self.assertIn("one_analyzer_many_profiles", service_boundary["invariants"])

        state_model = contract["state_model_contract"]
        self.assertEqual(state_model["coverage_states"], ["MISSING", "PARTIAL", "PERMISSION_LIMITED", "STALE", "REFRESHED"])
        self.assertEqual(
            state_model["visibility_guarantees"],
            ["missing", "partial", "permission-limited", "stale", "refreshed"],
        )

        coverage = contract["mandatory_scope_coverage"]
        self.assertEqual(coverage["analytics_domain_snapshot_foundation"]["status"], "READY")
        self.assertEqual(coverage["one_analyzer_many_profiles_foundation_hooks"]["status"], "READY")

        self.assertEqual(coverage["required_metrics_breadth"]["status"], "GAP")
        self.assertEqual(coverage["refresh_selector_exactness"]["status"], "READY")
        self.assertEqual(coverage["planning_assistant_v1_surface"]["status"], "GAP")
        self.assertEqual(coverage["telegram_analyzer_surface"]["status"], "GAP")

        missing = set(contract["missing_required_metric_dimensions"])
        self.assertIn("unique_viewers", missing)
        self.assertIn("traffic_sources", missing)
        self.assertNotIn("views", missing)
