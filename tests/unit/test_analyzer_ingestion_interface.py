from __future__ import annotations

import unittest

from services.analytics_center.analyzer_ingestion_interface import (
    AnalyzerIngestionRequest,
    AnalyzerIngestionResponse,
    build_analyzer_ingestion_contract,
    normalize_ingestion_request,
    normalize_ingestion_response,
)


class TestAnalyzerIngestionInterface(unittest.TestCase):
    def test_contract_declares_required_scope_metrics_and_states(self) -> None:
        contract = build_analyzer_ingestion_contract()
        self.assertEqual(contract["core_analyzer_mode"], "ONE_ANALYZER_MANY_PROFILES")
        self.assertEqual(contract["supported_scope_types"], ["CHANNEL", "RELEASE_VIDEO"])
        self.assertIn("views", contract["required_metric_dimensions"])
        self.assertIn("unique_viewers", contract["required_metric_dimensions"])
        self.assertIn("PERMISSION_LIMITED", contract["coverage_states"])
        self.assertEqual(contract["execution_scope"], "INTERFACE_FOUNDATION_ONLY")

    def test_request_requires_profile_context_and_supported_metrics(self) -> None:
        req = normalize_ingestion_request(
            AnalyzerIngestionRequest(
                scope_type="channel",
                scope_ref="demo",
                metric_dimensions=("views", "watch_time"),
                channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
                format_profile="LONG_FORM",
                observed_from=1.0,
                observed_to=2.0,
            )
        )
        self.assertEqual(req.scope_type, "CHANNEL")
        self.assertEqual(req.metric_dimensions, ("views", "watch_time"))

        with self.assertRaises(ValueError):
            normalize_ingestion_request(
                AnalyzerIngestionRequest(
                    scope_type="channel",
                    scope_ref="demo",
                    metric_dimensions=("unknown_metric",),
                    channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
                    format_profile="LONG_FORM",
                    observed_from=None,
                    observed_to=None,
                )
            )

    def test_response_enforces_subset_and_coverage_state_visibility(self) -> None:
        response = normalize_ingestion_response(
            AnalyzerIngestionResponse(
                scope_type="channel",
                scope_ref="demo",
                metric_dimensions_requested=("views", "watch_time", "unique_viewers"),
                metric_dimensions_returned=("views", "watch_time"),
                metric_dimensions_unavailable=("unique_viewers",),
                coverage_state="permission_limited",
                freshness_basis="provider",
                payload={"ok": True},
            )
        )
        self.assertEqual(response.coverage_state, "PERMISSION_LIMITED")

        with self.assertRaises(ValueError):
            normalize_ingestion_response(
                AnalyzerIngestionResponse(
                    scope_type="channel",
                    scope_ref="demo",
                    metric_dimensions_requested=("views",),
                    metric_dimensions_returned=("watch_time",),
                    metric_dimensions_unavailable=(),
                    coverage_state="REFRESHED",
                    freshness_basis="provider",
                    payload={},
                )
            )
