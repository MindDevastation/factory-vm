from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.ui_contracts import (
    build_analytics_page_contract,
    build_freshness_coverage_summary,
    normalize_analytics_filters,
)


class TestMf6PageContracts(unittest.TestCase):
    def test_filter_validation(self) -> None:
        self.assertEqual(
            normalize_analytics_filters({"channel": "abc", "severity": "WARNING", "date_from": "2026-01-01", "period_compare": "PREVIOUS_PERIOD"}),
            {"channel": "abc", "severity": "WARNING", "date_from": "2026-01-01", "period_compare": "PREVIOUS_PERIOD"},
        )

        refreshed = build_analytics_page_contract(
            page_scope="OVERVIEW",
            applied_filters={},
            freshness_summary={"status": "REFRESHED"},
            source_coverage_summary={"status": "REFRESHED"},
            summary_cards=[],
            detail_blocks=[],
            anomaly_risk_markers=[],
            recommendation_summary=[],
            available_actions=[],
            export_report_actions=[],
        )
        self.assertEqual(refreshed["data_completeness"], "FULL")

        with self.assertRaises(AnalyticsDomainError):
            normalize_analytics_filters({"unknown": "x"})

    def test_freshness_coverage_builder(self) -> None:
        freshness, coverage = build_freshness_coverage_summary(source_states={"A": "FRESH", "B": "MISSING"})
        self.assertEqual(freshness["status"], "PARTIAL")
        self.assertIn("B", coverage["missing_sources"])

    def test_page_contract_validation(self) -> None:
        payload = build_analytics_page_contract(
            page_scope="OVERVIEW",
            applied_filters={},
            freshness_summary={"status": "PARTIAL"},
            source_coverage_summary={"status": "PARTIAL"},
            summary_cards=[],
            detail_blocks=[],
            anomaly_risk_markers=[],
            recommendation_summary=[],
            available_actions=[],
            export_report_actions=[],
        )
        self.assertIn("filter_state_token", payload)
        self.assertEqual(payload["data_completeness"], "PARTIAL")
        self.assertIn("period_semantics", payload)
        self.assertIn("chart_blocks", payload)
        self.assertTrue(all(bool(c.get("animated")) for c in payload["chart_blocks"]))
        self.assertEqual(payload["semantic_filter_contract"]["date_filters"]["period_compare"], "PREVIOUS_PERIOD")

        refreshed = build_analytics_page_contract(
            page_scope="OVERVIEW",
            applied_filters={},
            freshness_summary={"status": "REFRESHED"},
            source_coverage_summary={"status": "REFRESHED"},
            summary_cards=[],
            detail_blocks=[],
            anomaly_risk_markers=[],
            recommendation_summary=[],
            available_actions=[],
            export_report_actions=[],
        )
        self.assertEqual(refreshed["data_completeness"], "FULL")

        with self.assertRaises(AnalyticsDomainError):
            build_analytics_page_contract(
                page_scope="BAD",
                applied_filters={},
                freshness_summary={},
                source_coverage_summary={},
                summary_cards=[],
                detail_blocks=[],
                anomaly_risk_markers=[],
                recommendation_summary=[],
                available_actions=[],
                export_report_actions=[],
            )


if __name__ == "__main__":
    unittest.main()
