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
        self.assertEqual(normalize_analytics_filters({"channel": "abc", "severity": "WARNING"}), {"channel": "abc", "severity": "WARNING"})
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
