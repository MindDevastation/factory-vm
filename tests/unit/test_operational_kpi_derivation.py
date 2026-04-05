from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.operational_kpi import (
    KpiOutput,
    _validate_output,
    build_explainability_payload,
    normalize_problem_listing_filters,
)


class TestOperationalKpiDerivationUnit(unittest.TestCase):
    def test_scope_family_status_validation(self) -> None:
        normalize_problem_listing_filters(scope_type="CHANNEL", kpi_family="PIPELINE_TIMING", status_class="NORMAL")
        with self.assertRaises(AnalyticsDomainError):
            normalize_problem_listing_filters(scope_type="BAD")
        with self.assertRaises(AnalyticsDomainError):
            normalize_problem_listing_filters(kpi_family="BAD")
        with self.assertRaises(AnalyticsDomainError):
            normalize_problem_listing_filters(status_class="BAD")

    def test_explainability_and_evidence_payload_validation(self) -> None:
        payload = build_explainability_payload(
            primary_reason_code="R",
            primary_reason_text="reason",
            supporting_signals_json=[{"s": 1}],
            remediation_hint="do x",
            baseline_scope_type="CHANNEL",
            baseline_scope_ref="x",
            baseline_window_ref="latest",
            evidence_payload_json={"e": 1},
        )
        self.assertIn("primary_reason_code", payload)
        with self.assertRaises(AnalyticsDomainError):
            build_explainability_payload(
                primary_reason_code="",
                primary_reason_text="reason",
                supporting_signals_json=[{"s": 1}],
                remediation_hint="do x",
                baseline_scope_type="CHANNEL",
                baseline_scope_ref="x",
                baseline_window_ref="latest",
                evidence_payload_json={"e": 1},
            )

    def test_explainability_required_for_risk_or_anomaly(self) -> None:
        with self.assertRaises(AnalyticsDomainError):
            _validate_output(
                KpiOutput(
                    scope_type="CHANNEL",
                    scope_ref="x",
                    kpi_family="PIPELINE_TIMING",
                    kpi_code="k",
                    status_class="RISK",
                    value_payload={"v": 1},
                    explainability_payload=None,
                    source_snapshot_refs=["a"],
                )
            )


if __name__ == "__main__":
    unittest.main()
