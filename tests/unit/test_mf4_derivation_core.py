from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.mf4_derivation_core import (
    Mf4ComparisonOutput,
    build_comparison_basis_and_explainability,
    classify_variance,
    derive_predictions,
    resolve_baseline_window,
)


class TestMf4DerivationCoreUnit(unittest.TestCase):
    def test_baseline_window_resolver_supports_required_strategies(self) -> None:
        now = 1_700_000_000.0
        self.assertEqual(resolve_baseline_window(strategy="ROLLING_HISTORICAL", observed_to=now)["window_type"], "ROLLING_HISTORICAL")
        self.assertEqual(resolve_baseline_window(strategy="BOUNDED_COMPARISON", observed_to=now)["window_type"], "BOUNDED_COMPARISON")
        self.assertEqual(resolve_baseline_window(strategy="RECENT_N", observed_to=now)["window_type"], "RECENT_N")
        self.assertEqual(resolve_baseline_window(strategy="MONTHLY_BATCH", observed_to=now)["window_type"], "MONTHLY_BATCH")
        self.assertEqual(resolve_baseline_window(strategy="LAST_KNOWN_CURRENT", observed_to=now)["window_type"], "LAST_KNOWN_CURRENT")

    def test_variance_classification(self) -> None:
        self.assertEqual(classify_variance(delta_ratio=0.05, anomaly_threshold=0.2, risk_threshold=0.5), "NORMAL")
        self.assertEqual(classify_variance(delta_ratio=0.25, anomaly_threshold=0.2, risk_threshold=0.5), "ANOMALY")
        self.assertEqual(classify_variance(delta_ratio=0.7, anomaly_threshold=0.2, risk_threshold=0.5), "RISK")

    def test_explainability_required_for_prediction_payload(self) -> None:
        with self.assertRaises(AnalyticsDomainError):
            build_comparison_basis_and_explainability(
                primary_reason="",
                supporting_signals=[],
                remediation_hint_or_next_interpretation="inspect",
                scope={"scope_type": "CHANNEL", "scope_ref": "x"},
                comparison_baseline={"comparison_family": "CHANNEL_VS_SELF_HISTORY"},
            )

    def test_prediction_family_validation(self) -> None:
        with self.assertRaises(AnalyticsDomainError):
            derive_predictions(
                None,
                comparisons=[],
            )

    def test_payload_validation_for_prediction_builder(self) -> None:
        basis, explainability = build_comparison_basis_and_explainability(
            primary_reason="ok",
            supporting_signals=[{"signal": "s", "value": 1}],
            remediation_hint_or_next_interpretation="inspect queue",
            scope={"scope_type": "CHANNEL", "scope_ref": "darkwood-reverie"},
            comparison_baseline={"comparison_family": "CHANNEL_VS_SELF_HISTORY", "baseline_family": "CHANNEL_HISTORICAL"},
        )
        self.assertIn("comparison_baseline", basis)
        self.assertIn("supporting_signals", explainability)
        self.assertIn("next_recommended_operator_action", explainability)

    def test_prediction_registry_outputs_required_families(self) -> None:
        comparisons = [
            Mf4ComparisonOutput("CHANNEL", "darkwood-reverie", "RELEASE_VS_CHANNEL_BASELINE", "NORMAL", {"delta_ratio": 0.1, "relative_ranking_summary": "top-half"}, {}, ["snapshot:1"], "RELEASE_VS_CHANNEL"),
            Mf4ComparisonOutput("CHANNEL", "darkwood-reverie", "CHANNEL_VS_SELF_HISTORY", "NORMAL", {"delta_ratio": 0.1, "relative_ranking_summary": "top-half"}, {}, ["snapshot:1"], "CHANNEL_HISTORICAL"),
            Mf4ComparisonOutput("CHANNEL", "darkwood-reverie", "BATCH_MONTH_VS_RECENT_CHANNEL", "NORMAL", {"delta_ratio": 0.1, "relative_ranking_summary": "top-half"}, {}, ["snapshot:1"], "BATCH_MONTH_HISTORICAL"),
            Mf4ComparisonOutput("CHANNEL", "darkwood-reverie", "CHANNEL_VS_PORTFOLIO", "NORMAL", {"delta_ratio": 0.1, "relative_ranking_summary": "top-half"}, {}, ["snapshot:1"], "PORTFOLIO_COMPARISON"),
        ]
        outputs = derive_predictions(None, comparisons=comparisons)
        families = {o.prediction_family for o in outputs}
        self.assertEqual(
            families,
            {
                "VIEW_GROWTH_PREDICTION",
                "WATCH_TIME_GROWTH_PREDICTION",
                "CTR_PREDICTION",
                "STRONG_WEAK_RELEASE_PREDICTION",
                "BEST_PUBLISH_WINDOW_PREDICTION",
                "CHANNEL_TREND_PREDICTION",
                "ANOMALY_DROP_RISK_PREDICTION",
            },
        )


if __name__ == "__main__":
    unittest.main()
