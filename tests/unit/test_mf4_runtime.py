from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.mf4_derivation_core import Mf4PredictionOutput, persist_mf4_derivation
from services.analytics_center.mf4_runtime import (
    _validate_recompute_mode,
    _validate_run_kind,
    _validate_run_state,
    create_prediction_run,
    finalize_prediction_run,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestMf4RuntimeUnit(unittest.TestCase):
    def test_run_kind_validation(self) -> None:
        self.assertEqual(_validate_run_kind("FULL_STACK_RECOMPUTE"), "FULL_STACK_RECOMPUTE")
        with self.assertRaises(AnalyticsDomainError):
            _validate_run_kind("BAD_KIND")

    def test_recompute_mode_validation(self) -> None:
        self.assertEqual(_validate_recompute_mode("FULL_RECOMPUTE"), "FULL_RECOMPUTE")
        with self.assertRaises(AnalyticsDomainError):
            _validate_recompute_mode("BAD_MODE")

    def test_run_state_validation(self) -> None:
        self.assertEqual(_validate_run_state("SUCCEEDED"), "SUCCEEDED")
        with self.assertRaises(AnalyticsDomainError):
            _validate_run_state("BAD_STATE")

    def test_finalize_only_once(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                run_id = create_prediction_run(
                    conn,
                    run_kind="FULL_STACK_RECOMPUTE",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
                finalize_prediction_run(
                    conn,
                    run_id=run_id,
                    run_state="SUCCEEDED",
                    baseline_count=1,
                    comparison_count=1,
                    prediction_count=1,
                    anomaly_count=0,
                    risk_count=0,
                )
                with self.assertRaises(AnalyticsDomainError):
                    finalize_prediction_run(
                        conn,
                        run_id=run_id,
                        run_state="FAILED",
                        baseline_count=1,
                        comparison_count=1,
                        prediction_count=1,
                        anomaly_count=0,
                        risk_count=0,
                    )
            finally:
                conn.close()

    def test_explainability_and_comparison_basis_required_for_current_prediction(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                with self.assertRaises(AnalyticsDomainError):
                    persist_mf4_derivation(
                        conn,
                        baselines=[],
                        comparisons=[],
                        predictions=[
                            Mf4PredictionOutput(
                                scope_type="CHANNEL",
                                scope_ref="darkwood-reverie",
                                prediction_family="STRONG_WEAK_RELEASE_PREDICTION",
                                variance_class="RISK",
                                confidence_class="HIGH",
                                predicted_label="RISK",
                                predicted_value={"risk_score": 0.9},
                                signals_used=[],
                                comparison_basis={},
                                explainability_payload={},
                                source_snapshot_refs=["snapshot:1"],
                                comparison_family="RELEASE_VS_CHANNEL_BASELINE",
                            )
                        ],
                    )
            finally:
                conn.close()

    def test_source_snapshot_refs_json_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                with self.assertRaises(AnalyticsDomainError):
                    persist_mf4_derivation(
                        conn,
                        baselines=[],
                        comparisons=[],
                        predictions=[
                            Mf4PredictionOutput(
                                scope_type="CHANNEL",
                                scope_ref="darkwood-reverie",
                                prediction_family="STRONG_WEAK_RELEASE_PREDICTION",
                                variance_class="RISK",
                                confidence_class="HIGH",
                                predicted_label="RISK",
                                predicted_value={"risk_score": 0.9},
                                signals_used=[],
                                comparison_basis={"x": 1},
                                explainability_payload={"primary_reason": "x"},
                                source_snapshot_refs=[],
                                comparison_family="RELEASE_VS_CHANNEL_BASELINE",
                            )
                        ],
                    )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
