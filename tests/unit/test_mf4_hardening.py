from __future__ import annotations

import logging
import unittest
from unittest import mock

from services.analytics_center.mf4_derivation_core import Mf4ComparisonOutput, Mf4PredictionOutput
from services.analytics_center.mf4_runtime import list_mf4_problems, read_mf4_predictions, recompute_mf4
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot


class TestMf4Hardening(unittest.TestCase):
    def _seed_scope(self, conn) -> None:
        seed_mf4_mixed_input_snapshots(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
        seed_mf4_operational_kpi_snapshot(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")

    def test_required_mf4_auditable_events_are_recorded(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_scope(conn)
                with mock.patch(
                    "services.analytics_center.mf4_runtime.derive_comparisons",
                    return_value=[
                        Mf4ComparisonOutput("CHANNEL", "darkwood-reverie", "RELEASE_VS_CHANNEL_BASELINE", "ANOMALY", {"delta_ratio": 0.3, "relative_ranking_summary": "bottom-half"}, {"baseline_family": "RELEASE_VS_CHANNEL"}, ["snapshot:1"], "RELEASE_VS_CHANNEL"),
                        Mf4ComparisonOutput("CHANNEL", "darkwood-reverie", "CHANNEL_VS_SELF_HISTORY", "RISK", {"delta_ratio": 0.6, "relative_ranking_summary": "bottom-half"}, {"baseline_family": "CHANNEL_HISTORICAL"}, ["snapshot:1"], "CHANNEL_HISTORICAL"),
                        Mf4ComparisonOutput("CHANNEL", "darkwood-reverie", "BATCH_MONTH_VS_RECENT_CHANNEL", "NORMAL", {"delta_ratio": 0.1, "relative_ranking_summary": "top-half"}, {"baseline_family": "BATCH_MONTH_HISTORICAL"}, ["snapshot:1"], "BATCH_MONTH_HISTORICAL"),
                        Mf4ComparisonOutput("CHANNEL", "darkwood-reverie", "CHANNEL_VS_PORTFOLIO", "NORMAL", {"delta_ratio": 0.1, "relative_ranking_summary": "top-half"}, {"baseline_family": "PORTFOLIO_COMPARISON"}, ["snapshot:1"], "PORTFOLIO_COMPARISON"),
                    ],
                ), mock.patch(
                    "services.analytics_center.mf4_runtime.derive_predictions",
                    return_value=[
                        Mf4PredictionOutput(
                            scope_type="CHANNEL",
                            scope_ref="darkwood-reverie",
                            prediction_family="STRONG_WEAK_RELEASE_PREDICTION",
                            variance_class="RISK",
                            confidence_class="HIGH",
                            predicted_label="RISK",
                            predicted_value={"risk_score": 0.9},
                            signals_used=[{"signal": "delta_ratio", "value": 0.6}],
                            comparison_basis={"comparison_baseline": {"comparison_family": "RELEASE_VS_CHANNEL_BASELINE"}},
                            explainability_payload={
                                "primary_reason": "risk",
                                "supporting_signals": [],
                                "remediation_hint_or_next_interpretation": "inspect",
                                "predicted_outcome_or_risk": "RISK",
                                "confidence_class_or_band": "HIGH",
                                "key_reasons_signals": [{"signal": "delta_ratio", "value": 0.6}],
                                "comparison_basis": {"comparison_baseline": {"comparison_family": "RELEASE_VS_CHANNEL_BASELINE"}},
                                "next_recommended_operator_action": "inspect",
                            },
                            source_snapshot_refs=["snapshot:1"],
                            comparison_family="RELEASE_VS_CHANNEL_BASELINE",
                        )
                    ],
                ):
                    recompute_mf4(
                        conn,
                        run_kind="FULL_STACK_RECOMPUTE",
                        target_scope_type="CHANNEL",
                        target_scope_ref="darkwood-reverie",
                        recompute_mode="FULL_RECOMPUTE",
                    )
                with mock.patch("services.analytics_center.mf4_runtime.derive_predictions", side_effect=RuntimeError("boom")):
                    recompute_mf4(
                        conn,
                        run_kind="FULL_STACK_RECOMPUTE",
                        target_scope_type="CHANNEL",
                        target_scope_ref="darkwood-reverie",
                        recompute_mode="INCREMENTAL_RECOMPUTE",
                    )
                event_types = {str(r["event_type"]) for r in conn.execute("SELECT event_type FROM analytics_prediction_events").fetchall()}
                required = {
                    "MF4_BASELINE_RECOMPUTE_STARTED",
                    "MF4_BASELINE_RECOMPUTE_COMPLETED",
                    "MF4_COMPARISON_RECOMPUTE_STARTED",
                    "MF4_COMPARISON_RECOMPUTE_COMPLETED",
                    "MF4_PREDICTION_RECOMPUTE_STARTED",
                    "MF4_PREDICTION_RECOMPUTE_COMPLETED",
                    "MF4_BASELINE_SNAPSHOT_CREATED",
                    "MF4_COMPARISON_SNAPSHOT_CREATED",
                    "MF4_PREDICTION_SNAPSHOT_CREATED",
                    "MF4_ANOMALY_CLASSIFIED",
                    "MF4_RISK_CLASSIFIED",
                    "MF4_EXPLAINABILITY_PAYLOAD_ATTACHED",
                    "MF4_RECOMPUTE_PARTIAL_FAILURE_RECORDED",
                }
                self.assertTrue(required.issubset(event_types))
            finally:
                conn.close()

    def test_required_log_fields_present(self) -> None:
        with temp_env() as (_td, env), self.assertLogs("services.analytics_center.mf4_runtime", level=logging.INFO) as logs:
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_scope(conn)
                recompute_mf4(
                    conn,
                    run_kind="FULL_STACK_RECOMPUTE",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
            finally:
                conn.close()
        merged = "\n".join(logs.output)
        for field in (
            "target_scope_type=",
            "target_scope_ref=",
            "run_kind=",
            "prediction_family=",
            "comparison_family=",
            "variance_class=",
            "confidence_class=",
            "snapshot_id=",
            "anomaly_count=",
            "risk_count=",
        ):
            self.assertIn(field, merged)

    def test_operator_facing_observability_fields_available(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_scope(conn)
                recompute_mf4(
                    conn,
                    run_kind="FULL_STACK_RECOMPUTE",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
                preds = read_mf4_predictions(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie", current_only=True)
                self.assertGreaterEqual(len(preds), 1)
                first = preds[0]
                self.assertIn("comparison_baseline", str(first["comparison_basis_json"]))
                self.assertIn("primary_reason", str(first["explainability_payload_json"]))
                self.assertIn("supporting_signals", str(first["explainability_payload_json"]))
                self.assertIn("remediation_hint_or_next_interpretation", str(first["explainability_payload_json"]))
                problems = list_mf4_problems(conn, scope_type="CHANNEL")
                if problems:
                    self.assertTrue(all(str(p["variance_class"]) in {"ANOMALY", "RISK"} for p in problems))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
