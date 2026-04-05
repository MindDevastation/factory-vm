from __future__ import annotations

import unittest

from services.analytics_center.mf4_derivation_core import derive_baselines, derive_comparisons, derive_predictions, persist_mf4_derivation
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot


class TestMf4DerivationCoreIntegration(unittest.TestCase):
    def _seed_scope(self, conn, *, scope_type: str, scope_ref: str) -> None:
        seed_mf4_mixed_input_snapshots(conn, scope_type=scope_type, scope_ref=scope_ref)
        seed_mf4_operational_kpi_snapshot(conn, scope_type=scope_type, scope_ref=scope_ref)

    def test_recompute_channel_historical_baseline(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_scope(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                baselines = derive_baselines(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                self.assertTrue(any(b.baseline_family == "CHANNEL_HISTORICAL" for b in baselines))
            finally:
                conn.close()

    def test_recompute_release_vs_channel_comparison(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                release_id = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, origin_meta_file_id, created_at) VALUES(?, 'mf4-release', 'd', '[]', 'meta-mf4-rel', ?)",
                        (channel_id, dbm.now_ts()),
                    ).lastrowid
                )
                self._seed_scope(conn, scope_type="RELEASE", scope_ref=str(release_id))
                baselines = derive_baselines(conn, scope_type="RELEASE", scope_ref=str(release_id))
                comparisons = derive_comparisons(conn, baselines=baselines)
                self.assertTrue(any(c.comparison_family == "RELEASE_VS_CHANNEL_BASELINE" for c in comparisons))
            finally:
                conn.close()

    def test_recompute_batch_month_and_portfolio_baselines(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_scope(conn, scope_type="BATCH_MONTH", scope_ref="2026-04")
                self._seed_scope(conn, scope_type="PORTFOLIO", scope_ref="core")
                baselines_batch = derive_baselines(conn, scope_type="BATCH_MONTH", scope_ref="2026-04")
                baselines_portfolio = derive_baselines(conn, scope_type="PORTFOLIO", scope_ref="core")
                self.assertTrue(any(b.baseline_family == "BATCH_MONTH_HISTORICAL" for b in baselines_batch))
                self.assertTrue(any(b.baseline_family == "PORTFOLIO_COMPARISON" for b in baselines_portfolio))
            finally:
                conn.close()

    def test_recompute_all_prediction_families_and_persist(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_scope(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                baselines = derive_baselines(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                comparisons = derive_comparisons(conn, baselines=baselines)
                predictions = derive_predictions(conn, comparisons=comparisons)
                counts = persist_mf4_derivation(conn, baselines=baselines, comparisons=comparisons, predictions=predictions)
                families = {p.prediction_family for p in predictions}
                self.assertIn("WEAK_RELEASE_RISK", families)
                self.assertIn("PUBLISH_WINDOW_QUALITY", families)
                self.assertIn("CHANNEL_MOMENTUM", families)
                self.assertIn("CADENCE_DEGRADATION_RISK", families)
                self.assertIn("OPERATIONAL_ANOMALY_RISK", families)
                self.assertEqual(counts["prediction_count"], 5)
                row = conn.execute("SELECT COUNT(*) AS c FROM analytics_prediction_snapshots WHERE is_current = 1").fetchone()
                self.assertEqual(int(row["c"]), 5)
            finally:
                conn.close()

    def test_mixed_external_internal_input_path_persists_prediction_snapshot(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_scope(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                baselines = derive_baselines(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                comparisons = derive_comparisons(conn, baselines=baselines)
                predictions = derive_predictions(conn, comparisons=comparisons)
                persist_mf4_derivation(conn, baselines=baselines, comparisons=comparisons, predictions=predictions)
                row = conn.execute(
                    "SELECT source_snapshot_refs_json, explainability_payload_json FROM analytics_prediction_snapshots ORDER BY id DESC LIMIT 1"
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertIn("snapshot:", str(row["source_snapshot_refs_json"]))
                self.assertIn("primary_reason", str(row["explainability_payload_json"]))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
