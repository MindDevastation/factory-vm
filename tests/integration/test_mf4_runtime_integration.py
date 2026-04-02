from __future__ import annotations

import unittest
from unittest import mock

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.mf4_runtime import (
    list_mf4_problems,
    read_mf4_baselines,
    read_mf4_comparisons,
    read_mf4_predictions,
    recompute_mf4,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot


class TestMf4RuntimeIntegration(unittest.TestCase):
    def _seed_channel_scope(self, conn) -> None:
        seed_mf4_mixed_input_snapshots(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
        seed_mf4_operational_kpi_snapshot(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")

    def test_current_snapshot_supersession_works_correctly(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_channel_scope(conn)
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                recompute_mf4(
                    conn,
                    run_kind="FULL_STACK_RECOMPUTE",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
                recompute_mf4(
                    conn,
                    run_kind="FULL_STACK_RECOMPUTE",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="INCREMENTAL_RECOMPUTE",
                )
                for table in ("analytics_baseline_snapshots", "analytics_comparison_snapshots", "analytics_prediction_snapshots"):
                    old = conn.execute(f"SELECT COUNT(*) AS c FROM {table} WHERE scope_type='CHANNEL' AND scope_ref=? AND is_current = 0", (str(channel_id),)).fetchone()
                    self.assertGreater(int(old["c"]), 0)
            finally:
                conn.close()

    def test_partial_recompute_retains_successful_outputs(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_channel_scope(conn)
                with mock.patch("services.analytics_center.mf4_runtime.derive_predictions", side_effect=RuntimeError("prediction boom")):
                    recompute_mf4(
                        conn,
                        run_kind="FULL_STACK_RECOMPUTE",
                        target_scope_type="CHANNEL",
                        target_scope_ref="darkwood-reverie",
                        recompute_mode="FULL_RECOMPUTE",
                    )
                run = conn.execute("SELECT run_state, baseline_count, comparison_count, prediction_count FROM analytics_prediction_runs ORDER BY id DESC LIMIT 1").fetchone()
                self.assertEqual(str(run["run_state"]), "PARTIAL")
                self.assertGreater(int(run["baseline_count"]), 0)
                self.assertGreater(int(run["comparison_count"]), 0)
                self.assertEqual(int(run["prediction_count"]), 0)
                baseline_current = conn.execute("SELECT COUNT(*) AS c FROM analytics_baseline_snapshots WHERE is_current = 1").fetchone()
                comparison_current = conn.execute("SELECT COUNT(*) AS c FROM analytics_comparison_snapshots WHERE is_current = 1").fetchone()
                self.assertGreater(int(baseline_current["c"]), 0)
                self.assertGreater(int(comparison_current["c"]), 0)
            finally:
                conn.close()

    def test_read_interfaces_return_current_and_history(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_channel_scope(conn)
                recompute_mf4(conn, run_kind="FULL_STACK_RECOMPUTE", target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie", recompute_mode="FULL_RECOMPUTE")
                recompute_mf4(conn, run_kind="FULL_STACK_RECOMPUTE", target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie", recompute_mode="INCREMENTAL_RECOMPUTE")

                baselines_current = read_mf4_baselines(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie", current_only=True)
                baselines_all = read_mf4_baselines(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie", current_only=False)
                comparisons_current = read_mf4_comparisons(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie", current_only=True)
                predictions_current = read_mf4_predictions(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie", current_only=True)

                self.assertGreater(len(baselines_current), 0)
                self.assertGreater(len(baselines_all), len(baselines_current))
                self.assertGreater(len(comparisons_current), 0)
                self.assertGreater(len(predictions_current), 0)
            finally:
                conn.close()

    def test_problem_listing_group_filters(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_channel_scope(conn)
                recompute_mf4(conn, run_kind="FULL_STACK_RECOMPUTE", target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie", recompute_mode="FULL_RECOMPUTE")
                all_problems = list_mf4_problems(conn, scope_type="CHANNEL")
                if all_problems:
                    fam = str(all_problems[0]["prediction_family"])
                    status = str(all_problems[0]["variance_class"])
                    filtered = list_mf4_problems(conn, scope_type="CHANNEL", prediction_family=fam, status_class=status)
                    self.assertTrue(all(str(r["prediction_family"]) == fam for r in filtered))
                    self.assertTrue(all(str(r["variance_class"]) == status for r in filtered))
            finally:
                conn.close()

    def test_missing_source_and_missing_baseline_fail_explicitly(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                with self.assertRaises(AnalyticsDomainError):
                    recompute_mf4(
                        conn,
                        run_kind="BASELINE_RECOMPUTE",
                        target_scope_type="CHANNEL",
                        target_scope_ref="darkwood-reverie",
                        recompute_mode="FULL_RECOMPUTE",
                    )
                with self.assertRaises(AnalyticsDomainError):
                    recompute_mf4(
                        conn,
                        run_kind="COMPARISON_RECOMPUTE",
                        target_scope_type="CHANNEL",
                        target_scope_ref="darkwood-reverie",
                        recompute_mode="FULL_RECOMPUTE",
                    )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
