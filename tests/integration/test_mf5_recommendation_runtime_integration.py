from __future__ import annotations

import unittest
from unittest import mock

from services.analytics_center.recommendation_runtime import (
    group_recommendations,
    inspect_recommendation,
    list_prioritized_recommendation_queue,
    read_recommendations,
    recompute_recommendations,
    update_recommendation_lifecycle,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.recommendation_fixtures import seed_recommendation_inputs


class TestMf5RecommendationRuntimeIntegration(unittest.TestCase):
    def test_supersede_current_recommendation_and_preserve_history(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                run_id_1 = recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="FULL_RECOMPUTE")
                self.assertGreater(run_id_1, 0)
                run_id_2 = recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="INCREMENTAL_RECOMPUTE")
                self.assertGreater(run_id_2, run_id_1)
                rows = conn.execute("SELECT lifecycle_status, is_current FROM analytics_recommendation_snapshots WHERE recommendation_family='WEAK_RELEASE_ATTENTION' ORDER BY id").fetchall()
                self.assertGreaterEqual(len(rows), 2)
                self.assertEqual(str(rows[-1]["lifecycle_status"]), "OPEN")
                self.assertEqual(int(rows[-1]["is_current"]), 1)
                self.assertEqual(str(rows[-2]["lifecycle_status"]), "SUPERSEDED")
            finally:
                conn.close()

    def test_preserve_acknowledged_or_dismissed_history(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="FULL_RECOMPUTE")
                rec = read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", current_only=True)[0]
                update_recommendation_lifecycle(conn, recommendation_id=int(rec["id"]), target_status="ACKNOWLEDGED")
                recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="TARGETED_RECOMPUTE")
                statuses = [str(r["lifecycle_status"]) for r in read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", current_only=False)]
                self.assertIn("ACKNOWLEDGED", statuses)
            finally:
                conn.close()

    def test_partial_recompute_preserves_successful_writes(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                recs = [
                    r for r in __import__("services.analytics_center.recommendation_core", fromlist=["synthesize_recommendations"]).synthesize_recommendations(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                    if r.recommendation_family == "PUBLISH_TIMING_SUGGESTION"
                ]
                recs = recs + recs
                with mock.patch("services.analytics_center.recommendation_runtime.synthesize_recommendations", return_value=recs), mock.patch(
                    "services.analytics_center.recommendation_runtime.persist_recommendation_snapshot", side_effect=[1, RuntimeError("boom")]
                ):
                    recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="PUBLISH_TIMING_SUGGESTION", recompute_mode="FULL_RECOMPUTE")
                run = conn.execute("SELECT run_state, recommendation_count FROM analytics_recommendation_runs ORDER BY id DESC LIMIT 1").fetchone()
                self.assertEqual(str(run["run_state"]), "PARTIAL")
                self.assertGreaterEqual(int(run["recommendation_count"]), 1)
            finally:
                conn.close()

    def test_read_filters_and_prioritized_queue_and_grouping(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                for fam in ("WEAK_RELEASE_ATTENTION", "PUBLISH_TIMING_SUGGESTION", "OPERATIONAL_REMEDIATION"):
                    recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family=fam, recompute_mode="FULL_RECOMPUTE")
                filtered = read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", lifecycle_status="OPEN", target_domain="PUBLISH", current_only=True)
                self.assertTrue(all(str(r["recommendation_family"]) == "WEAK_RELEASE_ATTENTION" for r in filtered))
                queue = list_prioritized_recommendation_queue(conn, scope_type="CHANNEL")
                self.assertGreater(len(queue), 0)
                for i in range(len(queue) - 1):
                    left = queue[i]
                    right = queue[i + 1]
                    self.assertGreaterEqual({"CRITICAL": 3, "WARNING": 2, "INFO": 1}[str(left["severity_class"])], {"CRITICAL": 3, "WARNING": 2, "INFO": 1}[str(right["severity_class"])])
                grouped_scope = group_recommendations(conn, by="scope")
                grouped_family = group_recommendations(conn, by="family")
                grouped_domain = group_recommendations(conn, by="target_domain")
                self.assertIn("CHANNEL", grouped_scope)
                self.assertIn("WEAK_RELEASE_ATTENTION", grouped_family)
                self.assertTrue(len(grouped_domain) > 0)
                inspected = inspect_recommendation(conn, recommendation_id=int(queue[0]["id"]))
                self.assertIn("next_action_hint", inspected["explainability_payload_json"])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
