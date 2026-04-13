from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.recommendation_core import persist_recommendation_snapshot, synthesize_recommendations
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.recommendation_fixtures import seed_recommendation_inputs


class TestMf5RecommendationCoreIntegration(unittest.TestCase):
    def test_synthesize_recommendations_from_kpi_prediction_and_anomaly_risk(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                recs = synthesize_recommendations(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                self.assertGreater(len(recs), 0)
                families = {r.recommendation_family for r in recs}
                self.assertIn("OPERATIONAL_REMEDIATION", families)
                self.assertIn("CONTENT_PACKAGING_SUGGESTION", families)
                self.assertIn("WEAK_RELEASE_ATTENTION", families)
                self.assertIn("TITLE_METADATA_IMPROVEMENT", families)
                self.assertIn("VISUAL_IMPROVEMENT", families)
                self.assertIn("CONTENT_PLANNING_SUGGESTION", families)
            finally:
                conn.close()

    def test_write_recommendation_with_target_domain_pointer(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                rec = synthesize_recommendations(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")[0]
                rec_id = persist_recommendation_snapshot(conn, recommendation=rec)
                row = conn.execute("SELECT target_domain, target_pointer_payload_json FROM analytics_recommendation_snapshots WHERE id = ?", (rec_id,)).fetchone()
                self.assertEqual(str(row["target_domain"]), rec.target_domain)
                self.assertIn("scope_ref", str(row["target_pointer_payload_json"]))
            finally:
                conn.close()

    def test_reject_recommendation_without_explainability_payload(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                rec = synthesize_recommendations(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")[0]
                rec = rec.__class__(**{**rec.__dict__, "explainability_payload": {}})
                with self.assertRaises(AnalyticsDomainError):
                    persist_recommendation_snapshot(conn, recommendation=rec)
            finally:
                conn.close()

    def test_reject_recommendation_without_next_action_hint(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                rec = synthesize_recommendations(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")[0]
                payload = dict(rec.explainability_payload)
                payload["next_action_hint"] = ""
                rec = rec.__class__(**{**rec.__dict__, "explainability_payload": payload})
                with self.assertRaises(AnalyticsDomainError):
                    persist_recommendation_snapshot(conn, recommendation=rec)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
