from __future__ import annotations

import logging
import unittest
from unittest import mock

from services.analytics_center.recommendation_runtime import read_recommendations, recompute_recommendations, update_recommendation_lifecycle
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.recommendation_fixtures import seed_recommendation_inputs


class TestMf5RecommendationHardening(unittest.TestCase):
    def test_required_events_are_recorded(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="FULL_RECOMPUTE")
                recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="TARGETED_RECOMPUTE")
                with mock.patch("services.analytics_center.recommendation_runtime.synthesize_recommendations") as syn, mock.patch("services.analytics_center.recommendation_runtime.persist_recommendation_snapshot", side_effect=[1, RuntimeError("boom")]):
                    one = read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", current_only=True)[0]
                    from services.analytics_center.recommendation_core import RecommendationOutput
                    syn.return_value = [RecommendationOutput(
                        scope_type="CHANNEL", scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", issue_key="tmp-1",
                        title_text="t", summary_text="s", severity_class="WARNING", confidence_class="HIGH", target_domain="PUBLISH",
                        target_pointer_payload={"target_domain":"PUBLISH","scope_type":"CHANNEL","scope_ref":"darkwood-reverie"},
                        explainability_payload={"next_action_hint":"x"}, source_snapshot_refs=["snapshot:1"]
                    )] * 2
                    recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="INCREMENTAL_RECOMPUTE")
                rec = read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", current_only=True)[0]
                update_recommendation_lifecycle(conn, recommendation_id=int(rec["id"]), target_status="ACKNOWLEDGED")
                update_recommendation_lifecycle(conn, recommendation_id=int(rec["id"]), target_status="DISMISSED")
                events = {str(r["event_type"]) for r in conn.execute("SELECT event_type FROM analytics_recommendation_events").fetchall()}
                required = {
                    "MF5_RECOMMENDATION_RECOMPUTE_STARTED",
                    "MF5_RECOMMENDATION_RECOMPUTE_COMPLETED",
                    "MF5_RECOMMENDATION_CREATED",
                    "MF5_RECOMMENDATION_SUPERSEDED",
                    "MF5_RECOMMENDATION_ACKNOWLEDGED",
                    "MF5_RECOMMENDATION_DISMISSED",
                    "MF5_EXPLAINABILITY_PAYLOAD_ATTACHED",
                    "MF5_TARGET_POINTER_ATTACHED",
                    "MF5_RECOMMENDATION_RUN_PARTIAL_FAILURE_RECORDED",
                }
                self.assertTrue(required.issubset(events))
            finally:
                conn.close()

    def test_required_log_fields_present(self) -> None:
        with temp_env() as (_td, env), self.assertLogs("services.analytics_center.recommendation_runtime", level=logging.INFO) as logs:
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="FULL_RECOMPUTE")
            finally:
                conn.close()
        merged = "\n".join(logs.output)
        for field in (
            "recommendation_scope_type=",
            "recommendation_scope_ref=",
            "recommendation_family=",
            "target_domain=",
            "severity_class=",
            "confidence_class=",
            "lifecycle_status=",
            "recommendation_id=",
            "run_state=",
        ):
            self.assertIn(field, merged)

    def test_operator_observability_payload_is_readable(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seed_recommendation_inputs(conn)
                recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="FULL_RECOMPUTE")
                with mock.patch("services.analytics_center.recommendation_runtime.synthesize_recommendations") as syn, mock.patch("services.analytics_center.recommendation_runtime.persist_recommendation_snapshot", side_effect=[1, RuntimeError("boom")]):
                    one = read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", current_only=True)[0]
                    from services.analytics_center.recommendation_core import RecommendationOutput
                    syn.return_value = [RecommendationOutput(
                        scope_type="CHANNEL", scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", issue_key="tmp-1",
                        title_text="t", summary_text="s", severity_class="WARNING", confidence_class="HIGH", target_domain="PUBLISH",
                        target_pointer_payload={"target_domain":"PUBLISH","scope_type":"CHANNEL","scope_ref":"darkwood-reverie"},
                        explainability_payload={"next_action_hint":"x"}, source_snapshot_refs=["snapshot:1"]
                    )] * 2
                    recompute_recommendations(conn, recommendation_scope_type="CHANNEL", recommendation_scope_ref="darkwood-reverie", recommendation_family="WEAK_RELEASE_ATTENTION", recompute_mode="INCREMENTAL_RECOMPUTE")
                rec = read_recommendations(conn, recommendation_family="WEAK_RELEASE_ATTENTION", current_only=True)[0]
                self.assertIn("PUBLISH", str(rec["target_domain"]))
                self.assertIn("CRITICAL", str(rec["severity_class"]))
                self.assertIn("HIGH", str(rec["confidence_class"]))
                self.assertIn("next_action_hint", str(rec["explainability_payload_json"]))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
