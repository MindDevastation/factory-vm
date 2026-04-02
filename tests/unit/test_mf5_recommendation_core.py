from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.recommendation_core import (
    RecommendationOutput,
    build_explainability_payload,
    build_target_domain_pointer,
    persist_recommendation_snapshot,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestMf5RecommendationCore(unittest.TestCase):
    def test_build_explainability_requires_fields(self) -> None:
        with self.assertRaises(AnalyticsDomainError):
            build_explainability_payload(
                primary_reason_code="",
                primary_reason_text="x",
                supporting_signals_json=[],
                comparison_context_json={},
                confidence_class="HIGH",
                severity_class="WARNING",
                next_action_hint="do it",
                target_domain="PUBLISH",
                target_pointer_payload_json={"scope_ref": "a"},
                source_snapshot_refs_json=["snapshot:1"],
            )

    def test_build_target_pointer_validates_domain_and_scope(self) -> None:
        pointer = build_target_domain_pointer(target_domain="PUBLISH", scope_type="CHANNEL", scope_ref="darkwood-reverie")
        self.assertEqual(pointer["target_domain"], "PUBLISH")
        with self.assertRaises(AnalyticsDomainError):
            build_target_domain_pointer(target_domain="BAD", scope_type="CHANNEL", scope_ref="x")

    def test_persist_requires_next_action_and_sources(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                explainability = build_explainability_payload(
                    primary_reason_code="X",
                    primary_reason_text="Because",
                    supporting_signals_json=[{"x": 1}],
                    comparison_context_json={"base": "a"},
                    confidence_class="HIGH",
                    severity_class="WARNING",
                    next_action_hint="open publish",
                    target_domain="PUBLISH",
                    target_pointer_payload_json={"target_domain": "PUBLISH", "scope_type": "CHANNEL", "scope_ref": "darkwood-reverie"},
                    source_snapshot_refs_json=["snapshot:1"],
                )
                rec = RecommendationOutput(
                    scope_type="CHANNEL",
                    scope_ref="darkwood-reverie",
                    recommendation_family="PUBLISH_TIMING_SUGGESTION",
                    issue_key="k1",
                    title_text="Title",
                    summary_text="Summary",
                    severity_class="WARNING",
                    confidence_class="HIGH",
                    target_domain="PUBLISH",
                    target_pointer_payload={"target_domain": "PUBLISH", "scope_type": "CHANNEL", "scope_ref": "darkwood-reverie"},
                    explainability_payload=explainability,
                    source_snapshot_refs=["snapshot:1"],
                )
                rec_id = persist_recommendation_snapshot(conn, recommendation=rec)
                self.assertGreater(rec_id, 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
