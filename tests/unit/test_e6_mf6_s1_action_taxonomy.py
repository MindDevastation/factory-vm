from __future__ import annotations

import unittest

from services.factory_api.action_taxonomy import (
    classify_action_class,
    classify_result_class,
    classify_stale_conflict,
    pattern_family_for_action,
    representative_surfaces_matrix,
)


class TestE6Mf6S1ActionTaxonomy(unittest.TestCase):
    def test_action_class_and_pattern_mapping(self) -> None:
        self.assertEqual(classify_action_class(action="retry"), "LOW_RISK_MUTATE")
        self.assertEqual(classify_action_class(action="approve"), "GUARDED_MUTATE")
        self.assertEqual(pattern_family_for_action(action="retry"), "PREVIEW_TO_APPLY")
        self.assertEqual(pattern_family_for_action(action="cancel"), "DIRECT_MUTATE_WITH_CONFIRMATION")

    def test_result_class_mapping(self) -> None:
        self.assertEqual(classify_result_class(outcome="ok"), "SUCCEEDED")
        self.assertEqual(classify_result_class(outcome="partial"), "PARTIAL")
        self.assertEqual(classify_result_class(outcome="conflict"), "DENIED")
        self.assertEqual(classify_result_class(outcome="failed"), "FAILED")

    def test_stale_conflict_classification(self) -> None:
        self.assertEqual(classify_stale_conflict(expected_version="job:1", actual_version="job:2"), "STALE")
        self.assertEqual(classify_stale_conflict(expected_version="job:1", actual_version="release:2"), "CONFLICT")

    def test_representative_surfaces_matrix(self) -> None:
        surfaces = representative_surfaces_matrix()
        names = {item["surface"] for item in surfaces}
        self.assertTrue({"planner", "publish", "visuals", "analytics", "ops_recovery"}.issubset(names))


if __name__ == "__main__":
    unittest.main()
