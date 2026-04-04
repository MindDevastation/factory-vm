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
        self.assertEqual(classify_action_class(action="retry"), "MUTATE_RETRY")
        self.assertEqual(pattern_family_for_action(action="retry"), "PREVIEW_CONFIRM_EXECUTE")
        self.assertEqual(pattern_family_for_action(action="refresh"), "INLINE_SAFE_ACTION")

    def test_result_class_mapping(self) -> None:
        self.assertEqual(classify_result_class(outcome="ok"), "SUCCESS")
        self.assertEqual(classify_result_class(outcome="partial"), "PARTIAL")
        self.assertEqual(classify_result_class(outcome="stale"), "STALE")

    def test_stale_conflict_classification(self) -> None:
        self.assertEqual(classify_stale_conflict(expected_version="job:1", actual_version="job:2"), "STALE")
        self.assertEqual(classify_stale_conflict(expected_version="job:1", actual_version="release:2"), "CONFLICT")

    def test_representative_surfaces_matrix(self) -> None:
        surfaces = representative_surfaces_matrix()
        names = {item["surface"] for item in surfaces}
        self.assertTrue({"planner", "publish", "visuals", "analytics", "ops_recovery"}.issubset(names))


if __name__ == "__main__":
    unittest.main()
