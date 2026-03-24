from __future__ import annotations

import unittest

from services.factory_api import planner


class TestPlannerListReadinessSurfaceHelpers(unittest.TestCase):
    def test_parse_readiness_status_filter_single_and_csv(self) -> None:
        self.assertEqual(planner._parse_readiness_status_filter("NOT_READY"), {"NOT_READY"})
        self.assertEqual(
            planner._parse_readiness_status_filter("NOT_READY, BLOCKED"),
            {"NOT_READY", "BLOCKED"},
        )

    def test_parse_readiness_status_filter_invalid(self) -> None:
        with self.assertRaises(ValueError):
            planner._parse_readiness_status_filter("NOPE")

    def test_parse_readiness_problem_filter_and_invalid(self) -> None:
        self.assertEqual(
            planner._parse_readiness_problem_filter("attention_required"),
            {"NOT_READY", "BLOCKED"},
        )
        self.assertEqual(planner._parse_readiness_problem_filter("blocked_only"), {"BLOCKED"})
        self.assertEqual(planner._parse_readiness_problem_filter("ready_only"), {"READY_FOR_MATERIALIZATION"})
        with self.assertRaises(ValueError):
            planner._parse_readiness_problem_filter("nope")

    def test_readiness_rank_attention_first_and_ready_first(self) -> None:
        self.assertLess(
            planner._readiness_rank("BLOCKED", readiness_priority="attention_first"),
            planner._readiness_rank("NOT_READY", readiness_priority="attention_first"),
        )
        self.assertLess(
            planner._readiness_rank("NOT_READY", readiness_priority="attention_first"),
            planner._readiness_rank("READY_FOR_MATERIALIZATION", readiness_priority="attention_first"),
        )
        self.assertLess(
            planner._readiness_rank("READY_FOR_MATERIALIZATION", readiness_priority="ready_first"),
            planner._readiness_rank("NOT_READY", readiness_priority="ready_first"),
        )
        self.assertLess(
            planner._readiness_rank("NOT_READY", readiness_priority="ready_first"),
            planner._readiness_rank("BLOCKED", readiness_priority="ready_first"),
        )

    def test_build_readiness_summary_excludes_unavailable_and_sets_attention_count(self) -> None:
        scope_ids = [1, 2, 3, 4]
        readiness_map = {
            1: {"aggregate_status": "READY_FOR_MATERIALIZATION", "computed_at": "2026-03-24T12:30:00Z"},
            2: {"aggregate_status": "NOT_READY", "computed_at": "2026-03-24T12:31:00Z"},
            3: {"aggregate_status": "BLOCKED", "computed_at": "2026-03-24T12:32:00Z"},
        }
        summary = planner._build_readiness_summary(scope_ids, readiness_map, unavailable_ids={4})
        self.assertEqual(summary["scope_total"], 4)
        self.assertEqual(summary["ready_for_materialization"], 1)
        self.assertEqual(summary["not_ready"], 1)
        self.assertEqual(summary["blocked"], 1)
        self.assertEqual(summary["unavailable"], 1)
        self.assertEqual(summary["attention_count"], 2)
        self.assertEqual(summary["computed_at"], "2026-03-24T12:32:00Z")


    def test_build_readiness_summary_computed_at_none_when_all_rows_unavailable(self) -> None:
        scope_ids = [10, 11]
        readiness_map = {}
        summary = planner._build_readiness_summary(scope_ids, readiness_map, unavailable_ids={10, 11})
        self.assertEqual(summary["scope_total"], 2)
        self.assertEqual(summary["ready_for_materialization"], 0)
        self.assertEqual(summary["not_ready"], 0)
        self.assertEqual(summary["blocked"], 0)
        self.assertEqual(summary["unavailable"], 2)
        self.assertEqual(summary["attention_count"], 0)
        self.assertIsNone(summary["computed_at"])

    def test_unavailable_payload_shape(self) -> None:
        payload = planner._readiness_unavailable_payload()
        self.assertEqual(payload["aggregate_status"], None)
        self.assertEqual(payload["error"]["code"], "PRS_READINESS_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
