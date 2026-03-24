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

    def test_unavailable_payload_shape(self) -> None:
        payload = planner._readiness_unavailable_payload()
        self.assertEqual(payload["aggregate_status"], None)
        self.assertEqual(payload["error"]["code"], "PRS_READINESS_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
