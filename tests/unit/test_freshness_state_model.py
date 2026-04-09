from __future__ import annotations

import unittest

from services.analytics_center.freshness_state_model import (
    ANALYZER_COVERAGE_STATES,
    normalize_coverage_state,
    summarize_coverage_states,
)


class TestFreshnessStateModel(unittest.TestCase):
    def test_normalize_includes_required_visibility_states(self) -> None:
        self.assertEqual(
            ANALYZER_COVERAGE_STATES,
            ("MISSING", "PARTIAL", "PERMISSION_LIMITED", "STALE", "REFRESHED"),
        )
        self.assertEqual(normalize_coverage_state("NOT_YET_SYNCED"), "MISSING")
        self.assertEqual(normalize_coverage_state("PARTIAL"), "PARTIAL")
        self.assertEqual(normalize_coverage_state("PERMISSION_LIMITED"), "PERMISSION_LIMITED")
        self.assertEqual(normalize_coverage_state("STALE"), "STALE")
        self.assertEqual(normalize_coverage_state("FRESH"), "REFRESHED")

    def test_summary_exposes_distinct_state_buckets(self) -> None:
        freshness, coverage = summarize_coverage_states(
            source_states={
                "a": "NOT_YET_SYNCED",
                "b": "PARTIAL",
                "c": "PERMISSION_LIMITED",
                "d": "STALE",
                "e": "FRESH",
            }
        )
        self.assertEqual(freshness["status"], "PERMISSION_LIMITED")
        self.assertIn("a", coverage["missing_sources"])
        self.assertIn("b", coverage["partial_sources"])
        self.assertIn("c", coverage["permission_limited_sources"])
        self.assertIn("d", coverage["stale_sources"])
        self.assertIn("e", coverage["refreshed_sources"])
