from __future__ import annotations

import unittest

from services.common import db as dbm
from services.factory_api.ui_jobs_filters import (
    filter_jobs,
    parse_statuses_from_query,
    serialize_statuses_to_query,
)


class TestUiJobsFiltersHelpers(unittest.TestCase):
    def test_parse_statuses_from_query_reads_valid_comma_list(self):
        source_statuses = list(dbm.UI_JOB_STATES)
        first = source_statuses[0]
        failed = "FAILED"

        parsed = parse_statuses_from_query(f"{first},{failed}", source_statuses)

        self.assertEqual(parsed, {first, failed})

    def test_parse_statuses_from_query_ignores_unknown_statuses(self):
        source_statuses = list(dbm.UI_JOB_STATES)
        first = source_statuses[0]

        parsed = parse_statuses_from_query(f"{first},NOPE", source_statuses)

        self.assertEqual(parsed, {first})

    def test_parse_statuses_from_query_empty_or_missing_returns_empty_set(self):
        source_statuses = list(dbm.UI_JOB_STATES)

        self.assertEqual(parse_statuses_from_query("", source_statuses), set())
        self.assertEqual(parse_statuses_from_query(None, source_statuses), set())

    def test_serialize_statuses_to_query_uses_backend_source_order(self):
        source_statuses = list(dbm.UI_JOB_STATES)
        selected = {"FAILED", "PUBLISHED"}

        serialized = serialize_statuses_to_query(selected, source_statuses)

        self.assertEqual(serialized, "FAILED,PUBLISHED")

    def test_filter_jobs_applies_multi_select_or_semantics(self):
        jobs = [
            {"id": "1", "status": "DRAFT"},
            {"id": "2", "status": "RENDERING"},
            {"id": "3", "status": "FAILED"},
        ]

        filtered = filter_jobs(jobs, {"FAILED", "DRAFT"})

        self.assertEqual(filtered, [{"id": "1", "status": "DRAFT"}, {"id": "3", "status": "FAILED"}])

    def test_filter_jobs_empty_selection_returns_all_jobs(self):
        jobs = [
            {"id": "1", "status": "DRAFT"},
            {"id": "2", "status": "RENDERING"},
            {"id": "3", "status": "FAILED"},
        ]

        self.assertIs(filter_jobs(jobs, set()), jobs)
        self.assertIs(filter_jobs(jobs, None), jobs)

    def test_query_roundtrip_preserves_selected_statuses_in_source_order(self):
        source_statuses = list(dbm.UI_JOB_STATES)
        raw = "PUBLISHED,FAILED,UNKNOWN"

        selected = parse_statuses_from_query(raw, source_statuses)
        serialized = serialize_statuses_to_query(selected, source_statuses)

        self.assertEqual(selected, {"PUBLISHED", "FAILED"})
        self.assertEqual(serialized, "FAILED,PUBLISHED")


if __name__ == "__main__":
    unittest.main()
