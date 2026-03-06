from __future__ import annotations

import unittest

from services.factory_api.ui_jobs_filters import (
    filter_jobs,
    parse_statuses_from_query,
    serialize_statuses_to_query,
)


class TestUiJobsFiltersHelpers(unittest.TestCase):
    def test_parse_statuses_from_query_reads_valid_comma_list(self):
        source_statuses = ["PLANNED", "RUNNING", "FAILED"]

        parsed = parse_statuses_from_query("PLANNED,FAILED", source_statuses)

        self.assertEqual(parsed, {"PLANNED", "FAILED"})

    def test_parse_statuses_from_query_ignores_unknown_statuses(self):
        source_statuses = ["PLANNED", "RUNNING", "FAILED"]

        parsed = parse_statuses_from_query("PLANNED,NOPE", source_statuses)

        self.assertEqual(parsed, {"PLANNED"})

    def test_parse_statuses_from_query_empty_or_missing_returns_empty_set(self):
        source_statuses = ["PLANNED", "RUNNING", "FAILED"]

        self.assertEqual(parse_statuses_from_query("", source_statuses), set())
        self.assertEqual(parse_statuses_from_query(None, source_statuses), set())

    def test_serialize_statuses_to_query_uses_backend_source_order(self):
        source_statuses = ["PLANNED", "RUNNING", "FAILED", "DONE"]

        serialized = serialize_statuses_to_query({"DONE", "PLANNED"}, source_statuses)

        self.assertEqual(serialized, "PLANNED,DONE")

    def test_filter_jobs_returns_matching_subset(self):
        jobs = [
            {"id": "1", "status": "PLANNED"},
            {"id": "2", "status": "RUNNING"},
            {"id": "3", "status": "FAILED"},
        ]

        filtered = filter_jobs(jobs, {"FAILED", "PLANNED"})

        self.assertEqual(filtered, [{"id": "1", "status": "PLANNED"}, {"id": "3", "status": "FAILED"}])


if __name__ == "__main__":
    unittest.main()
