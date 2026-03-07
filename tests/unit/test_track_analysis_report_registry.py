from __future__ import annotations

import unittest

from services.track_analysis_report.flatten import flatten_value, resolve_source_path
from services.track_analysis_report.registry import COLUMN_GROUPS, COLUMN_REGISTRY, VALID_FLATTEN_RULES


class TestTrackAnalysisReportRegistry(unittest.TestCase):
    def test_registry_keys_are_unique(self) -> None:
        keys = [entry["key"] for entry in COLUMN_REGISTRY]
        self.assertEqual(len(keys), len(set(keys)))

    def test_registry_groups_and_rules_are_valid(self) -> None:
        valid_groups = set(COLUMN_GROUPS)
        for entry in COLUMN_REGISTRY:
            self.assertIn(entry["group"], valid_groups)
            self.assertIn(entry["flatten"], VALID_FLATTEN_RULES)

    def test_flatten_scalar_direct(self) -> None:
        self.assertEqual(flatten_value(3.14, "direct"), 3.14)

    def test_flatten_scalar_array_join_csv(self) -> None:
        self.assertEqual(flatten_value(["a", "b", 3], "join_csv"), "a, b, 3")
        self.assertEqual(flatten_value([], "join_csv"), "")

    def test_flatten_nested_object_json_string(self) -> None:
        value = {"b": 2, "a": {"x": 1}}
        self.assertEqual(flatten_value(value, "json_string"), '{"a": {"x": 1}, "b": 2}')

    def test_unix_ts_iso_conversion(self) -> None:
        self.assertEqual(flatten_value(0, "unix_ts_iso"), "1970-01-01T00:00:00Z")

    def test_missing_path_returns_none(self) -> None:
        row_sources = {
            "tracks": {"track_id": "t1"},
            "features": {"payload_json": {"analysis_status": "ok"}},
            "tags": {"payload_json": {}},
            "scores": {"payload_json": {}},
        }
        self.assertIsNone(resolve_source_path(row_sources, "features", "payload_json.not_found"))
        self.assertIsNone(resolve_source_path(row_sources, "unknown", "payload_json.anything"))


if __name__ == "__main__":
    unittest.main()
