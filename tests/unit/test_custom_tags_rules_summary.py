from __future__ import annotations

import unittest

from services.custom_tags.catalog_service import build_rules_summary


class TestCustomTagsRulesSummary(unittest.TestCase):
    def test_no_rules_summary(self) -> None:
        self.assertEqual(build_rules_summary([]), "No rules")

    def test_compact_active_rules_summary(self) -> None:
        rules = [
            {"source_path": "track_features.payload_json.voice_flag", "operator": "equals", "value_json": "false"},
            {"source_path": "track_features.payload_json.speech_flag", "operator": "equals", "value_json": "false"},
        ]
        self.assertEqual(
            build_rules_summary(rules),
            "2 active rules: voice_flag=false; speech_flag=false",
        )

    def test_summary_truncates_after_three_conditions(self) -> None:
        rules = [
            {"source_path": "a.one", "operator": "equals", "value_json": '"x"'},
            {"source_path": "a.two", "operator": "equals", "value_json": '"y"'},
            {"source_path": "a.three", "operator": "equals", "value_json": '"z"'},
            {"source_path": "a.four", "operator": "equals", "value_json": '"w"'},
        ]
        self.assertEqual(
            build_rules_summary(rules),
            "4 active rules: one=x; two=y; three=z; …",
        )


if __name__ == "__main__":
    unittest.main()
