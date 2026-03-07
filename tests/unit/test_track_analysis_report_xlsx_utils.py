from __future__ import annotations

import unittest

from services.track_analysis_report.xlsx_export import build_group_header_spans, sanitize_sheet_name


class TestTrackAnalysisReportXlsxUtils(unittest.TestCase):
    def test_sanitize_sheet_name_replaces_invalid_chars_and_truncates(self) -> None:
        raw_name = "My/Channel*Name:With?Invalid[Chars] And Very Long Suffix"
        sanitized = sanitize_sheet_name(raw_name)

        self.assertNotIn("/", sanitized)
        self.assertNotIn("*", sanitized)
        self.assertLessEqual(len(sanitized), 31)

    def test_sanitize_sheet_name_fallback_when_blank(self) -> None:
        self.assertEqual(sanitize_sheet_name("   "), "Sheet1")

    def test_build_group_header_spans_for_contiguous_groups(self) -> None:
        columns = [
            {"key": "a", "group": "g1"},
            {"key": "b", "group": "g1"},
            {"key": "c", "group": "g2"},
            {"key": "d", "group": "g3"},
            {"key": "e", "group": "g3"},
        ]

        spans = build_group_header_spans(columns)

        self.assertEqual(spans, [(1, 2, "g1"), (3, 3, "g2"), (4, 5, "g3")])


if __name__ == "__main__":
    unittest.main()
