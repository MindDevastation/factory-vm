from __future__ import annotations

import json
import unittest

from services.metadata import video_tag_preset_service as svc


class TestVideoTagPresetService(unittest.TestCase):
    def setUp(self) -> None:
        self.channel = {
            "slug": "darkwood-reverie",
            "display_name": "Darkwood Reverie",
            "kind": "LONG",
        }

    def test_parser_accepts_valid_placeholders(self) -> None:
        parsed = svc.parse_tag_item("{{ channel_display_name }} {{release_title}}")
        self.assertEqual(parsed.errors, [])
        self.assertEqual([t.value for t in parsed.tokens if t.kind == "var"], ["channel_display_name", "release_title"])

    def test_parser_rejects_invalid_syntax(self) -> None:
        parsed = svc.parse_tag_item("{{channel_display_name")
        self.assertIn("Unmatched '{{' in template", parsed.errors)

    def test_whitelist_rejects_forbidden_variables(self) -> None:
        result = svc.validate_preset_for_save(
            channel=self.channel,
            preset_name="Main",
            preset_body_json=json.dumps(["{{artist_name}}"]),
        )
        self.assertIn("MTV_TEMPLATE_VARIABLE_NOT_ALLOWED", [item["code"] for item in result.validation_errors])

    def test_preset_body_json_must_be_ordered_string_array(self) -> None:
        result = svc.validate_preset_for_save(
            channel=self.channel,
            preset_name="Main",
            preset_body_json=json.dumps(["ok", 123]),
        )
        self.assertIn("MTV_PRESET_BODY_ITEM_TYPE", [item["code"] for item in result.validation_errors])

    def test_release_title_usability_logic(self) -> None:
        missing = svc.preview_video_tag_preset(
            channel=self.channel,
            preset_body=["{{release_title}}"],
            release_row={"title": "  ", "planned_at": None},
        )
        self.assertEqual(missing.render_status, "PARTIAL")
        self.assertEqual(missing.missing_variables, ["release_title"])

        usable = svc.preview_video_tag_preset(
            channel=self.channel,
            preset_body=["{{release_title}}"],
            release_row={"title": "  Real Title  ", "planned_at": None},
        )
        self.assertEqual(usable.render_status, "FULL")
        self.assertEqual(usable.final_normalized_tags, ["Real Title"])

    def test_date_vars_missing_when_schedule_absent(self) -> None:
        result = svc.preview_video_tag_preset(
            channel=self.channel,
            preset_body=["{{release_year}}", "{{release_month_number}}", "{{release_day_number}}"],
            release_row={"title": "x", "planned_at": None},
        )
        self.assertEqual(result.render_status, "PARTIAL")
        self.assertEqual(result.missing_variables, ["release_day_number", "release_month_number", "release_year"])

    def test_normalization_trims_and_drops_empty_and_dedupes(self) -> None:
        result = svc.preview_video_tag_preset(
            channel=self.channel,
            preset_body=[" ambient ", "  ", "ambient", "Ambient"],
            release_row=None,
        )
        self.assertEqual(result.final_normalized_tags, ["ambient", "Ambient"])
        self.assertEqual(result.dropped_empty_items, ["  "])
        self.assertEqual(result.removed_duplicates, ["ambient"])

    def test_per_item_500_char_enforced(self) -> None:
        result = svc.preview_video_tag_preset(
            channel=self.channel,
            preset_body=["x" * 501],
            release_row=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        self.assertIn("MTV_TAG_ITEM_TOO_LONG", [item["code"] for item in result.validation_errors])

    def test_total_count_enforcement(self) -> None:
        result = svc.preview_video_tag_preset(
            channel=self.channel,
            preset_body=[f"tag-{idx}" for idx in range(501)],
            release_row=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        self.assertIn("MTV_TAG_COUNT_EXCEEDED", [item["code"] for item in result.validation_errors])

    def test_total_combined_size_enforcement(self) -> None:
        result = svc.preview_video_tag_preset(
            channel=self.channel,
            preset_body=["x" * 3000, "y" * 2500],
            release_row=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        self.assertIn("MTV_TAG_TOTAL_CHARS_EXCEEDED", [item["code"] for item in result.validation_errors])

    def test_meaningless_empty_list_rejected(self) -> None:
        result = svc.validate_preset_for_save(
            channel=self.channel,
            preset_name="Main",
            preset_body_json=json.dumps([" ", "  "]),
        )
        codes = [item["code"] for item in result.validation_errors]
        self.assertIn("MTV_PRESET_EMPTY_AFTER_NORMALIZATION", codes)


if __name__ == "__main__":
    unittest.main()
