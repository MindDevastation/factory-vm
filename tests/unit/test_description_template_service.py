from __future__ import annotations

import unittest

from services.metadata import description_template_service as svc


class TestDescriptionTemplateService(unittest.TestCase):
    def setUp(self) -> None:
        self.channel = {
            "slug": "darkwood-reverie",
            "display_name": "Darkwood Reverie",
            "kind": "LONG",
        }

    def test_parser_accepts_valid_placeholders_with_inner_whitespace(self) -> None:
        parsed = svc.parse_template("{{ channel_display_name }}\n{{release_title}}")
        self.assertEqual(parsed.errors, [])
        vars_used = [tok.value for tok in parsed.tokens if tok.kind == "var"]
        self.assertEqual(vars_used, ["channel_display_name", "release_title"])

    def test_parser_rejects_invalid_syntax(self) -> None:
        parsed = svc.parse_template("{{channel_display_name")
        self.assertIn("Unmatched '{{' in template", parsed.errors)

    def test_whitelist_rejects_forbidden_variable(self) -> None:
        result = svc.validate_template_for_save(
            channel=self.channel,
            template_name="Main",
            template_body="{{artist_name}}",
        )
        codes = [item["code"] for item in result.validation_errors]
        self.assertIn("MTD_TEMPLATE_VARIABLE_NOT_ALLOWED", codes)

    def test_multiline_normalization_preserves_paragraph_structure(self) -> None:
        normalized = svc.normalize_multiline("Line one  \r\n\r\nLine   two\r\n")
        self.assertEqual(normalized, "Line one\n\nLine   two")

    def test_tabs_rejected(self) -> None:
        result = svc.preview_description_template(
            channel=self.channel,
            template_body="Hello\t{{channel_display_name}}",
            release_row=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        codes = [item["code"] for item in result.validation_errors]
        self.assertIn("MTD_TEMPLATE_TAB_NOT_ALLOWED", codes)

    def test_control_character_restrictions_enforced(self) -> None:
        result = svc.preview_description_template(
            channel=self.channel,
            template_body="A\x00B",
            release_row=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        codes = [item["code"] for item in result.validation_errors]
        self.assertIn("MTD_TEMPLATE_CONTROL_CHAR", codes)

    def test_release_title_missing_marker_without_release_context(self) -> None:
        result = svc.preview_description_template(
            channel=self.channel,
            template_body="{{release_title}}",
            release_row=None,
        )
        self.assertEqual(result.render_status, "PARTIAL")
        self.assertEqual(result.missing_variables, ["release_title"])
        self.assertEqual(result.rendered_description_preview, "<<missing:release_title>>")

    def test_release_date_missing_markers_when_schedule_absent(self) -> None:
        result = svc.preview_description_template(
            channel=self.channel,
            template_body="{{release_year}}-{{release_month_number}}-{{release_day_number}}",
            release_row={"id": 1, "title": "X", "planned_at": None},
        )
        self.assertEqual(result.render_status, "PARTIAL")
        self.assertEqual(
            result.missing_variables,
            ["release_day_number", "release_month_number", "release_year"],
        )

    def test_estimated_length_validation_enforces_max(self) -> None:
        result = svc.validate_template_for_save(
            channel=self.channel,
            template_name="Main",
            template_body=("X" * 4001) + "{{release_title}}",
        )
        codes = [item["code"] for item in result.validation_errors]
        self.assertIn("MTD_RENDER_TOO_LONG", codes)

    def test_whitespace_only_after_normalization_rejected(self) -> None:
        result = svc.preview_description_template(
            channel=self.channel,
            template_body=" \n  \n",
            release_row=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        codes = [item["code"] for item in result.validation_errors]
        self.assertIn("MTD_RENDER_EMPTY", codes)


if __name__ == "__main__":
    unittest.main()
