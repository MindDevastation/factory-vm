from __future__ import annotations

import unittest
from datetime import date

from services.metadata import title_template_service


class TestTitleTemplateService(unittest.TestCase):
    def setUp(self) -> None:
        self.channel = {
            "slug": "darkwood-reverie",
            "display_name": "Darkwood Reverie",
            "kind": "LONG",
        }

    def test_accepts_valid_placeholders(self) -> None:
        result = title_template_service.preview_title_template(
            channel=self.channel,
            template_body="{{channel_display_name}} - {{ release_year }}",
            release_date=date(2026, 4, 9),
        )
        self.assertEqual(result.render_status, "FULL")
        self.assertEqual(result.rendered_title, "Darkwood Reverie - 2026")
        self.assertEqual(list(result.validation_errors), [])

    def test_rejects_invalid_syntax(self) -> None:
        result = title_template_service.preview_title_template(
            channel=self.channel,
            template_body="{{channel_display_name",
            release_date=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        self.assertTrue(any(err["code"] == "MTB_TEMPLATE_SYNTAX" for err in result.validation_errors))

    def test_rejects_unknown_variables(self) -> None:
        result = title_template_service.preview_title_template(
            channel=self.channel,
            template_body="{{channel_display_name}} {{not_allowed}}",
            release_date=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        self.assertTrue(any(err["code"] == "MTB_UNKNOWN_VARIABLE" for err in result.validation_errors))

    def test_marks_missing_release_date_variables_explicitly(self) -> None:
        result = title_template_service.preview_title_template(
            channel=self.channel,
            template_body="{{channel_slug}} {{release_year}}",
            release_date=None,
        )
        self.assertEqual(result.render_status, "PARTIAL")
        self.assertEqual(list(result.missing_variables), ["release_year"])
        self.assertIn("<<missing:release_year>>", str(result.rendered_title))

    def test_enforces_control_char_ban(self) -> None:
        result = title_template_service.preview_title_template(
            channel=self.channel,
            template_body="{{channel_display_name}}\n{{channel_slug}}",
            release_date=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        self.assertTrue(any(err["code"] == "MTB_TITLE_CONTROL_CHARS" for err in result.validation_errors))

    def test_enforces_empty_result_guard(self) -> None:
        blank_channel = {
            "slug": "",
            "display_name": "   ",
            "kind": "",
        }
        result = title_template_service.preview_title_template(
            channel=blank_channel,
            template_body="{{ channel_display_name }}",
            release_date=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        self.assertTrue(any(err["code"] == "MTB_TITLE_EMPTY" for err in result.validation_errors))

    def test_enforces_full_render_max_length(self) -> None:
        long_text = "x" * 101
        result = title_template_service.preview_title_template(
            channel=self.channel,
            template_body=long_text,
            release_date=None,
        )
        self.assertEqual(result.render_status, "ERROR")
        self.assertTrue(any(err["code"] == "MTB_TITLE_TOO_LONG" for err in result.validation_errors))

    def test_whitespace_normalization_deterministic(self) -> None:
        result = title_template_service.preview_title_template(
            channel=self.channel,
            template_body="  {{channel_display_name}}    {{channel_slug}}  ",
            release_date=None,
        )
        self.assertEqual(result.render_status, "FULL")
        self.assertEqual(result.rendered_title, "Darkwood Reverie darkwood-reverie")


if __name__ == "__main__":
    unittest.main()
