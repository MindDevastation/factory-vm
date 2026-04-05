from __future__ import annotations

import unittest

from services.factory_api.ui_state_templates import (
    BLOCKED,
    PARTIAL,
    STALE,
    SUCCESS,
    classify_state_template,
    state_template_catalog,
)


class TestE6Mf2S3UiStateTemplates(unittest.TestCase):
    def test_catalog_includes_required_state_templates(self) -> None:
        catalog = state_template_catalog()
        self.assertIn("LOADING", catalog)
        self.assertIn("EMPTY", catalog)
        self.assertIn("STALE", catalog)
        self.assertIn("ERROR", catalog)
        self.assertIn("SUCCESS", catalog)
        self.assertIn("PARTIAL", catalog)
        self.assertIn("BLOCKED", catalog)

    def test_stale_is_not_blocked(self) -> None:
        state = classify_state_template(has_data=True, has_error=False, is_stale=True, is_blocked=False, is_partial=False)
        self.assertEqual(state, STALE)
        self.assertNotEqual(state, BLOCKED)

    def test_partial_is_not_success(self) -> None:
        state = classify_state_template(has_data=True, has_error=False, is_stale=False, is_blocked=False, is_partial=True)
        self.assertEqual(state, PARTIAL)
        self.assertNotEqual(state, SUCCESS)


if __name__ == "__main__":
    unittest.main()
