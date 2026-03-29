from __future__ import annotations

import unittest

from services.factory_api.publish_policy import (
    ALLOWED_PUBLISH_MODES,
    ALLOWED_PUBLISH_REASON_CODES,
    ALLOWED_TARGET_VISIBILITY,
    validate_publish_mode,
    validate_publish_reason_code,
    validate_target_visibility,
)


class TestPublishPolicyValidation(unittest.TestCase):
    def test_publish_mode_values(self) -> None:
        for value in ALLOWED_PUBLISH_MODES:
            self.assertEqual(validate_publish_mode(value), value)
        self.assertIsNone(validate_publish_mode(None))
        for bad in ("", "AUTO", "manual", "hold "):
            with self.assertRaises(ValueError):
                validate_publish_mode(bad)

    def test_target_visibility_values(self) -> None:
        for value in ALLOWED_TARGET_VISIBILITY:
            self.assertEqual(validate_target_visibility(value), value)
        self.assertIsNone(validate_target_visibility(None))
        for bad in ("", "private", "PUBLIC"):
            with self.assertRaises(ValueError):
                validate_target_visibility(bad)

    def test_reason_code_common_canonical_enum_and_unset(self) -> None:
        for value in ALLOWED_PUBLISH_REASON_CODES:
            self.assertEqual(validate_publish_reason_code(value), value)
        self.assertIsNone(validate_publish_reason_code(None))
        with self.assertRaises(ValueError):
            validate_publish_reason_code("")
        with self.assertRaises(ValueError):
            validate_publish_reason_code("not_allowed")


if __name__ == "__main__":
    unittest.main()
