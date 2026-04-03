from __future__ import annotations

import unittest

from services.telegram_operator import classify_stale_conflict, is_callback_expired, render_operator_safe_result


class TestE6AMf6Slice2StaleExpiredUnit(unittest.TestCase):
    def test_expiry_and_stale_classifier(self) -> None:
        self.assertTrue(bool(is_callback_expired(expires_at="2020-01-01T00:00:00Z")))
        self.assertEqual(classify_stale_conflict(expected_state="A", current_state="B"), "STALE")
        self.assertEqual(classify_stale_conflict(expected_state="A", current_state="A"), "CURRENT")

    def test_operator_safe_renderer(self) -> None:
        payload = render_operator_safe_result(code="E6A_TARGET_STALE", message="state changed", detail="open web")
        self.assertEqual(payload["code"], "E6A_TARGET_STALE")


if __name__ == "__main__":
    unittest.main()
