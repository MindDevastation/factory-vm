from __future__ import annotations

import unittest

from services.telegram_operator import classify_stale_conflict, render_operator_safe_result


class TestE6AMf6Slice2StaleExpiredIntegration(unittest.TestCase):
    def test_result_semantics_for_stale_and_already_applied(self) -> None:
        stale = classify_stale_conflict(expected_state="pending", current_state="done")
        done = classify_stale_conflict(expected_state="done", current_state="done", already_applied=True)
        s_payload = render_operator_safe_result(code="E6A_TARGET_STALE", message="state changed")
        d_payload = render_operator_safe_result(code="E6A_ALREADY_APPLIED", message="already applied")
        self.assertEqual(stale, "STALE")
        self.assertEqual(done, "ALREADY_APPLIED")
        self.assertEqual(s_payload["code"], "E6A_TARGET_STALE")
        self.assertEqual(d_payload["code"], "E6A_ALREADY_APPLIED")


if __name__ == "__main__":
    unittest.main()
