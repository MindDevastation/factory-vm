from __future__ import annotations

import unittest

from services.telegram_inbox import build_confirmation_envelope, ops_action_policy


class TestE6AMf5Slice1OpsTaxonomyUnit(unittest.TestCase):
    def test_policy_matrix_and_out_of_scope(self) -> None:
        self.assertTrue(bool(ops_action_policy("retry")["enabled"]))
        self.assertFalse(bool(ops_action_policy("dangerous_reset_db")["enabled"]))

    def test_confirmation_envelope(self) -> None:
        env = build_confirmation_envelope(action="retry", confirm=True, reason="retry now", request_id="mf5-s1")
        self.assertEqual(env["request_id"], "mf5-s1")
        with self.assertRaises(ValueError):
            build_confirmation_envelope(action="retry", confirm=False, reason="x", request_id="r1")


if __name__ == "__main__":
    unittest.main()
