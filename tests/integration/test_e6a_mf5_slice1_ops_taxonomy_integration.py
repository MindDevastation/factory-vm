from __future__ import annotations

import unittest

from services.telegram_inbox import build_confirmation_envelope


class TestE6AMf5Slice1OpsTaxonomyIntegration(unittest.TestCase):
    def test_out_of_scope_action_unavailable(self) -> None:
        with self.assertRaises(ValueError):
            build_confirmation_envelope(action="dangerous_reset_db", confirm=True, reason="x", request_id="r")


if __name__ == "__main__":
    unittest.main()
