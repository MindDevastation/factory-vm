from __future__ import annotations

import unittest

from services.telegram_inbox import build_status_fixture


class TestE6AMf4Slice1ReadViewFoundationIntegration(unittest.TestCase):
    def test_read_view_fixture_is_compact_and_has_freshness(self) -> None:
        view = build_status_fixture(kind="readiness")
        self.assertTrue(bool(view["compact"]))
        self.assertIn(view["freshness"]["freshness"], {"current", "stale", "unknown"})


if __name__ == "__main__":
    unittest.main()
