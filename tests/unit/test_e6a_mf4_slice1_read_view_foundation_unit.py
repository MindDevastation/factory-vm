from __future__ import annotations

import unittest

from services.telegram_inbox import build_compact_read_view, build_freshness_summary


class TestE6AMf4Slice1ReadViewFoundationUnit(unittest.TestCase):
    def test_compact_read_view_contract(self) -> None:
        view = build_compact_read_view(
            summary="factory degraded",
            reason="queue backlog",
            risk="high",
            actions=["open_web"],
            web_link="/ops/overview",
            generated_at="2026-04-02T00:00:00Z",
        )
        self.assertTrue(bool(view["compact"]))
        self.assertIn("freshness", view)

    def test_freshness_summary_stale_marker(self) -> None:
        f = build_freshness_summary(generated_at="2020-01-01T00:00:00Z")
        self.assertTrue(bool(f["is_stale"]))


if __name__ == "__main__":
    unittest.main()
