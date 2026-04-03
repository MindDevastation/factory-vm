from __future__ import annotations

import unittest

from services.telegram_inbox import build_compact_read_view, build_factory_overview


class TestE6AMf4Slice4Hardening(unittest.TestCase):
    def test_stale_visibility_and_compact_contract(self) -> None:
        view = build_compact_read_view(
            summary="x",
            reason="y",
            risk="low",
            actions=[],
            web_link="/ops",
            generated_at="2020-01-01T00:00:00Z",
        )
        self.assertTrue(bool(view["compact"]))
        self.assertTrue(bool(view["freshness"]["is_stale"]))

    def test_read_surface_is_non_mutating_projection(self) -> None:
        rows = [{"job_id": 1, "publish_state": "manual_handoff_pending"}]
        before = list(rows)
        _ = build_factory_overview(rows=rows, generated_at="2026-04-02T00:00:00Z")
        self.assertEqual(rows, before)


if __name__ == "__main__":
    unittest.main()
