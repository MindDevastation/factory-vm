from __future__ import annotations

import unittest

from services.workers.orchestrator import _parse_progress_pct


class TestOrchestratorParsePct(unittest.TestCase):
    def test_parse_progress_pct(self) -> None:
        self.assertEqual(_parse_progress_pct("12%"), 12.0)
        self.assertEqual(_parse_progress_pct("render 12.5 %"), 12.5)
        self.assertEqual(_parse_progress_pct(" 100 % "), 100.0)
        self.assertIsNone(_parse_progress_pct("nope"))
        self.assertIsNone(_parse_progress_pct("-1%"))
        self.assertIsNone(_parse_progress_pct("101%"))


if __name__ == "__main__":
    unittest.main()
