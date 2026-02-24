from __future__ import annotations

import unittest

from services.workers.qa import _safe_float


class TestQaSafeFloat(unittest.TestCase):
    def test_safe_float_handles_na(self) -> None:
        self.assertIsNone(_safe_float("N/A"))
        self.assertIsNone(_safe_float(""))
        self.assertIsNone(_safe_float(None))
        self.assertEqual(_safe_float("1.5"), 1.5)
