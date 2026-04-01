from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.operational_kpi_runtime import _validate_recompute_mode, _validate_run_state


class TestOperationalKpiRuntimeUnit(unittest.TestCase):
    def test_recompute_mode_validation(self) -> None:
        self.assertEqual(_validate_recompute_mode("FULL_RECOMPUTE"), "FULL_RECOMPUTE")
        with self.assertRaises(AnalyticsDomainError):
            _validate_recompute_mode("BAD")

    def test_run_state_validation(self) -> None:
        self.assertEqual(_validate_run_state("PARTIAL"), "PARTIAL")
        with self.assertRaises(AnalyticsDomainError):
            _validate_run_state("BAD")


if __name__ == "__main__":
    unittest.main()
