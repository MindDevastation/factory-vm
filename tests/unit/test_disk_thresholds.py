from __future__ import annotations

import unittest
from unittest.mock import patch

from services.common.disk_thresholds import DiskThresholds, evaluate_disk_status, load_disk_thresholds


class TestDiskThresholds(unittest.TestCase):
    def test_load_disk_thresholds_uses_spec_defaults(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            thresholds = load_disk_thresholds()

        self.assertEqual(thresholds, DiskThresholds(warn_percent=15.0, warn_gib=20.0, fail_percent=8.0, fail_gib=10.0))

    def test_load_disk_thresholds_honors_environment_overrides(self) -> None:
        env = {
            "FACTORY_SMOKE_DISK_WARN_PERCENT": "18",
            "FACTORY_SMOKE_DISK_WARN_GIB": "25",
            "FACTORY_SMOKE_DISK_FAIL_PERCENT": "7",
            "FACTORY_SMOKE_DISK_FAIL_GIB": "9",
        }
        with patch.dict("os.environ", env, clear=True):
            thresholds = load_disk_thresholds()

        self.assertEqual(thresholds, DiskThresholds(warn_percent=18.0, warn_gib=25.0, fail_percent=7.0, fail_gib=9.0))

    def test_evaluate_disk_status(self) -> None:
        thresholds = DiskThresholds()

        self.assertEqual(evaluate_disk_status(free_percent=30, free_gib=100, thresholds=thresholds), "PASS")
        self.assertEqual(evaluate_disk_status(free_percent=14, free_gib=100, thresholds=thresholds), "WARN")
        self.assertEqual(evaluate_disk_status(free_percent=20, free_gib=9, thresholds=thresholds), "FAIL")
