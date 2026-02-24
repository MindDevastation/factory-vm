import os
import tempfile
import unittest
from pathlib import Path

from render_worker.main import OutputGrowthWatchdog, _render_output_total_bytes


class TestOutputGrowthWatchdog(unittest.TestCase):
    def test_grace_period_disables_stuck(self) -> None:
        wd = OutputGrowthWatchdog(start_ts=0.0, grace_sec=30.0, idle_sec=10.0, min_delta_bytes=1)
        wd.update(total_bytes=0, now_ts=0.0)
        self.assertFalse(wd.is_stuck(now_ts=5.0))
        self.assertFalse(wd.is_stuck(now_ts=25.0))
        # After grace passes and still no growth -> stuck
        self.assertTrue(wd.is_stuck(now_ts=41.0))

    def test_idle_detects_stuck(self) -> None:
        wd = OutputGrowthWatchdog(start_ts=0.0, grace_sec=0.0, idle_sec=10.0, min_delta_bytes=1)
        wd.update(total_bytes=0, now_ts=0.0)
        self.assertFalse(wd.is_stuck(now_ts=9.9))
        self.assertTrue(wd.is_stuck(now_ts=10.0))
        self.assertTrue(wd.is_stuck(now_ts=100.0))

    def test_growth_resets_timer(self) -> None:
        wd = OutputGrowthWatchdog(start_ts=0.0, grace_sec=0.0, idle_sec=10.0, min_delta_bytes=1)
        wd.update(total_bytes=0, now_ts=0.0)
        wd.update(total_bytes=5000, now_ts=5.0)
        self.assertFalse(wd.is_stuck(now_ts=14.0))  # 9s after growth
        self.assertTrue(wd.is_stuck(now_ts=16.0))   # 11s after growth

    def test_min_delta(self) -> None:
        wd = OutputGrowthWatchdog(start_ts=0.0, grace_sec=0.0, idle_sec=10.0, min_delta_bytes=1024)
        wd.update(total_bytes=0, now_ts=0.0)
        # Small increments don't count
        wd.update(total_bytes=512, now_ts=2.0)
        self.assertTrue(wd.is_stuck(now_ts=12.0))
        # A big enough increment resets
        wd.update(total_bytes=2048, now_ts=13.0)
        self.assertFalse(wd.is_stuck(now_ts=20.0))


class TestRenderOutputTotalBytes(unittest.TestCase):
    def test_sums_final_and_temp_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            out = d / "video.mp4"
            tmp1 = d / "video.mp4.tmp"
            tmp2 = d / "video.mp4.part"

            out.write_bytes(b"a" * 10)
            tmp1.write_bytes(b"b" * 5)
            tmp2.write_bytes(b"c" * 7)

            self.assertEqual(_render_output_total_bytes(out), 22)
