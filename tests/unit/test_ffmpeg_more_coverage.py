from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from services.common import ffmpeg as ffm


class TestFfmpegMoreCoverage(unittest.TestCase):
    def test_parse_fps_float_exception_branch(self) -> None:
        stream = {"avg_frame_rate": object()}
        self.assertIsNone(ffm.parse_fps(stream))

    def test_volumedetect_nonzero_return_code(self) -> None:
        p = Path("/tmp/x.mp4")
        with mock.patch.object(ffm, "run", return_value=(1, "", "err")):
            mean_db, max_db, warn = ffm.volumedetect(p, seconds=1)
        self.assertIsNone(mean_db)
        self.assertIsNone(max_db)
        self.assertEqual(warn, "volumedetect failed")
