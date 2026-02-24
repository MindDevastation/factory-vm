from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from services.common import ffmpeg as ffm


class TestCommonFfmpegMock(unittest.TestCase):
    def test_ffprobe_json_success(self) -> None:
        with patch("services.common.ffmpeg.run", lambda cmd: (0, '{"streams": [], "format": {"duration": "1.0"}}', "")):
            data = ffm.ffprobe_json(Path("x.mp4"))
        self.assertIn("streams", data)

    def test_ffprobe_json_failure_raises(self) -> None:
        with patch("services.common.ffmpeg.run", lambda cmd: (1, "", "boom")):
            with self.assertRaises(RuntimeError):
                ffm.ffprobe_json(Path("x.mp4"))

    def test_volumedetect_parses_mean_and_max(self) -> None:
        txt = "[Parsed_volumedetect_0] mean_volume: -20.0 dB\n[Parsed_volumedetect_0] max_volume: -1.0 dB\n"
        with patch("services.common.ffmpeg.run", lambda cmd: (0, "", txt)):
            mean_db, max_db, warn = ffm.volumedetect(Path("x.mp4"), seconds=1)
        self.assertEqual(mean_db, -20.0)
        self.assertEqual(max_db, -1.0)
        self.assertIsNone(warn)

    def test_make_preview_raises_on_failure(self) -> None:
        with patch("services.common.ffmpeg.run", lambda cmd: (1, "", "err")):
            with self.assertRaises(RuntimeError):
                ffm.make_preview_60s(
                    src_mp4=Path("in.mp4"),
                    dst_mp4=Path("/tmp/out.mp4"),
                    seconds=1,
                    width=1280,
                    height=720,
                    fps=24,
                    v_bitrate="1200k",
                    a_bitrate="96k",
                )


if __name__ == "__main__":
    unittest.main()
