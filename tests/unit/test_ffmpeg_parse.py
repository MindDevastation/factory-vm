from __future__ import annotations

import unittest

from services.common.ffmpeg import parse_fps


class TestParseFps(unittest.TestCase):
    def test_parse_fps_fraction(self) -> None:
        self.assertAlmostEqual(parse_fps({"avg_frame_rate": "24/1"}) or 0.0, 24.0)
        self.assertIsNone(parse_fps({"avg_frame_rate": "0/0"}))

    def test_parse_fps_numeric(self) -> None:
        self.assertEqual(parse_fps({"avg_frame_rate": 30}), 30.0)
        self.assertEqual(parse_fps({"r_frame_rate": "60"}), 60.0)

    def test_parse_fps_invalid(self) -> None:
        self.assertIsNone(parse_fps({"avg_frame_rate": "x/y"}))
        self.assertIsNone(parse_fps({}))


if __name__ == "__main__":
    unittest.main()
