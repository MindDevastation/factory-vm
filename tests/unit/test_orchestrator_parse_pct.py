from __future__ import annotations

import unittest

from services.workers.orchestrator import _parse_progress_pct, _workspace_audio_stem


class TestOrchestratorParsePct(unittest.TestCase):
    def test_parse_progress_pct(self) -> None:
        self.assertEqual(_parse_progress_pct("12%"), 12.0)
        self.assertEqual(_parse_progress_pct("render 12.5 %"), 12.5)
        self.assertEqual(_parse_progress_pct(" 100 % "), 100.0)
        self.assertIsNone(_parse_progress_pct("nope"))
        self.assertIsNone(_parse_progress_pct("-1%"))
        self.assertIsNone(_parse_progress_pct("101%"))


    def test_workspace_audio_stem_normalization(self) -> None:
        self.assertEqual(
            _workspace_audio_stem(queue_idx=1, original_filename_stem="028_no downtime"),
            "001_No_Downtime",
        )
        self.assertEqual(
            _workspace_audio_stem(queue_idx=12, original_filename_stem="123_song: part*1__mix"),
            "012_Song_Part_1_Mix",
        )
        out = _workspace_audio_stem(queue_idx=2, original_filename_stem="028_no downtime")
        self.assertTrue(out.startswith("002_"))
        self.assertNotIn("028", out.split("_", 1)[1])


if __name__ == "__main__":
    unittest.main()
