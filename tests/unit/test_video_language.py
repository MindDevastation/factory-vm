from __future__ import annotations

import unittest

from services.common.video_language import normalize_video_language


class TestVideoLanguageNormalization(unittest.TestCase):
    def test_known_mappings_and_codes(self) -> None:
        self.assertEqual(normalize_video_language("English"), "en")
        self.assertEqual(normalize_video_language("english"), "en")
        self.assertEqual(normalize_video_language("EN"), "en")
        self.assertEqual(normalize_video_language("en"), "en")
        self.assertEqual(normalize_video_language("Ukrainian"), "uk")
        self.assertEqual(normalize_video_language("russian"), "ru")
        self.assertEqual(normalize_video_language("Spanish"), "es")

    def test_uk_ambiguity_and_invalid_values(self) -> None:
        self.assertIsNone(normalize_video_language("UK"))
        self.assertIsNone(normalize_video_language("en-US"))
        self.assertIsNone(normalize_video_language("Klingon"))


if __name__ == "__main__":
    unittest.main()
