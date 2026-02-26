from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.common.youtube_token_resolver import (
    YouTubeTokenResolutionError,
    resolve_channel_token_path,
)


class TestYouTubeTokenResolver(unittest.TestCase):
    def test_builds_channel_token_path(self):
        with tempfile.TemporaryDirectory() as td:
            token_file = Path(td) / "titanwave-sonic" / "token.json"
            token_file.parent.mkdir(parents=True, exist_ok=True)
            token_file.write_text("{}", encoding="utf-8")

            token_path = resolve_channel_token_path(
                channel_slug="titanwave-sonic",
                tokens_dir=td,
            )

        self.assertEqual(token_path, str(token_file))

    def test_error_when_tokens_dir_missing(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(YouTubeTokenResolutionError) as ctx:
                resolve_channel_token_path(channel_slug="music-a")

        self.assertIn("YT_TOKENS_DIR is required", str(ctx.exception))

    def test_error_when_token_file_missing(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(YouTubeTokenResolutionError) as ctx:
                resolve_channel_token_path(channel_slug="music-a", tokens_dir=td)

        self.assertIn("YouTube token missing for channel music-a", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
