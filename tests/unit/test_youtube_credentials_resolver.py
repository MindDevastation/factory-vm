from __future__ import annotations

import os
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

    def test_error_when_token_file_missing_exact_message(self):
        with tempfile.TemporaryDirectory() as td:
            expected = str(Path(td) / "music-a" / "token.json")
            with self.assertRaises(YouTubeTokenResolutionError) as ctx:
                resolve_channel_token_path(channel_slug="music-a", tokens_dir=td)

        self.assertEqual(str(ctx.exception), f"YouTube token missing for channel music-a at {expected}")

    def test_error_when_token_file_unreadable_exact_message(self):
        with tempfile.TemporaryDirectory() as td:
            token_file = Path(td) / "music-a" / "token.json"
            token_file.parent.mkdir(parents=True, exist_ok=True)
            token_file.write_text("{}", encoding="utf-8")

            original_access = os.access

            def _deny_token(path, mode):
                if str(path) == str(token_file):
                    return False
                return original_access(path, mode)

            with patch("os.access", side_effect=_deny_token):
                with self.assertRaises(YouTubeTokenResolutionError) as ctx:
                    resolve_channel_token_path(channel_slug="music-a", tokens_dir=td)

            self.assertEqual(
                str(ctx.exception),
                f"YouTube token missing for channel music-a at {token_file}",
            )


if __name__ == "__main__":
    unittest.main()
