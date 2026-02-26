from __future__ import annotations

import unittest
from unittest.mock import patch

from services.common.youtube_credentials import (
    YouTubeCredentialResolutionError,
    resolve_youtube_channel_credentials,
)


class TestYouTubeCredentialsResolver(unittest.TestCase):
    def test_channel_slug_uses_convention_token_path(self):
        with patch.dict(
            "os.environ",
            {
                "YT_TOKEN_BASE_DIR": "/secure/youtube",
                "YT_CLIENT_SECRET_JSON": "/secure/youtube/client_secret.json",
            },
            clear=True,
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a")

        self.assertEqual(client_secret, "/secure/youtube/client_secret.json")
        self.assertEqual(token_path, "/secure/youtube/music-a/token.json")
        self.assertEqual(source, "convention")

    def test_fallback_to_global_env_when_base_dir_missing(self):
        with patch.dict(
            "os.environ",
            {
                "YT_TOKEN_BASE_DIR": "",
                "YT_TOKEN_JSON": "/secrets/global-token.json",
                "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
            },
            clear=True,
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a")

        self.assertEqual(client_secret, "/secrets/global-client.json")
        self.assertEqual(token_path, "/secrets/global-token.json")
        self.assertEqual(source, "global_env")

    def test_error_when_both_convention_and_global_env_missing(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(YouTubeCredentialResolutionError) as ctx:
                resolve_youtube_channel_credentials("music-a")

        self.assertIn("YT_TOKEN_BASE_DIR", str(ctx.exception))
        self.assertIn("YT_TOKEN_JSON", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
