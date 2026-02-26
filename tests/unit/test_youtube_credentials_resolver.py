from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from services.common.youtube_credentials import (
    YouTubeCredentialResolutionError,
    resolve_youtube_channel_credentials,
)


class TestYouTubeCredentialsResolver(unittest.TestCase):
    def test_channel_specific_token_used(self):
        channels = [
            SimpleNamespace(
                slug="music-a",
                yt_token_json_path="/secrets/channel-token.json",
                yt_client_secret_json_path="/secrets/channel-client.json",
            )
        ]
        with (
            patch("services.common.youtube_credentials.load_channels", return_value=channels),
            patch.dict(
                "os.environ",
                {
                    "YT_TOKEN_JSON": "/secrets/global-token.json",
                    "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
                },
                clear=True,
            ),
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a")

        self.assertEqual(client_secret, "/secrets/channel-client.json")
        self.assertEqual(token_path, "/secrets/channel-token.json")
        self.assertEqual(source, "channel")

    def test_fallback_to_global_token(self):
        channels = [
            SimpleNamespace(
                slug="music-a",
                yt_token_json_path=None,
                yt_client_secret_json_path=None,
            )
        ]
        with (
            patch("services.common.youtube_credentials.load_channels", return_value=channels),
            patch.dict(
                "os.environ",
                {
                    "YT_TOKEN_JSON": "/secrets/global-token.json",
                    "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
                },
                clear=True,
            ),
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a")

        self.assertEqual(client_secret, "/secrets/global-client.json")
        self.assertEqual(token_path, "/secrets/global-token.json")
        self.assertEqual(source, "global")

    def test_error_when_neither_configured(self):
        channels = [
            SimpleNamespace(
                slug="music-a",
                yt_token_json_path=None,
                yt_client_secret_json_path="/secrets/channel-client.json",
            )
        ]
        with (
            patch("services.common.youtube_credentials.load_channels", return_value=channels),
            patch.dict("os.environ", {}, clear=True),
        ):
            with self.assertRaises(YouTubeCredentialResolutionError) as ctx:
                resolve_youtube_channel_credentials("music-a")

        self.assertIn("YouTube credentials not configured for channel music-a", str(ctx.exception))

    def test_client_secret_fallback_behavior(self):
        channels = [
            SimpleNamespace(
                slug="music-a",
                yt_token_json_path="/secrets/channel-token.json",
                yt_client_secret_json_path=None,
            )
        ]
        with (
            patch("services.common.youtube_credentials.load_channels", return_value=channels),
            patch.dict(
                "os.environ",
                {
                    "YT_TOKEN_JSON": "/secrets/global-token.json",
                    "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
                },
                clear=True,
            ),
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a")

        self.assertEqual(client_secret, "/secrets/global-client.json")
        self.assertEqual(token_path, "/secrets/channel-token.json")
        self.assertEqual(source, "channel")


if __name__ == "__main__":
    unittest.main()
