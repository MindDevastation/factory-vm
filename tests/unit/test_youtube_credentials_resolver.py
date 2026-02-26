from __future__ import annotations

import unittest
from unittest.mock import patch

from services.common.youtube_credentials import (
    YouTubeCredentialResolutionError,
    resolve_youtube_channel_credentials,
)


class TestYouTubeCredentialsResolver(unittest.TestCase):
    def test_uses_global_env_credentials(self):
        with patch.dict(
            "os.environ",
            {
                "YT_TOKEN_JSON": "/secrets/global-token.json",
                "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
            },
            clear=True,
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a")

        self.assertEqual(client_secret, "/secrets/global-client.json")
        self.assertEqual(token_path, "/secrets/global-token.json")
        self.assertEqual(source, "global")

    def test_error_when_missing_required_env(self):
        with patch.dict(
            "os.environ",
            {
                "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
            },
            clear=True,
        ):
            with self.assertRaises(YouTubeCredentialResolutionError) as ctx:
                resolve_youtube_channel_credentials("music-a")

        self.assertIn("YouTube credentials not configured for channel music-a", str(ctx.exception))

    def test_explicit_args_override_env(self):
        with patch.dict(
            "os.environ",
            {
                "YT_TOKEN_JSON": "/secrets/global-token.json",
                "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
            },
            clear=True,
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials(
                "music-a",
                global_client_secret_path="/tmp/client.json",
                global_token_path="/tmp/token.json",
            )

        self.assertEqual(client_secret, "/tmp/client.json")
        self.assertEqual(token_path, "/tmp/token.json")
        self.assertEqual(source, "global")


if __name__ == "__main__":
    unittest.main()
