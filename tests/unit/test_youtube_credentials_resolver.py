from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from services.common.youtube_credentials import (
    YouTubeCredentialResolutionError,
    resolve_youtube_channel_credentials,
)


class TestYouTubeCredentialsResolver(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT NOT NULL UNIQUE,
                yt_token_json_path TEXT,
                yt_client_secret_json_path TEXT
            )
            """
        )

    def tearDown(self) -> None:
        self.conn.close()

    def _insert_channel(self, slug: str, token_path: str | None, client_secret_path: str | None) -> None:
        self.conn.execute(
            "INSERT INTO channels(slug, yt_token_json_path, yt_client_secret_json_path) VALUES(?, ?, ?)",
            (slug, token_path, client_secret_path),
        )

    def test_channel_specific_token_used(self):
        self._insert_channel("music-a", "/secrets/channel-token.json", "/secrets/channel-client.json")
        with patch.dict(
            "os.environ",
            {
                "YT_TOKEN_JSON": "/secrets/global-token.json",
                "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
            },
            clear=True,
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a", conn=self.conn)

        self.assertEqual(client_secret, "/secrets/channel-client.json")
        self.assertEqual(token_path, "/secrets/channel-token.json")
        self.assertEqual(source, "channel")

    def test_fallback_to_global_token(self):
        self._insert_channel("music-a", None, None)
        with patch.dict(
            "os.environ",
            {
                "YT_TOKEN_JSON": "/secrets/global-token.json",
                "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
            },
            clear=True,
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a", conn=self.conn)

        self.assertEqual(client_secret, "/secrets/global-client.json")
        self.assertEqual(token_path, "/secrets/global-token.json")
        self.assertEqual(source, "global")

    def test_error_when_neither_configured(self):
        self._insert_channel("music-a", None, "/secrets/channel-client.json")
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(YouTubeCredentialResolutionError) as ctx:
                resolve_youtube_channel_credentials("music-a", conn=self.conn)

        self.assertIn("YouTube credentials not configured for channel music-a", str(ctx.exception))

    def test_client_secret_fallback_behavior(self):
        self._insert_channel("music-a", "/secrets/channel-token.json", None)
        with patch.dict(
            "os.environ",
            {
                "YT_TOKEN_JSON": "/secrets/global-token.json",
                "YT_CLIENT_SECRET_JSON": "/secrets/global-client.json",
            },
            clear=True,
        ):
            client_secret, token_path, source = resolve_youtube_channel_credentials("music-a", conn=self.conn)

        self.assertEqual(client_secret, "/secrets/global-client.json")
        self.assertEqual(token_path, "/secrets/channel-token.json")
        self.assertEqual(source, "channel")


if __name__ == "__main__":
    unittest.main()
