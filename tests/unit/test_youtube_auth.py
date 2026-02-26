from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import youtube_auth


class _FakeCreds:
    def to_json(self) -> str:
        return '{"access_token":"x"}'


class _FakeFlow:
    def run_console(self):
        return _FakeCreds()


class TestYoutubeAuth(unittest.TestCase):
    def test_token_path_builder(self):
        token_path = youtube_auth._token_path('/secure/youtube/channels', 'titanwave-sonic')
        self.assertEqual(str(token_path), '/secure/youtube/channels/titanwave-sonic/token.json')

    def test_main_writes_to_tokens_dir_with_channel_slug(self):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / 'demo-channel' / 'token.json'
            with patch.dict(
                'os.environ',
                {
                    'YT_CLIENT_SECRET_JSON': '/tmp/client_secret.json',
                    'YT_TOKENS_DIR': td,
                    'YT_TOKEN_JSON': '/tmp/legacy/token.json',
                },
                clear=True,
            ):
                rc = youtube_auth.main(
                    ['--channel-slug', 'demo-channel'],
                    flow_builder=lambda _client_secret: _FakeFlow(),
                )

            self.assertEqual(rc, 0)
            self.assertTrue(target.exists())
            self.assertEqual(target.read_text(encoding='utf-8'), '{"access_token":"x"}')

    def test_main_errors_when_tokens_dir_missing(self):
        with patch.dict('os.environ', {'YT_CLIENT_SECRET_JSON': '/tmp/client_secret.json'}, clear=True):
            rc = youtube_auth.main(['--channel-slug', 'demo-channel'], flow_builder=lambda _client_secret: _FakeFlow())
        self.assertEqual(rc, 1)


if __name__ == '__main__':
    unittest.main()
