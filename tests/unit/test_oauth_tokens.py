from __future__ import annotations

import base64
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlparse

from fastapi import HTTPException

from services.factory_api.oauth_tokens import (
    YOUTUBE_SCOPE,
    build_authorization_url,
    ensure_token_dir,
    exchange_code_for_token_json,
    generate_code_verifier,
    oauth_token_path,
    sign_state,
    verify_state,
)


class TestOauthTokens(unittest.TestCase):
    def test_generate_code_verifier_is_pkce_sized_and_urlsafe(self) -> None:
        verifier = generate_code_verifier()
        self.assertGreaterEqual(len(verifier), 43)
        self.assertLessEqual(len(verifier), 128)
        self.assertRegex(verifier, r"^[A-Za-z0-9_-]+$")

    def test_build_authorization_url_uses_supplied_pkce_verifier_without_exposing_it(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client_secret = Path(td) / "client.json"
            client_secret.write_text(
                '{"web":{"client_id":"cid","client_secret":"secret","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","redirect_uris":["http://localhost/callback"]}}',
                encoding="utf-8",
            )
            verifier = "A" * 64
            url = build_authorization_url(
                client_secret_path=str(client_secret),
                scope="https://www.googleapis.com/auth/drive",
                redirect_uri="http://localhost/callback",
                state="signed-state",
                code_verifier=verifier,
            )

        params = parse_qs(urlparse(url).query)
        digest = hashlib.sha256(verifier.encode("ascii")).digest()
        expected_challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        self.assertEqual(params.get("code_challenge"), [expected_challenge])
        self.assertEqual(params.get("code_challenge_method"), ["S256"])
        self.assertEqual(params.get("state"), ["signed-state"])
        self.assertNotIn(verifier, url)

    def test_exchange_code_for_token_json_configures_flow_with_code_verifier(self) -> None:
        credentials = mock.Mock()
        credentials.to_json.return_value = '{"access_token":"token"}'
        flow = mock.Mock()
        flow.credentials = credentials
        with mock.patch("services.factory_api.oauth_tokens.Flow.from_client_secrets_file", return_value=flow) as factory:
            token_json = exchange_code_for_token_json(
                client_secret_path="client.json",
                scope="scope",
                redirect_uri="http://localhost/callback",
                code="auth-code",
                code_verifier="verifier-123",
            )

        self.assertEqual(token_json, '{"access_token":"token"}')
        factory.assert_called_once_with(
            "client.json",
            scopes="scope",
            redirect_uri="http://localhost/callback",
            code_verifier="verifier-123",
        )
        flow.fetch_token.assert_called_once_with(code="auth-code")

    def test_sign_and_verify_state(self) -> None:
        state = sign_state(secret="secret", kind="gdrive", channel_slug="darkwood-reverie", now_ts=100)
        payload = verify_state(secret="secret", expected_kind="gdrive", state=state, now_ts=200)
        self.assertEqual(payload["kind"], "gdrive")
        self.assertEqual(payload["channel_slug"], "darkwood-reverie")

    def test_verify_state_rejects_wrong_kind(self) -> None:
        state = sign_state(secret="secret", kind="youtube", channel_slug="darkwood-reverie", now_ts=100)
        with self.assertRaises(HTTPException):
            verify_state(secret="secret", expected_kind="gdrive", state=state, now_ts=120)

    def test_verify_state_rejects_expired(self) -> None:
        state = sign_state(secret="secret", kind="gdrive", channel_slug="darkwood-reverie", now_ts=100)
        with self.assertRaises(HTTPException):
            verify_state(secret="secret", expected_kind="gdrive", state=state, now_ts=1000)

    def test_verify_state_rejects_tampered_signature(self) -> None:
        state = sign_state(secret="secret", kind="gdrive", channel_slug="darkwood-reverie", now_ts=100)
        payload = state.split('.', 1)[0]
        tampered = payload + '.AAAA'
        with self.assertRaises(HTTPException):
            verify_state(secret="secret", expected_kind="gdrive", state=tampered, now_ts=120)

    def test_token_path_and_dir_creation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            token_path = oauth_token_path(base_dir=td, channel_slug="darkwood-reverie")
            self.assertEqual(token_path, Path(td) / "darkwood-reverie" / "token.json")
            ensure_token_dir(token_path)
            self.assertTrue(token_path.parent.is_dir())


    def test_sign_verify_without_channel_slug_for_add_channel_state(self) -> None:
        state = sign_state(secret="secret", kind="youtube_add_channel", channel_slug=None)
        payload = verify_state(secret="secret", expected_kind="youtube_add_channel", state=state, require_channel_slug=False)
        self.assertEqual(payload["kind"], "youtube_add_channel")
        self.assertNotIn("channel_slug", payload)

    def test_youtube_oauth_scope_includes_playlist_management(self) -> None:
        self.assertIn("https://www.googleapis.com/auth/youtube.upload", YOUTUBE_SCOPE)
        self.assertIn("https://www.googleapis.com/auth/youtube", YOUTUBE_SCOPE)


if __name__ == "__main__":
    unittest.main()
