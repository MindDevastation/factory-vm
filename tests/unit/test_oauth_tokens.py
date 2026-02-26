from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fastapi import HTTPException

from services.factory_api.oauth_tokens import (
    ensure_token_dir,
    oauth_token_path,
    sign_state,
    verify_state,
)


class TestOauthTokens(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
