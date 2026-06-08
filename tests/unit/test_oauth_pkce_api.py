from __future__ import annotations

import base64
import importlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib.parse import parse_qs, urlencode, urlparse

from fastapi.testclient import TestClient

from services.common import db as dbm


class _FakeFlow:
    def __init__(self, *, scopes, redirect_uri):
        self.scopes = scopes
        self.redirect_uri = redirect_uri

    @classmethod
    def from_client_secrets_file(cls, _client_secret_path, scopes, redirect_uri):
        return cls(scopes=scopes, redirect_uri=redirect_uri)

    def authorization_url(self, **kwargs):
        query = urlencode(
            {
                "state": kwargs["state"],
                "code_challenge": kwargs["code_challenge"],
                "code_challenge_method": kwargs["code_challenge_method"],
                "redirect_uri": self.redirect_uri,
            }
        )
        return f"https://accounts.example/authorize?{query}", None


class TestOauthPkceApi(unittest.TestCase):
    def _load_app(self, td: str):
        env_values = {
            "FACTORY_DB_PATH": str(Path(td) / "factory.sqlite3"),
            "FACTORY_STORAGE_ROOT": str(Path(td) / "storage"),
            "FACTORY_BASIC_AUTH_USER": "admin",
            "FACTORY_BASIC_AUTH_PASS": "change_me",
            "OAUTH_REDIRECT_BASE_URL": "https://factory.example",
            "OAUTH_STATE_SECRET": "test-state-secret",
            "GDRIVE_CLIENT_SECRET_JSON": str(Path(td) / "gdrive_client.json"),
            "GDRIVE_TOKENS_DIR": str(Path(td) / "gdrive_tokens"),
            "GDRIVE_OAUTH_TOKEN_JSON": str(Path(td) / "global_gdrive_token.json"),
            "YT_CLIENT_SECRET_JSON": str(Path(td) / "yt_client.json"),
            "YT_TOKENS_DIR": str(Path(td) / "yt_tokens"),
        }
        Path(env_values["GDRIVE_CLIENT_SECRET_JSON"]).write_text("{}", encoding="utf-8")
        Path(env_values["YT_CLIENT_SECRET_JSON"]).write_text("{}", encoding="utf-8")
        patcher = mock.patch.dict("os.environ", env_values, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        conn = dbm.connect(mod.env)
        try:
            dbm.migrate(conn)
            dbm.create_channel(conn, slug="demo-channel", display_name="Demo Channel")
        finally:
            conn.close()
        return mod, TestClient(mod.app)

    def _headers(self) -> dict[str, str]:
        token = base64.b64encode(b"admin:change_me").decode("ascii")
        return {"Authorization": f"Basic {token}"}

    def test_youtube_start_stores_verifier_and_response_hides_it(self) -> None:
        with tempfile.TemporaryDirectory() as td, mock.patch("services.factory_api.oauth_tokens.Flow", _FakeFlow):
            mod, client = self._load_app(td)
            response = client.post("/v1/oauth/youtube/demo-channel/start", headers=self._headers())

            self.assertEqual(response.status_code, 200, response.text)
            body = response.json()
            self.assertIn("auth_url", body)
            self.assertNotIn("code_verifier", body)
            self.assertNotIn("verifier", response.text.lower())
            query = parse_qs(urlparse(body["auth_url"]).query)
            self.assertEqual(query["code_challenge_method"], ["S256"])
            self.assertRegex(query["code_challenge"][0], r"^[A-Za-z0-9_-]+$")
            payload = mod.verify_state(
                secret=mod.env.oauth_state_secret,
                expected_kind="youtube",
                state=query["state"][0],
            )
            verifier_path = mod._oauth_code_verifier_path(payload["nonce"])
            self.assertTrue(verifier_path.is_file())
            self.assertNotEqual(verifier_path.read_text(encoding="utf-8").strip(), query["code_challenge"][0])

    def test_youtube_callback_passes_verifier_and_deletes_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod, client = self._load_app(td)
            nonce = "callbacknonce"
            verifier = "v" * 64
            state = mod.sign_state(secret=mod.env.oauth_state_secret, kind="youtube", channel_slug="demo-channel", nonce=nonce)
            mod._write_oauth_code_verifier(nonce, verifier)
            captured = {}

            def fake_exchange(**kwargs):
                captured.update(kwargs)
                return '{"access_token":"token"}'

            with mock.patch("services.factory_api.app.exchange_code_for_token_json", side_effect=fake_exchange):
                response = client.get(
                    f"/v1/oauth/youtube/callback?code=abc&state={state}",
                    headers=self._headers(),
                )

            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(captured["code_verifier"], verifier)
            self.assertFalse(mod._oauth_code_verifier_path(nonce).exists())
            token_path = Path(mod.env.yt_tokens_dir) / "demo-channel" / "token.json"
            self.assertTrue(token_path.is_file())
            self.assertNotIn(verifier, response.text)

    def test_missing_verifier_returns_safe_400(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod, client = self._load_app(td)
            state = mod.sign_state(secret=mod.env.oauth_state_secret, kind="gdrive", channel_slug="demo-channel", nonce="missingnonce")
            response = client.get(
                f"/v1/oauth/gdrive/callback?code=abc&state={state}",
                headers=self._headers(),
            )
            self.assertEqual(response.status_code, 400)
            self.assertIn("OAuth session expired; start authorization again.", response.text)

    def test_youtube_add_channel_uses_verifier_and_keeps_temp_token_flow(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod, client = self._load_app(td)
            nonce = "addchannelnonce"
            verifier = "w" * 64
            state = mod.sign_state(secret=mod.env.oauth_state_secret, kind="youtube_add_channel", nonce=nonce)
            mod._write_oauth_code_verifier(nonce, verifier)
            captured = {}

            def fake_exchange(**kwargs):
                captured.update(kwargs)
                return '{"access_token":"token"}'

            channels = [{"id": "chan-1", "title": "New Channel"}, {"id": "chan-2", "title": "Other"}]
            with mock.patch("services.factory_api.app.exchange_code_for_token_json", side_effect=fake_exchange), mock.patch(
                "services.factory_api.app._youtube_channels_from_token_json", return_value=channels
            ):
                response = client.get(
                    f"/v1/oauth/youtube/add_channel/callback?code=abc&state={state}",
                    headers=self._headers(),
                )

            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(captured["code_verifier"], verifier)
            self.assertFalse(mod._oauth_code_verifier_path(nonce).exists())
            self.assertTrue((mod._storage_tmp_oauth_dir() / f"{nonce}.json").is_file())
            self.assertIn("Select YouTube Channel", response.text)
            self.assertNotIn(verifier, response.text)

    def test_global_gdrive_callback_uses_verifier(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mod, client = self._load_app(td)
            nonce = "globalnonce"
            verifier = "x" * 64
            state = mod.sign_state(secret=mod.env.oauth_state_secret, kind="gdrive_global", nonce=nonce)
            mod._write_oauth_code_verifier(nonce, verifier)
            captured = {}

            def fake_exchange(**kwargs):
                captured.update(kwargs)
                return '{"access_token":"token"}'

            with mock.patch("services.factory_api.app.exchange_code_for_token_json", side_effect=fake_exchange):
                response = client.get(
                    f"/v1/oauth/gdrive_global/callback?code=abc&state={state}",
                    headers=self._headers(),
                )

            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(captured["code_verifier"], verifier)
            self.assertFalse(mod._oauth_code_verifier_path(nonce).exists())
            self.assertTrue(Path(mod.env.gdrive_oauth_token_json).is_file())

    def test_callback_still_rejects_invalid_state_before_verifier_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _mod, client = self._load_app(td)
            response = client.get(
                "/v1/oauth/youtube/callback?code=abc&state=bad.state",
                headers=self._headers(),
            )
            self.assertEqual(response.status_code, 400)
            self.assertIn("invalid oauth state", response.text)


if __name__ == "__main__":
    unittest.main()
