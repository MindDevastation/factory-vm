from __future__ import annotations

import importlib
import os
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestChannelPlaylistsApiSync(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def test_populated_channel_syncs_and_returns_playlists(self) -> None:
        with temp_env() as (_, _):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/tmp/client.json"
            os.environ["YT_TOKENS_DIR"] = "/tmp/yt-tokens"
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            mod, client = self._new_client()
            mod.resolve_channel_token_path = lambda **_: "/tmp/token.json"

            class _YT:
                def __init__(self, **_: object):
                    pass

                def list_playlists(self) -> list[dict[str, str]]:
                    return [{"playlist_id": "PL_1", "playlist_title": "Playlist 1"}]

            mod.YouTubeClient = _YT
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/channels/{channel_id}/playlists", headers=h)
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertEqual(payload["load_status"], "synced")
            self.assertEqual(payload["playlists"], [{"playlist_id": "PL_1", "playlist_title": "Playlist 1"}])

    def test_truly_empty_channel_returns_honest_empty_state(self) -> None:
        with temp_env() as (_, _):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/tmp/client.json"
            os.environ["YT_TOKENS_DIR"] = "/tmp/yt-tokens"
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            mod, client = self._new_client()
            mod.resolve_channel_token_path = lambda **_: "/tmp/token.json"

            class _YT:
                def __init__(self, **_: object):
                    pass

                def list_playlists(self) -> list[dict[str, str]]:
                    return []

            mod.YouTubeClient = _YT
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/channels/{channel_id}/playlists", headers=h)
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertEqual(payload["load_status"], "synced")
            self.assertEqual(payload["playlists"], [])

    def test_sync_failure_surfaces_unavailable_instead_of_empty(self) -> None:
        with temp_env() as (_, _):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/tmp/client.json"
            os.environ["YT_TOKENS_DIR"] = "/tmp/yt-tokens"
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            mod, client = self._new_client()
            mod.resolve_channel_token_path = lambda **_: "/tmp/token.json"

            class _YT:
                def __init__(self, **_: object):
                    pass

                def list_playlists(self) -> list[dict[str, str]]:
                    raise RuntimeError("sync boom")

            mod.YouTubeClient = _YT
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/channels/{channel_id}/playlists", headers=h)
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertEqual(payload["load_status"], "unavailable")
            self.assertIn("sync boom", str(payload["load_error"]))


if __name__ == "__main__":
    unittest.main()
