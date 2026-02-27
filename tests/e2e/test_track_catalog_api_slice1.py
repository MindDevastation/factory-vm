from __future__ import annotations

import importlib
import json
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestTrackCatalogApiSlice1(unittest.TestCase):
    def _seed_canon(self, env: Env, *, slug: str, in_channels: bool = True, in_thresholds: bool = True) -> None:
        conn = dbm.connect(env)
        try:
            if in_channels:
                conn.execute("INSERT INTO canon_channels(value) VALUES(?)", (slug,))
            if in_thresholds:
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", (slug,))
        finally:
            conn.close()

    def _seed_tracks(self, env: Env, *, channel_slug: str) -> tuple[int, int]:
        conn = dbm.connect(env)
        try:
            cur1 = conn.execute(
                """
                INSERT INTO tracks(
                    channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (channel_slug, "trk-001", "gdrive-001", "gdrive", "001.wav", "Alpha", "Artist A", 120.0, 1000.0, 1001.0),
            )
            t1 = int(cur1.lastrowid)
            conn.execute(
                "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (t1, json.dumps({"scene": "night", "mood": "calm"}), 1002.0),
            )
            conn.execute(
                "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (t1, json.dumps({"scene": "night", "mood": "calm"}), 1002.0),
            )
            conn.execute(
                "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (t1, json.dumps({"safety": 0.92, "scene_match": 0.81}), 1002.0),
            )

            cur2 = conn.execute(
                """
                INSERT INTO tracks(
                    channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (channel_slug, "trk-002", "gdrive-002", "gdrive", "002.wav", "Beta", "Artist B", 100.0, 1003.0, None),
            )
            t2 = int(cur2.lastrowid)
            conn.execute(
                "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (t2, json.dumps({"scene": "day", "mood": "energetic"}), 1004.0),
            )
            return t1, t2
        finally:
            conn.close()

    def test_auth_required(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            r = client.get("/v1/track_catalog/channels")
            self.assertIn(r.status_code, (401, 403))

    def test_channels_requires_intersection_of_channels_and_canon_tables(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_canon(env, slug="darkwood-reverie", in_channels=True, in_thresholds=True)
            self._seed_canon(env, slug="channel-b", in_channels=True, in_thresholds=False)
            self._seed_canon(env, slug="channel-c", in_channels=False, in_thresholds=True)
            self._seed_canon(env, slug="ghost-channel", in_channels=True, in_thresholds=True)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get("/v1/track_catalog/channels", headers=h)
            self.assertEqual(r.status_code, 200)
            channels = r.json().get("channels", [])
            self.assertEqual([c["slug"] for c in channels], ["darkwood-reverie"])

    def test_tracks_list_filtering_and_detail(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_canon(env, slug="darkwood-reverie", in_channels=True, in_thresholds=True)
            t1, _t2 = self._seed_tracks(env, channel_slug="darkwood-reverie")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get(
                "/v1/track_catalog/darkwood-reverie/tracks?status=ANALYZED&scene=night&mood=calm&min_safety=0.9&min_scene_match=0.8",
                headers=h,
            )
            self.assertEqual(r.status_code, 200)
            tracks = r.json().get("tracks", [])
            self.assertEqual(len(tracks), 1)
            self.assertEqual(tracks[0]["track_id"], "trk-001")
            self.assertEqual(tracks[0]["status"], "ANALYZED")

            detail = client.get(f"/v1/track_catalog/tracks/{t1}", headers=h)
            self.assertEqual(detail.status_code, 200)
            track = detail.json()["track"]
            self.assertEqual(track["track_pk"], t1)
            self.assertEqual(track["scores"]["safety"], 0.92)


if __name__ == "__main__":
    unittest.main()
