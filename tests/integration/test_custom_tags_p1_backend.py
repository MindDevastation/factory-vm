from __future__ import annotations

import importlib
import json
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsP1Backend(unittest.TestCase):
    def _seed_analyzed_track(self, env: Env, *, track_id: str, channel_slug: str, voice_flag: bool) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (channel_slug, track_id, f"gid-{track_id}", "gdrive", "f.wav", "title", "artist", 12.0, 1000.0, 1001.0),
            )
            track_pk = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (track_pk, json.dumps({"voice_flag": voice_flag}), 1001.0),
            )
            conn.execute(
                "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (track_pk, json.dumps({"yamnet_tags": ["Music"]}), 1001.0),
            )
            conn.execute(
                "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (track_pk, json.dumps({"energy": 0.9}), 1001.0),
            )
            return track_pk
        finally:
            conn.close()

    def test_usage_preview_and_reassign_execute(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            self.assertEqual(
                client.post("/v1/channels", headers=h, json={"slug": "deep-space", "display_name": "Deep Space"}).status_code,
                200,
            )

            create_tag = client.post(
                "/v1/track-catalog/custom-tags/catalog",
                headers=h,
                json={"code": "cyber_arena", "label": "Cyber Arena", "category": "MOOD", "description": None, "is_active": True},
            )
            self.assertEqual(create_tag.status_code, 200)
            tag_id = int(create_tag.json()["tag"]["id"])

            create_rule = client.post(
                "/v1/track-catalog/custom-tags/rules",
                headers=h,
                json={
                    "tag_id": tag_id,
                    "source_path": "track_features.payload_json.voice_flag",
                    "operator": "equals",
                    "value_json": "false",
                    "match_mode": "ALL",
                    "priority": 100,
                    "required": False,
                    "stop_after_match": False,
                    "is_active": True,
                },
            )
            self.assertEqual(create_rule.status_code, 200)

            matching_track = self._seed_analyzed_track(env, track_id="t1", channel_slug="deep-space", voice_flag=False)
            self._seed_analyzed_track(env, track_id="t2", channel_slug="deep-space", voice_flag=True)

            preview_matches = client.post(
                "/v1/track-catalog/custom-tags/rules/preview-matches",
                headers=h,
                json={
                    "tag_code": "cyber_arena",
                    "rule": {
                        "source_path": "track_features.payload_json.voice_flag",
                        "operator": "equals",
                        "value_json": "false",
                        "match_mode": "ALL",
                        "priority": 100,
                        "required": False,
                        "stop_after_match": False,
                        "is_active": True,
                    },
                    "scope": {"channel_slug": "deep-space"},
                },
            )
            self.assertEqual(preview_matches.status_code, 200)
            self.assertEqual(preview_matches.json()["matched_tracks_count"], 1)

            preview_reassign = client.post(
                "/v1/track-catalog/custom-tags/reassign/preview",
                headers=h,
                json={"channel_slug": "deep-space", "tag_code": "cyber_arena"},
            )
            self.assertEqual(preview_reassign.status_code, 200)
            self.assertEqual(preview_reassign.json()["summary"]["new_assignments"], 1)

            execute_reassign = client.post(
                "/v1/track-catalog/custom-tags/reassign/execute",
                headers=h,
                json={"channel_slug": "deep-space", "tag_code": "cyber_arena"},
            )
            self.assertEqual(execute_reassign.status_code, 200)
            self.assertEqual(execute_reassign.json()["summary"]["new_assignments"], 1)

            listing = client.get("/v1/track-catalog/custom-tags?include_usage=true", headers=h)
            self.assertEqual(listing.status_code, 200)
            tag_item = listing.json()["tags"][0]
            self.assertEqual(tag_item["usage"]["rules_count"], 1)
            self.assertEqual(tag_item["usage"]["tracks_count"], 1)
            self.assertEqual(tag_item["usage"]["channels_count"], 0)

            conn = dbm.connect(env)
            try:
                row = conn.execute(
                    "SELECT state FROM track_custom_tag_assignments WHERE track_pk = ? AND tag_id = ?",
                    (matching_track, tag_id),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(str(row["state"]), "AUTO")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
