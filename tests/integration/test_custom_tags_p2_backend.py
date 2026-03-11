from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsP2Backend(unittest.TestCase):
    def _seed_track(self, env: Env, *, channel_slug: str, track_id: str, tag_id: int) -> int:
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
                "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                (track_pk, tag_id, "AUTO", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
            )
            return track_pk
        finally:
            conn.close()

    def test_clone_bulk_taxonomy_and_dashboard(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            client.post("/v1/channels", headers=h, json={"slug": "ctux-ch", "display_name": "CTUX Channel"})

            visual = client.post(
                "/v1/track-catalog/custom-tags/catalog",
                headers=h,
                json={"code": "neon", "label": "Neon", "category": "VISUAL", "description": None, "is_active": True},
            )
            mood = client.post(
                "/v1/track-catalog/custom-tags/catalog",
                headers=h,
                json={"code": "calm", "label": "Calm", "category": "MOOD", "description": None, "is_active": True},
            )
            self.assertEqual(visual.status_code, 200)
            self.assertEqual(mood.status_code, 200)
            visual_id = int(visual.json()["tag"]["id"])
            mood_id = int(mood.json()["tag"]["id"])

            rule = client.post(
                "/v1/track-catalog/custom-tags/rules",
                headers=h,
                json={
                    "tag_id": mood_id,
                    "source_path": "track_scores.payload_json.energy",
                    "operator": "gte",
                    "value_json": "0.8",
                    "match_mode": "ALL",
                    "priority": 80,
                    "required": False,
                    "stop_after_match": False,
                    "is_active": True,
                },
            )
            self.assertEqual(rule.status_code, 200)
            rule_id = int(rule.json()["rule"]["id"])

            bind = client.post(
                "/v1/track-catalog/custom-tags/channel-bindings",
                headers=h,
                json={"tag_id": visual_id, "channel_slug": "ctux-ch"},
            )
            self.assertEqual(bind.status_code, 200)

            cloned = client.post(
                f"/v1/track-catalog/custom-tags/{visual_id}/clone",
                headers=h,
                json={"code": "neon_clone", "label": "Neon Clone", "include_rules": True, "include_bindings": True, "is_active": True},
            )
            self.assertEqual(cloned.status_code, 200)
            clone_id = int(cloned.json()["tag"]["id"])
            self.assertEqual(cloned.json()["cloned_bindings"], 1)

            clone_rules = client.post(
                f"/v1/track-catalog/custom-tags/{clone_id}/rules/clone",
                headers=h,
                json={"source_tag_id": mood_id, "replace_all": True},
            )
            self.assertEqual(clone_rules.status_code, 200)
            self.assertEqual(clone_rules.json()["cloned_rules"], 1)

            bulk_tags = client.post(
                "/v1/track-catalog/custom-tags/tags/bulk-set-active",
                headers=h,
                json={"ids": [visual_id], "is_active": False},
            )
            self.assertEqual(bulk_tags.status_code, 200)
            self.assertEqual(bulk_tags.json()["updated"], 1)

            bulk_rules = client.post(
                "/v1/track-catalog/custom-tags/rules/bulk-set-active",
                headers=h,
                json={"ids": [rule_id], "is_active": False},
            )
            self.assertEqual(bulk_rules.status_code, 200)

            bulk_bind = client.post(
                "/v1/track-catalog/custom-tags/bindings/bulk-set-enabled",
                headers=h,
                json={"items": [{"tag_id": visual_id, "channel_slug": "ctux-ch", "is_enabled": False}]},
            )
            self.assertEqual(bulk_bind.status_code, 200)
            self.assertEqual(bulk_bind.json()["disabled"], 1)

            exported = client.get("/v1/track-catalog/custom-tags/taxonomy/export", headers=h)
            self.assertEqual(exported.status_code, 200)
            body = exported.json()
            self.assertIn("tags", body)
            self.assertIn("bindings", body)
            self.assertIn("rules", body)

            preview = client.post("/v1/track-catalog/custom-tags/taxonomy/import/preview", headers=h, json=body)
            self.assertEqual(preview.status_code, 200)
            self.assertTrue(preview.json()["can_confirm"])

            confirm = client.post("/v1/track-catalog/custom-tags/taxonomy/import/confirm", headers=h, json=body)
            self.assertEqual(confirm.status_code, 200)
            self.assertTrue(confirm.json()["can_confirm"])

            client.post("/v1/channels", headers=h, json={"slug": "ctux-ch-b", "display_name": "CTUX Channel B"})
            client.post(
                "/v1/track-catalog/custom-tags/channel-bindings",
                headers=h,
                json={"tag_id": clone_id, "channel_slug": "ctux-ch-b"},
            )

            self._seed_track(env, channel_slug="ctux-ch", track_id="t-a", tag_id=clone_id)
            self._seed_track(env, channel_slug="ctux-ch-b", track_id="t-b", tag_id=clone_id)
            dash = client.get("/v1/track-catalog/custom-tags/dashboard/ctux-ch", headers=h)
            self.assertEqual(dash.status_code, 200)
            payload = dash.json()
            self.assertEqual(payload["channel_slug"], "ctux-ch")
            self.assertTrue(any(item["code"] == "neon_clone" for item in payload["visual_tags"]))
            clone_visual = next(item for item in payload["visual_tags"] if item["code"] == "neon_clone")
            self.assertEqual(clone_visual["tracks_count"], 1)
            clone_usage = next(item for item in payload["tag_usage"] if item["tag_code"] == "neon_clone")
            self.assertEqual(clone_usage["tracks_count"], 1)

            dash_b = client.get("/v1/track-catalog/custom-tags/dashboard/ctux-ch-b", headers=h)
            self.assertEqual(dash_b.status_code, 200)
            payload_b = dash_b.json()
            clone_visual_b = next(item for item in payload_b["visual_tags"] if item["code"] == "neon_clone")
            self.assertEqual(clone_visual_b["tracks_count"], 1)

            by_channel = client.get("/v1/track-catalog/custom-tags/bindings/by-channel/ctux-ch", headers=h)
            self.assertEqual(by_channel.status_code, 200)
            by_channel_body = by_channel.json()
            self.assertEqual(by_channel_body["channel_slug"], "ctux-ch")
            self.assertGreaterEqual(len(by_channel_body["bindings"]), 1)
            self.assertTrue(all(item["tag_category"] == "VISUAL" for item in by_channel_body["bindings"]))
            self.assertTrue(all(item["channel_slug"] == "ctux-ch" for item in by_channel_body["bindings"]))

            missing_channel = client.get("/v1/track-catalog/custom-tags/bindings/by-channel/nope", headers=h)
            self.assertEqual(missing_channel.status_code, 400)
            self.assertEqual(missing_channel.json().get("error", {}).get("code"), "CTA_INVALID_INPUT")


if __name__ == "__main__":
    unittest.main()
