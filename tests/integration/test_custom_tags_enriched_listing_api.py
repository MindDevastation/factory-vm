from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsEnrichedListingApi(unittest.TestCase):
    def test_enriched_listing_filters_and_contract(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            ch = client.post("/v1/channels", headers=h, json={"slug": "deep-space", "display_name": "Deep Space"})
            self.assertEqual(ch.status_code, 200)

            visual = client.post(
                "/v1/track-catalog/custom-tags/catalog",
                headers=h,
                json={"code": "nebula", "label": "Nebula", "category": "VISUAL", "description": "fx", "is_active": True},
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

            rule1 = client.post(
                "/v1/track-catalog/custom-tags/rules",
                headers=h,
                json={
                    "tag_id": visual_id,
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
            rule2 = client.post(
                "/v1/track-catalog/custom-tags/rules",
                headers=h,
                json={
                    "tag_id": visual_id,
                    "source_path": "track_features.payload_json.speech_flag",
                    "operator": "equals",
                    "value_json": "false",
                    "match_mode": "ALL",
                    "priority": 99,
                    "required": False,
                    "stop_after_match": False,
                    "is_active": True,
                },
            )
            self.assertEqual(rule1.status_code, 200)
            self.assertEqual(rule2.status_code, 200)

            binding = client.post(
                "/v1/track-catalog/custom-tags/channel-bindings",
                headers=h,
                json={"tag_id": visual_id, "channel_slug": "deep-space"},
            )
            self.assertEqual(binding.status_code, 200)

            listed = client.get("/v1/track-catalog/custom-tags", headers=h)
            self.assertEqual(listed.status_code, 200)
            tags = listed.json()["tags"]
            self.assertEqual(len(tags), 2)

            visual_item = next(t for t in tags if t["id"] == visual_id)
            self.assertEqual(visual_item["rules_count"], 2)
            self.assertEqual(visual_item["rules_summary"], "2 active rules: voice_flag=false; speech_flag=false")
            self.assertEqual(len(visual_item["bindings"]), 1)

            mood_item = next(t for t in tags if t["id"] == mood_id)
            self.assertEqual(mood_item["bindings"], [])
            self.assertEqual(mood_item["rules_count"], 0)
            self.assertEqual(mood_item["rules_summary"], "No rules")

            filter_by_category = client.get("/v1/track-catalog/custom-tags?category=visual", headers=h)
            self.assertEqual(filter_by_category.status_code, 200)
            self.assertEqual(len(filter_by_category.json()["tags"]), 1)

            filter_by_q = client.get("/v1/track-catalog/custom-tags?q=cal", headers=h)
            self.assertEqual(filter_by_q.status_code, 200)
            self.assertEqual([t["id"] for t in filter_by_q.json()["tags"]], [mood_id])

            filter_by_id = client.get(f"/v1/track-catalog/custom-tags?tag_id={visual_id}", headers=h)
            self.assertEqual(filter_by_id.status_code, 200)
            self.assertEqual([t["id"] for t in filter_by_id.json()["tags"]], [visual_id])

            include_usage = client.get("/v1/track-catalog/custom-tags?include_usage=true", headers=h)
            self.assertEqual(include_usage.status_code, 200)
            self.assertIn("usage", include_usage.json()["tags"][0])

    def test_enriched_listing_validation(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            bad_category = client.get("/v1/track-catalog/custom-tags?category=OTHER", headers=h)
            self.assertEqual(bad_category.status_code, 400)
            self.assertEqual(bad_category.json()["error"]["code"], "CTA_INVALID_INPUT")

            bad_tag_id = client.get("/v1/track-catalog/custom-tags?tag_id=abc", headers=h)
            self.assertEqual(bad_tag_id.status_code, 400)
            self.assertEqual(bad_tag_id.json()["error"]["code"], "CTA_INVALID_INPUT")


if __name__ == "__main__":
    unittest.main()
