from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsEditModalBackend(unittest.TestCase):
    def _create_tag(self, client: TestClient, headers: dict[str, str], *, code: str, category: str) -> int:
        resp = client.post(
            "/v1/track-catalog/custom-tags/catalog",
            headers=headers,
            json={
                "code": code,
                "label": code.title(),
                "category": category,
                "description": None,
                "is_active": True,
            },
        )
        self.assertEqual(resp.status_code, 200)
        return int(resp.json()["tag"]["id"])

    def _create_channel(self, client: TestClient, headers: dict[str, str], slug: str) -> None:
        resp = client.post(
            "/v1/channels",
            headers=headers,
            json={"slug": slug, "display_name": slug.replace("-", " ").title()},
        )
        self.assertEqual(resp.status_code, 200)

    def test_modal_rules_endpoints_and_replace_all_atomicity(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            tag_id = self._create_tag(client, h, code="starlight", category="VISUAL")

            created = client.post(
                f"/v1/track-catalog/custom-tags/{tag_id}/rules",
                headers=h,
                json={
                    "source_path": "track_features.payload_json.voice_flag",
                    "operator": "equals",
                    "value_json": "false",
                    "match_mode": "ALL",
                    "priority": 100,
                    "weight": 1.25,
                    "required": True,
                    "stop_after_match": False,
                    "is_active": True,
                },
            )
            self.assertEqual(created.status_code, 200)
            rule = created.json()["rule"]
            rule_id = int(rule["id"])
            for key in [
                "id",
                "source_path",
                "operator",
                "value_json",
                "priority",
                "weight",
                "required",
                "stop_after_match",
                "is_active",
                "match_mode",
                "summary",
            ]:
                self.assertIn(key, rule)

            listed = client.get(f"/v1/track-catalog/custom-tags/{tag_id}/rules", headers=h)
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(len(listed.json()["rules"]), 1)
            self.assertTrue(listed.json()["rules"][0]["summary"])

            patched = client.patch(
                f"/v1/track-catalog/custom-tags/{tag_id}/rules/{rule_id}",
                headers=h,
                json={"operator": "contains", "value_json": '"vox"', "match_mode": "ANY"},
            )
            self.assertEqual(patched.status_code, 200)
            self.assertEqual(patched.json()["rule"]["operator"], "contains")

            invalid_replace = client.put(
                f"/v1/track-catalog/custom-tags/{tag_id}/rules/replace-all",
                headers=h,
                json={
                    "rules": [
                        {
                            "source_path": "track_features.payload_json.voice_flag",
                            "operator": "equals",
                            "value_json": "false",
                            "priority": 110,
                            "required": False,
                            "stop_after_match": False,
                            "is_active": True,
                        },
                        {
                            "source_path": "track_features.payload_json.voice_flag",
                            "operator": "bad-op",
                            "value_json": "false",
                            "priority": 90,
                            "required": False,
                            "stop_after_match": False,
                            "is_active": True,
                        },
                    ]
                },
            )
            self.assertEqual(invalid_replace.status_code, 400)
            self.assertEqual(invalid_replace.json()["error"]["code"], "CTU_INVALID_PAYLOAD")

            listed_after_invalid = client.get(f"/v1/track-catalog/custom-tags/{tag_id}/rules", headers=h)
            self.assertEqual(listed_after_invalid.status_code, 200)
            self.assertEqual(len(listed_after_invalid.json()["rules"]), 1)
            self.assertEqual(int(listed_after_invalid.json()["rules"][0]["id"]), rule_id)

            malformed_item_replace = client.put(
                f"/v1/track-catalog/custom-tags/{tag_id}/rules/replace-all",
                headers=h,
                json={"rules": ["bad"]},
            )
            self.assertEqual(malformed_item_replace.status_code, 400)
            self.assertEqual(malformed_item_replace.json()["error"]["code"], "CTU_INVALID_PAYLOAD")

            listed_after_malformed = client.get(f"/v1/track-catalog/custom-tags/{tag_id}/rules", headers=h)
            self.assertEqual(listed_after_malformed.status_code, 200)
            self.assertEqual(len(listed_after_malformed.json()["rules"]), 1)
            self.assertEqual(int(listed_after_malformed.json()["rules"][0]["id"]), rule_id)

            valid_replace = client.put(
                f"/v1/track-catalog/custom-tags/{tag_id}/rules/replace-all",
                headers=h,
                json={
                    "rules": [
                        {
                            "source_path": "track_features.payload_json.voice_flag",
                            "operator": "equals",
                            "value_json": "false",
                            "priority": 110,
                            "required": False,
                            "stop_after_match": False,
                            "is_active": True,
                        },
                        {
                            "source_path": "track_scores.payload_json.scene_match",
                            "operator": "gte",
                            "value_json": "0.8",
                            "priority": 90,
                            "required": False,
                            "stop_after_match": False,
                            "is_active": True,
                        },
                    ]
                },
            )
            self.assertEqual(valid_replace.status_code, 200)
            self.assertEqual(len(valid_replace.json()["rules"]), 2)

            deleted = client.delete(f"/v1/track-catalog/custom-tags/{tag_id}/rules/{rule_id}", headers=h)
            self.assertEqual(deleted.status_code, 404)
            self.assertEqual(deleted.json()["error"]["code"], "CTU_RULE_NOT_FOUND")

    def test_modal_bindings_read_write_with_category_rules(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            visual_tag_id = self._create_tag(client, h, code="prism", category="VISUAL")
            mood_tag_id = self._create_tag(client, h, code="serene", category="MOOD")
            self._create_channel(client, h, slug="neon-grid")
            self._create_channel(client, h, slug="vapor-core")

            replaced = client.put(
                f"/v1/track-catalog/custom-tags/{visual_tag_id}/bindings",
                headers=h,
                json={"channel_slugs": ["neon-grid", "vapor-core"]},
            )
            self.assertEqual(replaced.status_code, 200)
            self.assertEqual(len(replaced.json()["bindings"]), 2)

            listed = client.get(f"/v1/track-catalog/custom-tags/{visual_tag_id}/bindings", headers=h)
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(len(listed.json()["bindings"]), 2)

            mood_blocked = client.put(
                f"/v1/track-catalog/custom-tags/{mood_tag_id}/bindings",
                headers=h,
                json={"channel_slugs": ["neon-grid"]},
            )
            self.assertEqual(mood_blocked.status_code, 400)
            self.assertEqual(mood_blocked.json()["error"]["code"], "CTU_BINDING_NOT_ALLOWED_FOR_CATEGORY")

            missing_tag = client.get("/v1/track-catalog/custom-tags/99999/rules", headers=h)
            self.assertEqual(missing_tag.status_code, 404)
            self.assertEqual(missing_tag.json()["error"]["code"], "CTU_TAG_NOT_FOUND")

            invalid_payload = client.put(
                f"/v1/track-catalog/custom-tags/{visual_tag_id}/rules/replace-all",
                headers=h,
                json={"rules": {"bad": True}},
            )
            self.assertEqual(invalid_payload.status_code, 400)
            self.assertEqual(invalid_payload.json()["error"]["code"], "CTU_INVALID_PAYLOAD")
