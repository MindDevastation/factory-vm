from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsRulesApi(unittest.TestCase):
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

    def test_rules_and_channel_bindings_crud(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            visual_tag_id = self._create_tag(client, h, code="nebula", category="VISUAL")
            mood_tag_id = self._create_tag(client, h, code="calm", category="MOOD")

            rules_empty = client.get(f"/v1/track-catalog/custom-tags/rules?tag_id={visual_tag_id}", headers=h)
            self.assertEqual(rules_empty.status_code, 200)
            self.assertEqual(rules_empty.json(), {"rules": []})

            created_rule = client.post(
                "/v1/track-catalog/custom-tags/rules",
                headers=h,
                json={
                    "tag_id": visual_tag_id,
                    "source_path": "track_features.payload_json.voice_flag",
                    "operator": "equals",
                    "value_json": "false",
                    "match_mode": "ALL",
                    "priority": 100,
                    "weight": 2.0,
                    "required": True,
                    "stop_after_match": False,
                    "is_active": True,
                },
            )
            self.assertEqual(created_rule.status_code, 200)
            rule_id = int(created_rule.json()["rule"]["id"])
            self.assertEqual(created_rule.json()["rule"]["value_json"], "false")

            patched_rule = client.patch(
                f"/v1/track-catalog/custom-tags/rules/{rule_id}",
                headers=h,
                json={"operator": "contains", "value_json": '"vox"', "match_mode": "ANY"},
            )
            self.assertEqual(patched_rule.status_code, 200)
            self.assertEqual(patched_rule.json()["rule"]["operator"], "contains")
            self.assertEqual(patched_rule.json()["rule"]["match_mode"], "ANY")

            listed_rules = client.get(f"/v1/track-catalog/custom-tags/rules?tag_id={visual_tag_id}", headers=h)
            self.assertEqual(listed_rules.status_code, 200)
            self.assertEqual(len(listed_rules.json()["rules"]), 1)

            deleted_rule = client.delete(f"/v1/track-catalog/custom-tags/rules/{rule_id}", headers=h)
            self.assertEqual(deleted_rule.status_code, 200)
            self.assertEqual(deleted_rule.json(), {"ok": True})

            self._create_channel(client, h, slug="celestial-pulse")

            created_binding = client.post(
                "/v1/track-catalog/custom-tags/channel-bindings",
                headers=h,
                json={"tag_id": visual_tag_id, "channel_slug": "celestial-pulse"},
            )
            self.assertEqual(created_binding.status_code, 200)
            binding_id = int(created_binding.json()["binding"]["id"])

            list_bindings = client.get(
                f"/v1/track-catalog/custom-tags/channel-bindings?tag_id={visual_tag_id}", headers=h
            )
            self.assertEqual(list_bindings.status_code, 200)
            self.assertEqual(len(list_bindings.json()["bindings"]), 1)

            non_visual = client.post(
                "/v1/track-catalog/custom-tags/channel-bindings",
                headers=h,
                json={"tag_id": mood_tag_id, "channel_slug": "celestial-pulse"},
            )
            self.assertEqual(non_visual.status_code, 400)
            self.assertEqual(non_visual.json()["error"]["code"], "CTA_INVALID_INPUT")

            missing_channel = client.post(
                "/v1/track-catalog/custom-tags/channel-bindings",
                headers=h,
                json={"tag_id": visual_tag_id, "channel_slug": "unknown-slug"},
            )
            self.assertEqual(missing_channel.status_code, 400)
            self.assertEqual(missing_channel.json()["error"]["code"], "CTA_INVALID_INPUT")

            deleted_binding = client.delete(
                f"/v1/track-catalog/custom-tags/channel-bindings/{binding_id}", headers=h
            )
            self.assertEqual(deleted_binding.status_code, 200)
            self.assertEqual(deleted_binding.json(), {"ok": True})

    def test_rules_validation_and_not_found_errors(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            tag_not_found = client.get("/v1/track-catalog/custom-tags/rules?tag_id=99999", headers=h)
            self.assertEqual(tag_not_found.status_code, 404)
            self.assertEqual(tag_not_found.json()["error"]["code"], "CTA_TAG_NOT_FOUND")

            tag_id = self._create_tag(client, h, code="void", category="VISUAL")

            invalid_operator = client.post(
                "/v1/track-catalog/custom-tags/rules",
                headers=h,
                json={
                    "tag_id": tag_id,
                    "source_path": "track_features.payload_json.voice_flag",
                    "operator": "EQUALS",
                    "value_json": "false",
                    "match_mode": "ALL",
                    "priority": 100,
                    "required": False,
                    "stop_after_match": False,
                    "is_active": True,
                },
            )
            self.assertEqual(invalid_operator.status_code, 400)
            self.assertEqual(invalid_operator.json()["error"]["code"], "CTA_INVALID_INPUT")

            invalid_value_json = client.post(
                "/v1/track-catalog/custom-tags/rules",
                headers=h,
                json={
                    "tag_id": tag_id,
                    "source_path": "track_features.payload_json.voice_flag",
                    "operator": "equals",
                    "value_json": "not-json",
                    "match_mode": "ALL",
                    "priority": 100,
                    "required": False,
                    "stop_after_match": False,
                    "is_active": True,
                },
            )
            self.assertEqual(invalid_value_json.status_code, 400)
            self.assertEqual(invalid_value_json.json()["error"]["code"], "CTA_INVALID_INPUT")

            invalid_match_mode = client.post(
                "/v1/track-catalog/custom-tags/rules",
                headers=h,
                json={
                    "tag_id": tag_id,
                    "source_path": "track_features.payload_json.voice_flag",
                    "operator": "equals",
                    "value_json": "false",
                    "match_mode": "SOME",
                    "priority": 100,
                    "required": False,
                    "stop_after_match": False,
                    "is_active": True,
                },
            )
            self.assertEqual(invalid_match_mode.status_code, 400)
            self.assertEqual(invalid_match_mode.json()["error"]["code"], "CTA_INVALID_INPUT")


if __name__ == "__main__":
    unittest.main()
