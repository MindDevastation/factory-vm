from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf7PromptRegistryUiManagementActions(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _create_record(self, client: TestClient, headers: dict[str, str], *, slug: str, code: str, title: str) -> int:
        created = client.post(
            "/v1/prompt-registry/records",
            headers=headers,
            json={
                "slug": slug,
                "code": code,
                "title": title,
                "record_type": "prompt_template",
                "status": "draft",
            },
        )
        self.assertEqual(created.status_code, 200)
        return int(created.json()["id"])

    def _create_version(self, client: TestClient, headers: dict[str, str], *, prompt_id: int, body_text: str) -> int:
        created = client.post(
            f"/v1/prompt-registry/records/{prompt_id}/versions",
            headers=headers,
            json={
                "body_text": body_text,
                "variables": [
                    {"name": "title", "safety_class": "standard", "required": True},
                    {"name": "secret_token", "safety_class": "secret", "required": False, "default_value": "RAW-SECRET"},
                ],
            },
        )
        self.assertEqual(created.status_code, 200)
        return int(created.json()["id"])

    def test_create_record_ui_action_and_safe_error(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            created = client.post(
                "/ui/prompt-registry/create-record",
                headers=headers,
                data={
                    "slug": "mf7-ui-create",
                    "code": "PR-MF7-CREATE",
                    "title": "MF7 Create UI",
                    "record_type": "prompt_template",
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)
            self.assertIn("/ui/prompt-registry/", created.headers["location"])

            detail = client.get(created.headers["location"], headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Success:</strong> Record created", detail.text)
            self.assertIn("MF7 Create UI", detail.text)

            conflict = client.post(
                "/ui/prompt-registry/create-record",
                headers=headers,
                data={
                    "slug": "mf7-ui-create",
                    "code": "PR-MF7-CREATE-2",
                    "title": "MF7 Duplicate",
                    "record_type": "prompt_template",
                },
            )
            self.assertEqual(conflict.status_code, 200)
            self.assertIn("Create record error:</strong>", conflict.text)
            self.assertNotIn("Traceback", conflict.text)

    def test_create_version_ui_action_masks_defaults_and_invalid_json_safe_error(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf7-ver", code="PR-MF7-VER", title="MF7 Version")

            created = client.post(
                f"/ui/prompt-registry/{prompt_id}/versions/create",
                headers=headers,
                data={
                    "body_text": "Hello {{title}}",
                    "variables_json": '[{"name":"title","safety_class":"standard","required":true,"default_value":"","description":""},{"name":"secret_token","safety_class":"secret","required":false,"default_value":"RAW-SECRET","description":""}]',
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)

            detail = client.get(created.headers["location"], headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Success:</strong> Version created", detail.text)
            self.assertIn("***MASKED***", detail.text)
            self.assertNotIn("RAW-SECRET", detail.text)

            invalid = client.post(
                f"/ui/prompt-registry/{prompt_id}/versions/create",
                headers=headers,
                data={
                    "body_text": "Hello {{title}}",
                    "variables_json": '{"not":"a-list"}',
                },
            )
            self.assertEqual(invalid.status_code, 200)
            self.assertIn("Create version error:</strong> variables JSON must be a valid list of objects", invalid.text)
            self.assertNotIn("Traceback", invalid.text)

    def test_activate_version_and_cross_prompt_rejection_without_side_effects(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_a = self._create_record(client, headers, slug="mf7-act-a", code="PR-MF7-ACT-A", title="MF7 A")
            prompt_b = self._create_record(client, headers, slug="mf7-act-b", code="PR-MF7-ACT-B", title="MF7 B")
            version_a = self._create_version(client, headers, prompt_id=prompt_a, body_text="A {{title}}")
            version_b = self._create_version(client, headers, prompt_id=prompt_b, body_text="B {{title}}")

            activated = client.post(
                f"/ui/prompt-registry/{prompt_a}/versions/{version_a}/activate",
                headers=headers,
                follow_redirects=False,
            )
            self.assertEqual(activated.status_code, 303)
            detail_a = client.get(activated.headers["location"], headers=headers)
            self.assertIn("Success:</strong> Version activated", detail_a.text)

            record_a = client.get(f"/v1/prompt-registry/records/{prompt_a}", headers=headers)
            self.assertEqual(record_a.status_code, 200)
            self.assertEqual(int(record_a.json()["active_version_id"]), version_a)

            cross = client.post(
                f"/ui/prompt-registry/{prompt_a}/versions/{version_b}/activate",
                headers=headers,
                follow_redirects=False,
            )
            self.assertEqual(cross.status_code, 303)
            self.assertIn("error=version_id+does+not+belong+to+the+current+prompt", cross.headers["location"])

            record_b = client.get(f"/v1/prompt-registry/records/{prompt_b}", headers=headers)
            self.assertEqual(record_b.status_code, 200)
            self.assertIsNone(record_b.json()["active_version_id"])

    def test_binding_status_update_and_cross_prompt_rejection_without_side_effects(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_a = self._create_record(client, headers, slug="mf7-bind-a", code="PR-MF7-BIND-A", title="MF7 Bind A")
            prompt_b = self._create_record(client, headers, slug="mf7-bind-b", code="PR-MF7-BIND-B", title="MF7 Bind B")

            binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": prompt_b,
                    "binding_scope": "global",
                    "binding_status": "active",
                },
            )
            self.assertEqual(binding.status_code, 200)
            binding_id = int(binding.json()["id"])

            updated = client.post(
                f"/ui/prompt-registry/{prompt_b}/bindings/{binding_id}/status",
                headers=headers,
                data={"binding_status": "inactive"},
                follow_redirects=False,
            )
            self.assertEqual(updated.status_code, 303)

            list_b = client.get(f"/v1/prompt-registry/bindings?prompt_id={prompt_b}", headers=headers)
            self.assertEqual(list_b.status_code, 200)
            self.assertEqual(str(list_b.json()["items"][0]["binding_status"]), "inactive")

            cross = client.post(
                f"/ui/prompt-registry/{prompt_a}/bindings/{binding_id}/status",
                headers=headers,
                data={"binding_status": "active"},
                follow_redirects=False,
            )
            self.assertEqual(cross.status_code, 303)
            self.assertIn("error=binding_id+does+not+belong+to+the+current+prompt", cross.headers["location"])

            list_b_after = client.get(f"/v1/prompt-registry/bindings?prompt_id={prompt_b}", headers=headers)
            self.assertEqual(list_b_after.status_code, 200)
            self.assertEqual(str(list_b_after.json()["items"][0]["binding_status"]), "inactive")


if __name__ == "__main__":
    unittest.main()
