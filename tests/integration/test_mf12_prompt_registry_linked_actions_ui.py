from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf12PromptRegistryLinkedActionsUi(unittest.TestCase):
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

    def test_detail_shows_linked_actions_section_and_forms(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf12-links", code="PR-MF12-LINKS", title="MF12 Links")

            detail = client.get(f"/ui/prompt-registry/{prompt_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Linked actions", detail.text)
            self.assertIn("Execution is not available in this foundation slice.", detail.text)
            self.assertIn('name="action_key"', detail.text)
            self.assertIn('name="config_json"', detail.text)

    def test_create_linked_action_succeeds_and_status_update_works(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf12-create", code="PR-MF12-CREATE", title="MF12 Create")

            created = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "open-audit-ui",
                    "action_type": "ui_action",
                    "action_status": "active",
                    "target_kind": "route",
                    "target_ref": f"/ui/prompt-registry/{prompt_id}/audit",
                    "config_json": '{"note":"safe"}',
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)
            self.assertIn(f"/ui/prompt-registry/{prompt_id}", created.headers["location"])

            detail = client.get(created.headers["location"], headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Success:</strong> Linked action created", detail.text)
            self.assertIn("open-audit-ui", detail.text)

            api_items = client.get(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers).json()["items"]
            self.assertEqual(len(api_items), 1)
            action_id = int(api_items[0]["id"])

            updated = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/{action_id}/status",
                headers=headers,
                data={"action_status": "inactive"},
                follow_redirects=False,
            )
            self.assertEqual(updated.status_code, 303)
            updated_detail = client.get(updated.headers["location"], headers=headers)
            self.assertEqual(updated_detail.status_code, 200)
            self.assertIn("Linked action status updated", updated_detail.text)

            active_only = client.get(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions?include_inactive=false",
                headers=headers,
            )
            self.assertEqual(active_only.status_code, 200)
            self.assertEqual(active_only.json()["items"], [])

    def test_invalid_config_json_shows_safe_error_without_traceback(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf12-invalid", code="PR-MF12-INVALID", title="MF12 Invalid")

            invalid = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "bad-json",
                    "action_type": "ui_action",
                    "target_kind": "route",
                    "config_json": "not-json",
                },
            )
            self.assertEqual(invalid.status_code, 200)
            self.assertIn("Create linked action error:</strong>", invalid.text)
            self.assertIn("config_json must be valid JSON object", invalid.text)
            self.assertNotIn("Traceback", invalid.text)

    def test_secret_like_config_key_is_rejected_without_echoing_secret_value(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf12-secret", code="PR-MF12-SECRET", title="MF12 Secret")
            secret_marker = "SECRET-MF12-TOPLEVEL"

            rejected = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "secret-top-level",
                    "action_type": "ui_action",
                    "target_kind": "route",
                    "config_json": f'{{"api_token":"{secret_marker}"}}',
                },
            )
            self.assertEqual(rejected.status_code, 200)
            self.assertIn("Create linked action error:</strong>", rejected.text)
            self.assertIn("secret/token/password-like keys", rejected.text)
            self.assertNotIn(secret_marker, rejected.text)
            self.assertNotIn('"api_token"', rejected.text)
            self.assertIn('name="config_json"', rejected.text)
            self.assertIn("<textarea", rejected.text)
            self.assertIn(">{}</textarea>", rejected.text)
            self.assertNotIn("Traceback", rejected.text)

    def test_nested_secret_like_config_key_is_rejected_without_echoing_secret_value(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf12-secret-nested", code="PR-MF12-SECN", title="MF12 Secret Nested")
            secret_marker = "SECRET-MF12-NESTED"

            rejected = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "secret-nested",
                    "action_type": "ui_action",
                    "target_kind": "route",
                    "config_json": f'{{"meta":{{"authToken":"{secret_marker}"}}}}',
                },
            )
            self.assertEqual(rejected.status_code, 200)
            self.assertIn("Create linked action error:</strong>", rejected.text)
            self.assertIn("secret/token/password-like keys", rejected.text)
            self.assertNotIn(secret_marker, rejected.text)
            self.assertNotIn("authToken", rejected.text)
            self.assertIn(">{}</textarea>", rejected.text)
            self.assertNotIn("Traceback", rejected.text)


if __name__ == "__main__":
    unittest.main()
