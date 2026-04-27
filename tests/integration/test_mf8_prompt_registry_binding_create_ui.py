from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf8PromptRegistryBindingCreateUi(unittest.TestCase):
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

    def test_create_global_binding_from_detail_succeeds_and_renders_in_detail(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf8-global", code="PR-MF8-GLB", title="MF8 Global")

            created = client.post(
                f"/ui/prompt-registry/{prompt_id}/bindings/create",
                headers=headers,
                data={
                    "binding_scope": "global",
                    "binding_status": "active",
                    "workflow_slug": "",
                    "channel_slug": "",
                    "item_type": "",
                    "item_ref": "",
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)
            self.assertIn(f"/ui/prompt-registry/{prompt_id}", created.headers["location"])

            detail = client.get(created.headers["location"], headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Success:</strong> Binding created", detail.text)
            self.assertIn("<td>global</td>", detail.text)

    def test_create_workflow_binding_succeeds(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf8-workflow", code="PR-MF8-WF", title="MF8 Workflow")

            created = client.post(
                f"/ui/prompt-registry/{prompt_id}/bindings/create",
                headers=headers,
                data={
                    "binding_scope": "workflow",
                    "binding_status": "active",
                    "workflow_slug": "daily-run",
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)

            listed = client.get(f"/v1/prompt-registry/bindings?prompt_id={prompt_id}", headers=headers)
            self.assertEqual(listed.status_code, 200)
            items = listed.json()["items"]
            self.assertEqual(len(items), 1)
            self.assertEqual(str(items[0]["binding_scope"]), "workflow")
            self.assertEqual(str(items[0]["workflow_slug"]), "daily-run")

    def test_create_channel_binding_succeeds(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf8-channel", code="PR-MF8-CH", title="MF8 Channel")

            created = client.post(
                f"/ui/prompt-registry/{prompt_id}/bindings/create",
                headers=headers,
                data={
                    "binding_scope": "channel",
                    "binding_status": "active",
                    "channel_slug": "news_feed",
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)

            listed = client.get(f"/v1/prompt-registry/bindings?prompt_id={prompt_id}", headers=headers)
            self.assertEqual(listed.status_code, 200)
            items = listed.json()["items"]
            self.assertEqual(len(items), 1)
            self.assertEqual(str(items[0]["binding_scope"]), "channel")
            self.assertEqual(str(items[0]["channel_slug"]), "news_feed")

    def test_create_item_binding_succeeds(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf8-item", code="PR-MF8-IT", title="MF8 Item")

            created = client.post(
                f"/ui/prompt-registry/{prompt_id}/bindings/create",
                headers=headers,
                data={
                    "binding_scope": "item",
                    "binding_status": "active",
                    "item_type": "video",
                    "item_ref": "abc123",
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)

            listed = client.get(f"/v1/prompt-registry/bindings?prompt_id={prompt_id}", headers=headers)
            self.assertEqual(listed.status_code, 200)
            items = listed.json()["items"]
            self.assertEqual(len(items), 1)
            self.assertEqual(str(items[0]["binding_scope"]), "item")
            self.assertEqual(str(items[0]["item_type"]), "video")
            self.assertEqual(str(items[0]["item_ref"]), "abc123")

    def test_invalid_scope_payload_shows_safe_error_without_traceback(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf8-invalid", code="PR-MF8-BAD", title="MF8 Invalid")

            invalid = client.post(
                f"/ui/prompt-registry/{prompt_id}/bindings/create",
                headers=headers,
                data={
                    "binding_scope": "workflow",
                    "binding_status": "active",
                    "channel_slug": "should-not-be-here",
                },
            )
            self.assertEqual(invalid.status_code, 200)
            self.assertIn("Create binding error:</strong>", invalid.text)
            self.assertIn("workflow binding_scope requires workflow_slug", invalid.text)
            self.assertNotIn("Traceback", invalid.text)

    def test_duplicate_active_binding_shows_safe_error_without_traceback(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf8-dup", code="PR-MF8-DUP", title="MF8 Duplicate")

            first = client.post(
                f"/ui/prompt-registry/{prompt_id}/bindings/create",
                headers=headers,
                data={
                    "binding_scope": "global",
                    "binding_status": "active",
                },
                follow_redirects=False,
            )
            self.assertEqual(first.status_code, 303)

            duplicate = client.post(
                f"/ui/prompt-registry/{prompt_id}/bindings/create",
                headers=headers,
                data={
                    "binding_scope": "global",
                    "binding_status": "active",
                },
            )
            self.assertEqual(duplicate.status_code, 200)
            self.assertIn("Create binding error:</strong>", duplicate.text)
            self.assertIn("duplicate active binding", duplicate.text)
            self.assertNotIn("Traceback", duplicate.text)


if __name__ == "__main__":
    unittest.main()
