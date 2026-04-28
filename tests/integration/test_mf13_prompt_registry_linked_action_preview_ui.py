from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf13PromptRegistryLinkedActionPreviewUi(unittest.TestCase):
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

    def test_detail_contains_preview_link_and_preview_page_renders_diagnostics(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf13-preview", code="PR-MF13-PREV", title="MF13 Preview")

            created = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "preview-item",
                    "action_type": "ui_action",
                    "action_status": "active",
                    "target_kind": "route",
                    "target_ref": f"/ui/prompt-registry/{prompt_id}/audit",
                    "config_json": '{"note":"safe"}',
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)

            detail = client.get(f"/ui/prompt-registry/{prompt_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            preview_path_fragment = f"/ui/prompt-registry/{prompt_id}/linked-actions/"
            self.assertIn("Preview diagnostics", detail.text)
            self.assertIn(preview_path_fragment, detail.text)

            items = client.get(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers).json()["items"]
            action_id = int(items[0]["id"])
            preview = client.get(f"/ui/prompt-registry/{prompt_id}/linked-actions/{action_id}/preview", headers=headers)
            self.assertEqual(preview.status_code, 200)
            self.assertIn("Linked action preview diagnostics", preview.text)
            self.assertIn("Execution is not available in this foundation slice.", preview.text)
            self.assertIn("preview-item", preview.text)
            self.assertIn("Preview status:</strong> OK", preview.text)
            self.assertIn("Can execute later:</strong> yes", preview.text)
            self.assertIn("LINKED_ACTION_PREVIEW_OK", preview.text)
            self.assertNotIn("Traceback", preview.text)

    def test_cross_prompt_preview_is_rejected_safely(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_a = self._create_record(client, headers, slug="mf13-cross-a", code="PR-MF13-CA", title="MF13 Cross A")
            prompt_b = self._create_record(client, headers, slug="mf13-cross-b", code="PR-MF13-CB", title="MF13 Cross B")

            created = client.post(
                f"/ui/prompt-registry/{prompt_a}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "cross-item",
                    "action_type": "ui_action",
                    "action_status": "active",
                    "target_kind": "route",
                    "target_ref": "/ui/prompt-registry/usage",
                    "config_json": '{"note":"safe"}',
                },
            )
            self.assertEqual(created.status_code, 200)
            action_id = int(client.get(f"/v1/prompt-registry/records/{prompt_a}/linked-actions", headers=headers).json()["items"][0]["id"])

            rejected = client.get(
                f"/ui/prompt-registry/{prompt_b}/linked-actions/{action_id}/preview",
                headers=headers,
                follow_redirects=False,
            )
            self.assertEqual(rejected.status_code, 303)
            self.assertIn(f"/ui/prompt-registry/{prompt_b}", rejected.headers["location"])
            self.assertIn("linked_action_id+does+not+belong+to+the+current+prompt", rejected.headers["location"])


if __name__ == "__main__":
    unittest.main()
