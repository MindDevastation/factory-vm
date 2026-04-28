from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf14PromptRegistryLinkedActionGuardrailsUi(unittest.TestCase):
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

    def test_preview_page_renders_execution_request_forms_and_creates_preview_only(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf14-preview-only", code="PR-MF14-PO", title="MF14 Preview Only")

            created = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "mf14-preview-only-action",
                    "action_type": "ui_action",
                    "action_status": "active",
                    "target_kind": "route",
                    "target_ref": f"/ui/prompt-registry/{prompt_id}/audit",
                    "config_json": '{"note":"safe"}',
                },
                follow_redirects=False,
            )
            self.assertEqual(created.status_code, 303)
            action_id = int(client.get(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers).json()["items"][0]["id"])

            preview = client.get(f"/ui/prompt-registry/{prompt_id}/linked-actions/{action_id}/preview", headers=headers)
            self.assertEqual(preview.status_code, 200)
            self.assertIn("Create preview-only request", preview.text)
            self.assertIn("Accept guarded execution request", preview.text)
            self.assertIn("No runtime execution is performed in this slice.", preview.text)

            created_request = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/{action_id}/execution-requests",
                headers=headers,
                data={"request_mode": "preview_only", "request_context_json": '{"reason":"dry-run"}'},
                follow_redirects=False,
            )
            self.assertEqual(created_request.status_code, 303)
            self.assertIn(f"/ui/prompt-registry/{prompt_id}/linked-actions/{action_id}/preview", created_request.headers["location"])

            refreshed = client.get(f"/ui/prompt-registry/{prompt_id}/linked-actions/{action_id}/preview", headers=headers)
            self.assertEqual(refreshed.status_code, 200)
            self.assertIn("preview_only", refreshed.text)
            self.assertIn("No runtime execution is performed in this slice.", refreshed.text)

    def test_accept_request_becomes_accepted_or_blocked_by_preview_guardrails(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf14-accept", code="PR-MF14-AC", title="MF14 Accept")

            active = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "mf14-accept-active",
                    "action_type": "ui_action",
                    "action_status": "active",
                    "target_kind": "route",
                    "target_ref": f"/ui/prompt-registry/{prompt_id}/audit",
                    "config_json": '{"note":"safe"}',
                },
            )
            self.assertEqual(active.status_code, 200)
            inactive = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
                headers=headers,
                data={
                    "action_key": "mf14-accept-inactive",
                    "action_type": "ui_action",
                    "action_status": "inactive",
                    "target_kind": "route",
                    "target_ref": f"/ui/prompt-registry/{prompt_id}/audit",
                    "config_json": '{"note":"safe"}',
                },
            )
            self.assertEqual(inactive.status_code, 200)
            items = client.get(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers).json()["items"]
            active_id = int([item for item in items if item["action_key"] == "mf14-accept-active"][0]["id"])
            inactive_id = int([item for item in items if item["action_key"] == "mf14-accept-inactive"][0]["id"])

            accepted = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/{active_id}/execution-requests",
                headers=headers,
                data={"request_mode": "accept", "request_context_json": '{"reason":"operator-confirm"}'},
            )
            self.assertEqual(accepted.status_code, 200)
            self.assertIn("accepted", accepted.text)

            blocked = client.post(
                f"/ui/prompt-registry/{prompt_id}/linked-actions/{inactive_id}/execution-requests",
                headers=headers,
                data={"request_mode": "accept", "request_context_json": '{"reason":"operator-confirm"}'},
            )
            self.assertEqual(blocked.status_code, 200)
            self.assertIn("blocked", blocked.text)
            self.assertNotIn("Traceback", blocked.text)


if __name__ == "__main__":
    unittest.main()
