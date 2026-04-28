from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf16PromptRegistryDispatchPlanPreviewUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _create_record(self, client: TestClient, headers: dict[str, str], *, slug: str, code: str, title: str) -> int:
        created = client.post(
            "/v1/prompt-registry/records",
            headers=headers,
            json={"slug": slug, "code": code, "title": title, "record_type": "prompt_template", "status": "draft"},
        )
        self.assertEqual(created.status_code, 200)
        return int(created.json()["id"])

    def _create_linked_action(self, client: TestClient, headers: dict[str, str], *, prompt_id: int) -> int:
        created = client.post(
            f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
            headers=headers,
            data={
                "action_key": "mf16-ui-action",
                "action_type": "ui_action",
                "action_status": "active",
                "target_kind": "route",
                "target_ref": "/ui/prompt-registry/linked-action-requests",
                "config_json": '{"note":"safe","ui_label":"go"}',
            },
        )
        self.assertEqual(created.status_code, 200)
        items = client.get(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers).json()["items"]
        return int([item for item in items if item["action_key"] == "mf16-ui-action"][0]["id"])

    def test_review_page_contains_dispatch_preview_link_and_preview_page_is_safe(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            prompt_id = self._create_record(client, headers, slug="mf16-ui", code="PR-MF16-UI", title="MF16 UI")
            action_id = self._create_linked_action(client, headers, prompt_id=prompt_id)

            request_response = client.post(
                f"/v1/prompt-registry/linked-actions/{action_id}/execution-requests",
                headers=headers,
                json={"confirm_execution": True, "request_context_json": {"reason": "approved", "operator": "alice"}},
            )
            self.assertEqual(request_response.status_code, 200)
            request_id = int(request_response.json()["id"])

            review = client.get("/ui/prompt-registry/linked-action-requests", headers=headers)
            self.assertEqual(review.status_code, 200)
            self.assertIn(
                f'href="/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-preview"',
                review.text,
            )

            unauthorized = client.get(f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-preview")
            self.assertEqual(unauthorized.status_code, 401)

            preview_page = client.get(
                f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-preview",
                headers=headers,
            )
            self.assertEqual(preview_page.status_code, 200)
            self.assertIn("This is a dispatch preview only. No runtime execution is performed.", preview_page.text)
            self.assertIn("Dispatch status:", preview_page.text)
            self.assertIn("Dispatch kind:", preview_page.text)
            self.assertIn("Safe context summary:", preview_page.text)
            self.assertIn("Safe config summary:", preview_page.text)
            self.assertNotIn("request_context_json", preview_page.text)
            self.assertNotIn("config_json", preview_page.text)
            self.assertNotIn("Traceback", preview_page.text)


if __name__ == "__main__":
    unittest.main()
