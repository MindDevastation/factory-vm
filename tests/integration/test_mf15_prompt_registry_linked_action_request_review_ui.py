from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf15PromptRegistryLinkedActionRequestReviewUi(unittest.TestCase):
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

    def _create_linked_action(
        self,
        client: TestClient,
        headers: dict[str, str],
        *,
        prompt_id: int,
        action_key: str,
        action_status: str,
    ) -> int:
        created = client.post(
            f"/ui/prompt-registry/{prompt_id}/linked-actions/create",
            headers=headers,
            data={
                "action_key": action_key,
                "action_type": "ui_action",
                "action_status": action_status,
                "target_kind": "route",
                "target_ref": f"/ui/prompt-registry/{prompt_id}/audit",
                "config_json": '{"note":"safe"}',
            },
        )
        self.assertEqual(created.status_code, 200)
        items = client.get(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers).json()["items"]
        return int([item for item in items if item["action_key"] == action_key][0]["id"])

    def test_review_page_requires_auth_and_renders_safe_rows_with_filters(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            prompt_a = self._create_record(client, headers, slug="mf15-a", code="PR-MF15-A", title="MF15 A")
            prompt_b = self._create_record(client, headers, slug="mf15-b", code="PR-MF15-B", title="MF15 B")
            action_a = self._create_linked_action(client, headers, prompt_id=prompt_a, action_key="mf15-action-a", action_status="active")
            action_b = self._create_linked_action(client, headers, prompt_id=prompt_b, action_key="mf15-action-b", action_status="inactive")

            unauthorized = client.get("/ui/prompt-registry/linked-action-requests")
            self.assertEqual(unauthorized.status_code, 401)

            accepted = client.post(
                f"/v1/prompt-registry/linked-actions/{action_a}/execution-requests",
                headers=headers,
                json={"confirm_execution": True, "request_context_json": {"reason": "operator-ok", "note": "sensitive-ish"}},
            )
            self.assertEqual(accepted.status_code, 200)
            blocked = client.post(
                f"/v1/prompt-registry/linked-actions/{action_b}/execution-requests",
                headers=headers,
                json={"confirm_execution": True, "request_context_json": {"reason": "inactive-guard"}},
            )
            self.assertEqual(blocked.status_code, 200)

            page = client.get("/ui/prompt-registry/linked-action-requests", headers=headers)
            self.assertEqual(page.status_code, 200)
            self.assertIn("Linked action execution request review", page.text)
            self.assertIn("Request context summary", page.text)
            self.assertIn("Diagnostics summary", page.text)
            self.assertIn(f'href="/ui/prompt-registry/{prompt_a}"', page.text)
            self.assertIn(f'href="/ui/prompt-registry/{prompt_a}/linked-actions/{action_a}/preview"', page.text)
            self.assertNotIn("request_context_json", page.text)
            self.assertNotIn("diagnostics_json", page.text)
            self.assertNotIn("Traceback", page.text)

            filtered_prompt = client.get(f"/ui/prompt-registry/linked-action-requests?prompt_id={prompt_a}", headers=headers)
            self.assertEqual(filtered_prompt.status_code, 200)
            self.assertIn(f">{prompt_a}<", filtered_prompt.text)
            self.assertNotIn(f">{prompt_b}<", filtered_prompt.text)

            filtered_action = client.get(
                f"/ui/prompt-registry/linked-action-requests?action_id={action_a}&request_status=accepted&preview_status=OK&requested_by=admin",
                headers=headers,
            )
            self.assertEqual(filtered_action.status_code, 200)
            self.assertIn("accepted", filtered_action.text)
            self.assertNotIn(">blocked<", filtered_action.text)

            invalid = client.get("/ui/prompt-registry/linked-action-requests?request_status=bad", headers=headers)
            self.assertEqual(invalid.status_code, 200)
            self.assertIn("Linked action requests error", invalid.text)
            self.assertNotIn("Traceback", invalid.text)

    def test_prompt_registry_navigation_contains_request_review_links(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            prompt_id = self._create_record(client, headers, slug="mf15-links", code="PR-MF15-LINKS", title="MF15 Links")
            action_id = self._create_linked_action(client, headers, prompt_id=prompt_id, action_key="mf15-link-action", action_status="active")

            overview = client.get("/ui/prompt-registry", headers=headers)
            self.assertEqual(overview.status_code, 200)
            self.assertIn('href="/ui/prompt-registry/linked-action-requests"', overview.text)

            detail = client.get(f"/ui/prompt-registry/{prompt_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn(f'href="/ui/prompt-registry/linked-action-requests?prompt_id={prompt_id}"', detail.text)

            preview = client.get(f"/ui/prompt-registry/{prompt_id}/linked-actions/{action_id}/preview", headers=headers)
            self.assertEqual(preview.status_code, 200)
            self.assertIn(f'href="/ui/prompt-registry/linked-action-requests?action_id={action_id}"', preview.text)


if __name__ == "__main__":
    unittest.main()
