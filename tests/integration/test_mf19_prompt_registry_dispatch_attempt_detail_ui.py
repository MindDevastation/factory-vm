from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf19PromptRegistryDispatchAttemptDetailUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_attempt(self, client: TestClient, headers: dict[str, str], *, slug: str) -> tuple[int, int, int, int]:
        rec = client.post("/v1/prompt-registry/records", headers=headers, json={"slug": slug, "code": slug.upper(), "title": slug, "record_type": "prompt_template", "status": "draft"})
        self.assertEqual(rec.status_code, 200)
        prompt_id = int(rec.json()["id"])
        action = client.post(
            f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
            headers=headers,
            json={
                "action_key": f"{slug}-action",
                "action_type": "ui_action",
                "action_status": "active",
                "target_kind": "route",
                "target_ref": "/ui/prompt-registry/linked-action-requests",
                "config_json": {"ui_label": "go", "channel": "ops"},
            },
        )
        self.assertEqual(action.status_code, 200)
        action_id = int(action.json()["id"])
        req = client.post(
            f"/v1/prompt-registry/linked-actions/{action_id}/execution-requests",
            headers=headers,
            json={"confirm_execution": True, "request_context_json": {"x": "y", "scope": "mf19"}},
        )
        self.assertEqual(req.status_code, 200)
        request_id = int(req.json()["id"])
        created = client.post(
            f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-attempts",
            headers=headers,
            data={"note": "visible-safe-note", "operator_secret": "must-not-leak"},
        )
        self.assertEqual(created.status_code, 200)

        list_page = client.get(
            f"/ui/prompt-registry/linked-action-dispatch-attempts?execution_request_id={request_id}", headers=headers
        )
        self.assertEqual(list_page.status_code, 200)
        marker = '/ui/prompt-registry/linked-action-dispatch-attempts/'
        idx = list_page.text.find(marker)
        self.assertNotEqual(idx, -1)
        start = idx + len(marker)
        end = start
        while end < len(list_page.text) and list_page.text[end].isdigit():
            end += 1
        attempt_id = int(list_page.text[start:end])
        return prompt_id, action_id, request_id, attempt_id

    def test_detail_page_auth_rendering_links_and_safety(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id, action_id, request_id, attempt_id = self._seed_attempt(client, headers, slug="mf19-a")

            unauthorized = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}")
            self.assertEqual(unauthorized.status_code, 401)

            detail = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn(f"Attempt #{attempt_id}", detail.text)
            self.assertIn(f">{prompt_id}</td>", detail.text)
            self.assertIn(f">{action_id}</td>", detail.text)
            self.assertIn(f">{request_id}</td>", detail.text)
            self.assertIn("safe_context_summary", detail.text)
            self.assertIn("safe_config_summary", detail.text)
            self.assertIn("Back to dispatch attempts review", detail.text)
            self.assertIn(f'/ui/prompt-registry/{prompt_id}', detail.text)
            self.assertIn(f'/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-preview', detail.text)
            self.assertIn(f'/ui/prompt-registry/{prompt_id}/linked-actions/{action_id}/preview', detail.text)

            self.assertNotIn("plan_json", detail.text)
            self.assertNotIn("diagnostics_json", detail.text)
            self.assertNotIn("operator_secret", detail.text)
            self.assertNotIn("must-not-leak", detail.text)

            missing = client.get("/ui/prompt-registry/linked-action-dispatch-attempts/999999", headers=headers)
            self.assertEqual(missing.status_code, 200)
            self.assertIn("Dispatch attempt detail error:", missing.text)
            self.assertIn("not found", missing.text)
            self.assertNotIn("Traceback", missing.text)

            review = client.get("/ui/prompt-registry/linked-action-dispatch-attempts", headers=headers)
            self.assertIn(f'/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}', review.text)


if __name__ == "__main__":
    unittest.main()
