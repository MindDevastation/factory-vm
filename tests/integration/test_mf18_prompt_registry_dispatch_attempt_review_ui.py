from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf18PromptRegistryDispatchAttemptReviewUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_attempt(self, client: TestClient, headers: dict[str, str], *, slug: str, active: bool = True) -> tuple[int, int, int]:
        rec = client.post("/v1/prompt-registry/records", headers=headers, json={"slug": slug, "code": slug.upper(), "title": slug, "record_type": "prompt_template", "status": "draft"})
        self.assertEqual(rec.status_code, 200)
        prompt_id = int(rec.json()["id"])
        action = client.post(
            f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
            headers=headers,
            json={
                "action_key": f"{slug}-action",
                "action_type": "ui_action",
                "action_status": "active" if active else "inactive",
                "target_kind": "route",
                "target_ref": "/ui/prompt-registry/linked-action-requests",
                "config_json": {"ui_label": "go"},
            },
        )
        self.assertEqual(action.status_code, 200)
        action_id = int(action.json()["id"])
        req = client.post(f"/v1/prompt-registry/linked-actions/{action_id}/execution-requests", headers=headers, json={"confirm_execution": True, "request_context_json": {"x": "y"}})
        self.assertEqual(req.status_code, 200)
        request_id = int(req.json()["id"])
        created = client.post(f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-attempts", headers=headers, data={"note": "safe"})
        self.assertEqual(created.status_code, 200)
        return prompt_id, action_id, request_id

    def test_review_page_filters_links_and_safety(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            p1, a1, r1 = self._seed_attempt(client, headers, slug="mf18-a", active=True)
            p2, a2, r2 = self._seed_attempt(client, headers, slug="mf18-b", active=False)

            unauthorized = client.get("/ui/prompt-registry/linked-action-dispatch-attempts")
            self.assertEqual(unauthorized.status_code, 401)

            page = client.get("/ui/prompt-registry/linked-action-dispatch-attempts", headers=headers)
            self.assertEqual(page.status_code, 200)
            self.assertIn("Linked action dispatch attempt review", page.text)
            self.assertIn(f"/ui/prompt-registry/{p1}", page.text)
            self.assertIn(f"/ui/prompt-registry/linked-action-requests/{r1}/dispatch-preview", page.text)
            self.assertNotIn("plan_json", page.text)
            self.assertNotIn("diagnostics_json", page.text)

            by_prompt = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts?prompt_id={p1}", headers=headers)
            self.assertIn(f">{p1}</td>", by_prompt.text)
            self.assertNotIn(f">{p2}</td>", by_prompt.text)

            by_action = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts?action_id={a1}", headers=headers)
            self.assertIn(f">{a1}</td>", by_action.text)
            self.assertNotIn(f">{a2}</td>", by_action.text)

            by_req = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts?execution_request_id={r1}", headers=headers)
            self.assertIn(f">{r1}</td>", by_req.text)
            self.assertNotIn(f">{r2}</td>", by_req.text)

            by_status = client.get("/ui/prompt-registry/linked-action-dispatch-attempts?attempt_status=blocked", headers=headers)
            self.assertIn("blocked", by_status.text)
            self.assertNotIn(">dry_run_recorded</td>", by_status.text)

            bad_int = client.get("/ui/prompt-registry/linked-action-dispatch-attempts?prompt_id=bad", headers=headers)
            self.assertIn("must be an integer", bad_int.text)
            self.assertNotIn("Traceback", bad_int.text)

            bad_status = client.get("/ui/prompt-registry/linked-action-dispatch-attempts?attempt_status=bad", headers=headers)
            self.assertIn("attempt_status must be one of", bad_status.text)
            self.assertNotIn("Traceback", bad_status.text)

            overview = client.get("/ui/prompt-registry", headers=headers)
            self.assertIn('href="/ui/prompt-registry/linked-action-dispatch-attempts"', overview.text)
            requests_page = client.get("/ui/prompt-registry/linked-action-requests", headers=headers)
            self.assertIn('href="/ui/prompt-registry/linked-action-dispatch-attempts"', requests_page.text)
            self.assertIn('linked-action-dispatch-attempts?execution_request_id=', requests_page.text)
            preview = client.get(f"/ui/prompt-registry/linked-action-requests/{r1}/dispatch-preview", headers=headers)
            self.assertIn(f'/ui/prompt-registry/linked-action-dispatch-attempts?execution_request_id={r1}', preview.text)
            detail = client.get(f"/ui/prompt-registry/{p1}", headers=headers)
            self.assertIn(f'/ui/prompt-registry/linked-action-dispatch-attempts?prompt_id={p1}', detail.text)


if __name__ == "__main__":
    unittest.main()
