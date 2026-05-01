from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf24PromptRegistryDispatchExecutionAuditPreviewUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def test_dispatch_execution_audit_preview_section_is_read_only(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            rec = client.post("/v1/prompt-registry/records", headers=headers, json={"slug": "mf24", "code": "MF24", "title": "mf24", "record_type": "prompt_template", "status": "draft"})
            prompt_id = int(rec.json()["id"])
            action = client.post(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers, json={"action_key": "mf24-a", "action_type": "ui_action", "action_status": "active", "target_kind": "route", "target_ref": "/ui/prompt-registry", "config_json": {}})
            action_id = int(action.json()["id"])
            req = client.post(f"/v1/prompt-registry/linked-actions/{action_id}/execution-requests", headers=headers, json={"confirm_execution": True})
            request_id = int(req.json()["id"])
            client.post(f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-attempts", headers=headers, data={"note": "seed"})
            review = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts?execution_request_id={request_id}", headers=headers)
            marker = '/ui/prompt-registry/linked-action-dispatch-attempts/'
            idx = review.text.find(marker)
            start = idx + len(marker)
            end = start
            while end < len(review.text) and review.text[end].isdigit():
                end += 1
            attempt_id = int(review.text[start:end])

            detail = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Audit preview only. No audit event is written.", detail.text)
            self.assertIn("preview_status", detail.text)
            self.assertIn("PREVIEW_ONLY", detail.text)
            self.assertIn("would_write_event_type", detail.text)
            self.assertIn("linked_action_dispatch_execution_blocked", detail.text)
            self.assertIn("would_write", detail.text)
            self.assertIn("no", detail.text)
            self.assertIn("Audit payload preview", detail.text)
            self.assertIn("attempt_id", detail.text)
            self.assertIn("execution_request_id", detail.text)
            self.assertIn("prompt_id", detail.text)
            self.assertIn("action_id", detail.text)
            self.assertNotIn("plan_json", detail.text)
            self.assertNotIn("diagnostics_json", detail.text)
            self.assertNotIn("Traceback", detail.text)


if __name__ == "__main__":
    unittest.main()
