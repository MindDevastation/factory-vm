from __future__ import annotations

import html
import importlib
import json
import re
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf29PromptRegistryDispatchExecutionHandoffCopyPreviewUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def test_handoff_snapshot_preview_json_rendered_read_only(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            rec = client.post("/v1/prompt-registry/records", headers=headers, json={"slug": "mf29", "code": "MF29", "title": "mf29", "record_type": "prompt_template", "status": "draft"})
            prompt_id = int(rec.json()["id"])
            action = client.post(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers, json={"action_key": "mf29-a", "action_type": "ui_action", "action_status": "active", "target_kind": "route", "target_ref": "/ui/prompt-registry", "config_json": {}})
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
            self.assertIn("Handoff snapshot preview", detail.text)
            self.assertIn("Preview only. No runtime execution is performed.", detail.text)

            match = re.search(r'<pre data-testid="handoff-snapshot-preview">(.*?)</pre>', detail.text, flags=re.DOTALL)
            self.assertIsNotNone(match)
            payload = json.loads(html.unescape(match.group(1)))

            required_keys = {
                "attempt_id",
                "handoff_status",
                "recommended_operator_action",
                "execution_enabled",
                "runtime_available",
                "summary",
                "blocking_codes",
                "warning_codes",
                "checklist_items",
                "audit_payload_preview",
                "notes",
            }
            self.assertTrue(required_keys.issubset(payload.keys()))
            self.assertEqual(payload["attempt_id"], attempt_id)
            self.assertIs(payload["execution_enabled"], False)
            self.assertIs(payload["runtime_available"], False)
            self.assertTrue(any(isinstance(n, dict) and n.get("code") == "HANDOFF_SNAPSHOT_ONLY" for n in payload.get("notes", [])))
            self.assertNotIn("plan_json", detail.text)
            self.assertNotIn("diagnostics_json", detail.text)
            self.assertNotIn("Traceback", detail.text)


if __name__ == "__main__":
    unittest.main()
