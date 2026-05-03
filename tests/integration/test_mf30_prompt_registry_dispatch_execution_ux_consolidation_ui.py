from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf30PromptRegistryDispatchExecutionUxConsolidationUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def test_dispatch_execution_review_map_and_anchor_sections(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            rec = client.post("/v1/prompt-registry/records", headers=headers, json={"slug": "mf30", "code": "MF30", "title": "mf30", "record_type": "prompt_template", "status": "draft"})
            prompt_id = int(rec.json()["id"])
            action = client.post(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers, json={"action_key": "mf30-a", "action_type": "ui_action", "action_status": "active", "target_kind": "route", "target_ref": "/ui/prompt-registry", "config_json": {}})
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
            self.assertIn("Dispatch execution review map", detail.text)
            self.assertIn("Execution foundation review only. No runtime execution, audit write, queue job, or external call is performed from this page.", detail.text)

            anchors = [
                "#dispatch-recheck",
                "#dispatch-readiness",
                "#dispatch-execution-disabled",
                "#dispatch-capability",
                "#dispatch-audit-preview",
                "#dispatch-preflight",
                "#dispatch-operator-checklist",
                "#dispatch-operator-handoff",
                "#dispatch-handoff-preview",
            ]
            for anchor in anchors:
                self.assertIn(f'href="{anchor}"', detail.text)
                self.assertIn(f'id="{anchor[1:]}"', detail.text)

            self.assertIn("Handoff snapshot preview", detail.text)
            self.assertIn("Download handoff snapshot JSON", detail.text)
            self.assertNotIn("plan_json", detail.text)
            self.assertNotIn("diagnostics_json", detail.text)
            self.assertNotIn("Traceback", detail.text)
            self.assertNotIn(">Execute<", detail.text)
            self.assertNotIn(">Retry<", detail.text)
            self.assertNotIn(">Delete<", detail.text)


if __name__ == "__main__":
    unittest.main()
