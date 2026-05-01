from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf21PromptRegistryDispatchReadinessGateUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed(self, client: TestClient, headers: dict[str, str], slug: str = "mf21") -> tuple[int, int, int, int]:
        rec = client.post("/v1/prompt-registry/records", headers=headers, json={"slug": slug, "code": slug.upper(), "title": slug, "record_type": "prompt_template", "status": "draft"})
        prompt_id = int(rec.json()["id"])
        action = client.post(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers, json={"action_key": f"{slug}-action", "action_type": "ui_action", "action_status": "active", "target_kind": "route", "target_ref": "/ui/prompt-registry/linked-action-requests", "config_json": {"ui_label": "go"}})
        action_id = int(action.json()["id"])
        req = client.post(f"/v1/prompt-registry/linked-actions/{action_id}/execution-requests", headers=headers, json={"confirm_execution": True, "request_context_json": {"scope": slug}})
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
        return prompt_id, action_id, request_id, attempt_id

    def test_readiness_gate_renders_eligible_and_blocked_safely(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            _prompt_id, action_id, _request_id, attempt_id = self._seed(client, headers)

            eligible = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}", headers=headers)
            self.assertEqual(eligible.status_code, 200)
            self.assertIn("Readiness gate only. No runtime execution is performed.", eligible.text)
            self.assertIn("Dispatch readiness gate", eligible.text)
            self.assertIn("ELIGIBLE", eligible.text)
            self.assertIn("READY_FOR_FUTURE_EXECUTION", eligible.text)
            self.assertNotIn("plan_json", eligible.text)
            self.assertNotIn("diagnostics_json", eligible.text)
            self.assertNotIn("Traceback", eligible.text)

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE prompt_linked_actions SET action_status = ? WHERE id = ?", ("inactive", action_id))
                conn.commit()
            finally:
                conn.close()

            blocked = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}", headers=headers)
            self.assertEqual(blocked.status_code, 200)
            self.assertIn("BLOCKED", blocked.text)
            self.assertIn("DISPATCH_NOT_READY", blocked.text)
            self.assertNotIn("Traceback", blocked.text)


if __name__ == "__main__":
    unittest.main()
