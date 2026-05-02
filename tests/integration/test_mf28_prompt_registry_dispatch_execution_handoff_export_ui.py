from __future__ import annotations

import importlib
import sqlite3
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf28PromptRegistryDispatchExecutionHandoffExportUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_attempt(self, client: TestClient, headers: dict[str, str]) -> int:
        rec = client.post("/v1/prompt-registry/records", headers=headers, json={"slug": "mf28", "code": "MF28", "title": "mf28", "record_type": "prompt_template", "status": "draft"})
        prompt_id = int(rec.json()["id"])
        action = client.post(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers, json={"action_key": "mf28-a", "action_type": "ui_action", "action_status": "active", "target_kind": "route", "target_ref": "/ui/prompt-registry", "config_json": {}})
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
        return int(review.text[start:end])

    def test_download_handoff_snapshot_json(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            attempt_id = self._seed_attempt(client, headers)

            unauthorized = client.post(f"/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}/execution-operator-handoff/download")
            self.assertEqual(unauthorized.status_code, 401)

            detail = client.get(f"/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Download handoff snapshot JSON", detail.text)

            conn = sqlite3.connect(env.db_path)
            try:
                before_audit_events = int(conn.execute("SELECT COUNT(*) FROM prompt_audit_events").fetchone()[0])
                before_usage_events = int(conn.execute("SELECT COUNT(*) FROM prompt_usage_events").fetchone()[0])
            finally:
                conn.close()
            before_request = client.get(f"/v1/prompt-registry/linked-action-dispatch-attempts/{attempt_id}", headers=headers).json()

            download = client.post(f"/ui/prompt-registry/linked-action-dispatch-attempts/{attempt_id}/execution-operator-handoff/download", headers=headers)
            self.assertEqual(download.status_code, 200)
            self.assertEqual(download.headers["content-type"].split(";")[0], "application/json")
            self.assertIn(f'dispatch-handoff-attempt-{attempt_id}.json', download.headers["content-disposition"])

            payload = download.json()
            self.assertEqual(payload["attempt_id"], attempt_id)
            self.assertIn("handoff_status", payload)
            self.assertIs(payload["execution_enabled"], False)
            self.assertIs(payload["runtime_available"], False)
            self.assertIn("recommended_operator_action", payload)
            self.assertIsInstance(payload.get("summary"), dict)
            self.assertIsInstance(payload.get("checklist_items"), list)
            self.assertIsInstance(payload.get("audit_payload_preview"), dict)
            self.assertTrue(any(isinstance(n, dict) and n.get("code") == "HANDOFF_SNAPSHOT_ONLY" for n in payload.get("notes", [])))

            conn = sqlite3.connect(env.db_path)
            try:
                after_audit_events = int(conn.execute("SELECT COUNT(*) FROM prompt_audit_events").fetchone()[0])
                after_usage_events = int(conn.execute("SELECT COUNT(*) FROM prompt_usage_events").fetchone()[0])
            finally:
                conn.close()
            after_request = client.get(f"/v1/prompt-registry/linked-action-dispatch-attempts/{attempt_id}", headers=headers).json()
            self.assertEqual(before_audit_events, after_audit_events)
            self.assertEqual(before_usage_events, after_usage_events)
            self.assertEqual(before_request.get("attempt_status"), after_request.get("attempt_status"))
            self.assertEqual(before_request.get("dispatch_status"), after_request.get("dispatch_status"))

            missing = client.post("/ui/prompt-registry/linked-action-dispatch-attempts/999999/execution-operator-handoff/download", headers=headers)
            self.assertEqual(missing.status_code, 404)
            self.assertEqual(missing.json().get("error", {}).get("code"), "PROMPT_REGISTRY_NOT_FOUND")
            self.assertNotIn("Traceback", missing.text)


if __name__ == "__main__":
    unittest.main()
