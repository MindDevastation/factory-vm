from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf17PromptRegistryDispatchAttemptLedgerUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _create_record(self, client: TestClient, headers: dict[str, str], *, slug: str) -> int:
        created = client.post(
            "/v1/prompt-registry/records",
            headers=headers,
            json={"slug": slug, "code": f"CODE-{slug}", "title": slug, "record_type": "prompt_template", "status": "draft"},
        )
        self.assertEqual(created.status_code, 200)
        return int(created.json()["id"])

    def _create_linked_action(self, client: TestClient, headers: dict[str, str], *, prompt_id: int, action_key: str, status: str) -> int:
        created = client.post(
            f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
            headers=headers,
            json={
                "action_key": action_key,
                "action_type": "ui_action",
                "action_status": status,
                "target_kind": "route",
                "target_ref": "/ui/prompt-registry/linked-action-requests",
                "config_json": {"ui_label": "go"},
            },
        )
        self.assertEqual(created.status_code, 200)
        return int(created.json()["id"])

    def test_dispatch_attempt_ledger_ui_flow_and_safety(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            prompt_id = self._create_record(client, headers, slug="mf17-ready")
            action_id = self._create_linked_action(client, headers, prompt_id=prompt_id, action_key="mf17-ready-action", status="active")
            req = client.post(
                f"/v1/prompt-registry/linked-actions/{action_id}/execution-requests",
                headers=headers,
                json={"confirm_execution": True, "request_context_json": {"reason": "approved"}},
            )
            self.assertEqual(req.status_code, 200)
            request_id = int(req.json()["id"])

            unauthorized = client.get(f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-preview")
            self.assertEqual(unauthorized.status_code, 401)

            page = client.get(f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-preview", headers=headers)
            self.assertEqual(page.status_code, 200)
            self.assertIn("Record dry-run dispatch attempt", page.text)
            self.assertIn("Dry-run ledger only. No runtime execution is performed.", page.text)

            post_ready = client.post(
                f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-attempts",
                headers=headers,
                data={"note": "operator safe note"},
            )
            self.assertEqual(post_ready.status_code, 200)
            self.assertIn("dry_run_recorded", post_ready.text)
            self.assertNotIn("plan_json", post_ready.text)
            self.assertNotIn("diagnostics_json", post_ready.text)
            self.assertNotIn("Traceback", post_ready.text)

            blocked_prompt_id = self._create_record(client, headers, slug="mf17-blocked")
            blocked_action_id = self._create_linked_action(client, headers, prompt_id=blocked_prompt_id, action_key="mf17-blocked-action", status="inactive")
            blocked_req = client.post(
                f"/v1/prompt-registry/linked-actions/{blocked_action_id}/execution-requests",
                headers=headers,
                json={"confirm_execution": True, "request_context_json": {"reason": "inactive"}},
            )
            self.assertEqual(blocked_req.status_code, 200)
            blocked_request_id = int(blocked_req.json()["id"])

            post_blocked = client.post(
                f"/ui/prompt-registry/linked-action-requests/{blocked_request_id}/dispatch-attempts",
                headers=headers,
                data={"note": "blocked safe note"},
            )
            self.assertEqual(post_blocked.status_code, 200)
            self.assertIn("blocked", post_blocked.text)

            bad_note = client.post(
                f"/ui/prompt-registry/linked-action-requests/{request_id}/dispatch-attempts",
                headers=headers,
                data={"note": "password=abc123"},
            )
            self.assertEqual(bad_note.status_code, 200)
            self.assertIn("Dispatch attempt error", bad_note.text)
            self.assertNotIn("password=abc123", bad_note.text)
            self.assertNotIn("password", bad_note.text)
            self.assertNotIn("Traceback", bad_note.text)

            conn = dbm.connect(env)
            try:
                usage_row = conn.execute(
                    "SELECT COUNT(1) AS cnt FROM prompt_usage_events WHERE event_type = ?",
                    ("linked_action_dispatch_attempt_recorded",),
                ).fetchone()
                usage_count = usage_row[0] if isinstance(usage_row, tuple) else usage_row["cnt"]
                self.assertEqual(int(usage_count), 0)
                status_row = conn.execute(
                    "SELECT request_status FROM prompt_linked_action_execution_requests WHERE id = ?",
                    (request_id,),
                ).fetchone()
                status_value = status_row[0] if isinstance(status_row, tuple) else status_row["request_status"]
                self.assertEqual(str(status_value), "accepted")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
