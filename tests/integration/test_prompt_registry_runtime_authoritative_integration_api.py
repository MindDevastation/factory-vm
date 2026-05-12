from __future__ import annotations

import importlib
import sqlite3
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env
from tests._runtime_authority import seed_runtime_authorities


class TestPromptRegistryRuntimeAuthoritativeIntegrationApi(unittest.TestCase):
    def _client(self):
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed(self, env, *, operator="admin", with_authority=True):
        conn = sqlite3.connect(env.db_path); conn.row_factory = sqlite3.Row
        try:
            conn.execute("INSERT INTO prompt_records(id,slug,code,title,record_type,status,validation_status,bridge_policy_hook,active_version_id,created_at,updated_at) VALUES(1,'p','p','p','prompt_template','active','VALID',NULL,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
            conn.execute("INSERT INTO prompt_versions(id,prompt_id,version_no,body_text,render_fingerprint,status,validation_status,is_active,created_at,updated_at) VALUES(1,1,1,'body','fp','active','VALID',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
            if with_authority:
                seed_runtime_authorities(conn, operator=operator, capability="CREATE_BULK_JSON_DRAFT", target_type="workflow", binding_fingerprint="bf", render_hash="rh")
            conn.commit()
        finally:
            conn.close()

    def _payload(self, **overrides):
        payload = dict(capability_code="CREATE_BULK_JSON_DRAFT", target_type="workflow", target_id="wf-1", operator_id_or_system_actor="spoofed", operator_subject="spoofed", prompt_record_id=1, prompt_version_id=1, binding_resolution_fingerprint="bf", rendered_payload_hash="rh", action_payload_hash="ah", reviewed_target_state_hash="caller")
        payload.update(overrides)
        return payload

    def test_preflight_uses_basic_auth_operator_and_ignores_body_spoof(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env); self._seed(env, operator="admin")
            client = self._client(); headers = basic_auth_header("admin", "testpass")
            response = client.post("/v1/prompt-registry/runtime/preflight", json=self._payload(operator_id_or_system_actor="operator-1", operator_subject="operator-1"), headers=headers)
            self.assertEqual(200, response.status_code, response.text)
            body = response.json(); self.assertEqual("CONFIRMATION_REQUIRED", body["state"])
            conn = sqlite3.connect(env.db_path)
            try:
                actor = conn.execute("SELECT operator_id_or_system_actor FROM prompt_execution_attempts WHERE id=?", (body["execution_attempt_id"],)).fetchone()[0]
                self.assertEqual("admin", actor)
            finally:
                conn.close()

    def test_missing_authority_returns_blocked_without_confirmation_token_and_health_get_safe(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env); self._seed(env, with_authority=False)
            client = self._client(); headers = basic_auth_header("admin", "testpass")
            self.assertEqual(200, client.get("/health").status_code)
            self.assertEqual(401, client.post("/v1/prompt-registry/runtime/preflight", json=self._payload()).status_code)
            before = sqlite3.connect(env.db_path).execute("SELECT COUNT(*) FROM prompt_execution_attempts").fetchone()[0]
            response = client.post("/v1/prompt-registry/runtime/preflight", json=self._payload(), headers=headers)
            self.assertEqual(200, response.status_code, response.text)
            body = response.json(); self.assertEqual("PREFLIGHT_REJECTED", body["state"])
            self.assertNotIn("confirmation_token", body)
            after = sqlite3.connect(env.db_path).execute("SELECT COUNT(*) FROM prompt_execution_attempts").fetchone()[0]
            self.assertEqual(before, after)

    def test_confirm_rejects_when_authority_changes_after_preflight(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env); self._seed(env, operator="admin")
            client = self._client(); headers = basic_auth_header("admin", "testpass")
            pre = client.post("/v1/prompt-registry/runtime/preflight", json=self._payload(), headers=headers).json()
            conn = sqlite3.connect(env.db_path)
            try:
                conn.execute("UPDATE prompt_runtime_operator_permissions SET is_enabled=0 WHERE operator_subject='admin'"); conn.commit()
            finally:
                conn.close()
            response = client.post("/v1/prompt-registry/runtime/confirm", json={"execution_attempt_id": pre["execution_attempt_id"], "confirmation_token": pre["confirmation_token"], "reviewed_target_state_hash": pre["reviewed_target_state_hash"]}, headers=headers)
            self.assertEqual(200, response.status_code, response.text)
            self.assertEqual("CONFLICT_BLOCKED", response.json()["state"])


if __name__ == "__main__":
    unittest.main()
