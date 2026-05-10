from __future__ import annotations

import importlib
import sqlite3
import unittest
from unittest import mock

from fastapi.testclient import TestClient

from services.prompt_registry.runtime_adapters import RuntimeAdapterRegistry
from services.prompt_registry.runtime_execution import (
    confirm_prompt_execution,
    dispatch_prompt_execution,
    prepare_prompt_execution_preflight,
)
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPromptRegistryRuntimeApiMf6(unittest.TestCase):
    def _app_client(self):
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _seed_prompt(self, env) -> None:
        conn = sqlite3.connect(env.db_path)
        try:
            conn.execute(
                "INSERT INTO prompt_records(id,slug,code,title,record_type,status,validation_status,bridge_policy_hook,active_version_id,created_at,updated_at) "
                "VALUES(1,'runtime-mf6','runtime-mf6','Runtime MF6','prompt_template','active','VALID',NULL,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO prompt_versions(id,prompt_id,version_no,body_text,render_fingerprint,status,validation_status,is_active,created_at,updated_at) "
                "VALUES(1,1,1,'body','fp','active','VALID',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
            )
            conn.commit()
        finally:
            conn.close()

    def _base_payload(self, **overrides):
        payload = {
            "capability_code": "CREATE_BULK_JSON_DRAFT",
            "target_type": "workflow",
            "target_id": "wf-mf6",
            "operator_id_or_system_actor": "body-spoof",
            "prompt_record_id": 1,
            "prompt_version_id": 1,
            "binding_resolution_fingerprint": "bind-mf6",
            "rendered_payload_hash": "render-mf6",
            "action_payload_hash": "action-mf6",
            "reviewed_target_state_hash": "state-mf6",
        }
        payload.update(overrides)
        return payload

    def _preflight_api(self, client, headers, **overrides):
        return client.post("/v1/prompt-registry/runtime/preflight", json=self._base_payload(**overrides), headers=headers)

    def _preflight_service(self, env, **overrides):
        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        try:
            return prepare_prompt_execution_preflight(conn, **self._base_payload(operator_id_or_system_actor="admin", **overrides))
        finally:
            conn.close()

    def _admit_service(self, env, **overrides):
        pre = self._preflight_service(env, **overrides)
        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        try:
            return confirm_prompt_execution(
                conn,
                execution_attempt_id=pre["execution_attempt_id"],
                confirmation_token=pre["confirmation_token"],
                operator_id_or_system_actor="admin",
                reviewed_target_state_hash=overrides.get("reviewed_target_state_hash", "state-mf6"),
            )
        finally:
            conn.close()

    def test_health_is_unauthenticated_but_runtime_requires_basic_auth(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            _mod, client = self._app_client()
            self.assertEqual(200, client.get("/health").status_code)
            self.assertEqual(401, client.post("/v1/prompt-registry/runtime/preflight", json=self._base_payload()).status_code)

    def test_preflight_auth_returns_confirmation_token_and_uses_basic_auth_actor(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            res = self._preflight_api(client, headers)
            self.assertEqual(200, res.status_code, res.text)
            body = res.json()
            self.assertEqual("CONFIRMATION_REQUIRED", body["state"])
            self.assertTrue(body["confirmation_token"])
            conn = sqlite3.connect(env.db_path)
            try:
                actor = conn.execute("SELECT operator_id_or_system_actor FROM prompt_execution_attempts WHERE id=?", (body["execution_attempt_id"],)).fetchone()[0]
                self.assertEqual(env.basic_user, actor)
            finally:
                conn.close()

    def test_confirm_requires_token_and_admits_execution(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            pre = self._preflight_api(client, headers).json()
            missing = client.post("/v1/prompt-registry/runtime/confirm", json={"execution_attempt_id": pre["execution_attempt_id"], "reviewed_target_state_hash": "state-mf6"}, headers=headers)
            self.assertEqual(422, missing.status_code)
            admitted = client.post(
                "/v1/prompt-registry/runtime/confirm",
                json={"execution_attempt_id": pre["execution_attempt_id"], "confirmation_token": pre["confirmation_token"], "reviewed_target_state_hash": "state-mf6"},
                headers=headers,
            )
            self.assertEqual(200, admitted.status_code, admitted.text)
            self.assertEqual("ADMITTED", admitted.json()["state"])

    def test_status_timeline_ordered_and_get_endpoints_do_not_mutate(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            conn = sqlite3.connect(env.db_path)
            try:
                before = conn.execute("SELECT COUNT(*) FROM prompt_execution_lifecycle_events").fetchone()[0]
            finally:
                conn.close()
            status = client.get(f"/v1/prompt-registry/runtime/status/{admitted['execution_group_id']}", headers=headers)
            timeline = client.get(f"/v1/prompt-registry/runtime/timeline/{admitted['execution_group_id']}", headers=headers)
            self.assertEqual(200, status.status_code, status.text)
            self.assertEqual("ADMITTED", status.json()["current_state"])
            states = [item["state_after"] for item in timeline.json()["items"]]
            self.assertEqual(["CONFIRMATION_REQUIRED", "ADMITTED"], states)
            conn = sqlite3.connect(env.db_path)
            try:
                after = conn.execute("SELECT COUNT(*) FROM prompt_execution_lifecycle_events").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(before, after)

    def test_dispatch_sync_success_uses_server_side_adapter_and_rejects_non_admitted(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            pre = self._preflight_api(client, headers).json()
            non_admitted = client.post("/v1/prompt-registry/runtime/dispatch", json={"execution_attempt_id": pre["execution_attempt_id"], "adapter": "evil"}, headers=headers)
            self.assertEqual(422, non_admitted.status_code)
            client.post(
                "/v1/prompt-registry/runtime/confirm",
                json={"execution_attempt_id": pre["execution_attempt_id"], "confirmation_token": pre["confirmation_token"], "reviewed_target_state_hash": "state-mf6"},
                headers=headers,
            )
            called = []
            def fake_adapter(payload):
                called.append(payload)
                return {"result_code": "FAKE_OK", "secret_safe_message": "fake completed"}
            _mod.create_prompt_registry_runtime_router.__globals__["RUNTIME_ADAPTER_REGISTRY"].register("CREATE_BULK_JSON_DRAFT", fake_adapter)
            dispatched = client.post("/v1/prompt-registry/runtime/dispatch", json={"execution_attempt_id": pre["execution_attempt_id"], "adapter": "ignored", "payload": {"safe": "ok"}}, headers=headers)
            self.assertEqual(200, dispatched.status_code, dispatched.text)
            self.assertEqual("SUCCEEDED", dispatched.json()["state"])
            self.assertEqual("FAKE_OK", dispatched.json()["result_code"])
            self.assertEqual([{"safe": "ok"}], called)

    def test_retry_rejects_sync_and_accepts_async_failed_execution(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            sync_admitted = self._admit_service(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            sync_retry = client.post("/v1/prompt-registry/runtime/retry", json={"execution_attempt_id": sync_admitted["execution_attempt_id"]}, headers=headers)
            self.assertEqual(422, sync_retry.status_code)

            async_admitted = self._admit_service(env, capability_code="ENQUEUE_INTERNAL_PROMPT_JOB", target_id="async-mf6", action_payload_hash="action-async")
            conn = sqlite3.connect(env.db_path)
            try:
                conn.execute("UPDATE prompt_execution_attempts SET state='FAILED_TERMINAL',retryable_by_operator=1 WHERE id=?", (async_admitted["execution_attempt_id"],))
                conn.execute("UPDATE prompt_execution_groups SET current_state='FAILED_TERMINAL' WHERE id=?", (async_admitted["execution_group_id"],))
                conn.commit()
            finally:
                conn.close()
            retry = client.post("/v1/prompt-registry/runtime/retry", json={"execution_attempt_id": async_admitted["execution_attempt_id"], "retry_after": "2999-01-01T00:00:00Z"}, headers=headers)
            self.assertEqual(200, retry.status_code, retry.text)
            self.assertEqual("RETRY_PENDING", retry.json()["state"])

    def test_cancel_admitted_execution_works(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            cancelled = client.post("/v1/prompt-registry/runtime/cancel", json={"execution_attempt_id": admitted["execution_attempt_id"]}, headers=headers)
            self.assertEqual(200, cancelled.status_code, cancelled.text)
            self.assertEqual("CANCELLED", cancelled.json()["state"])

    def test_recover_endpoint_is_post_only_and_calls_recovery_helper(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            self.assertEqual(405, client.get("/v1/prompt-registry/runtime/recover", headers=headers).status_code)
            with mock.patch("services.factory_api.prompt_registry_runtime.recover_stale_runtime_executions", return_value=[{"state": "DISPATCHED"}]) as patched:
                res = client.post("/v1/prompt-registry/runtime/recover", json={"now": "2027-01-01T00:00:00Z"}, headers=headers)
            self.assertEqual(200, res.status_code, res.text)
            self.assertEqual([{"state": "DISPATCHED"}], res.json()["items"])
            self.assertTrue(patched.called)
            self.assertIs(mod, importlib.import_module("services.factory_api.app"))

    def test_response_payloads_are_secret_safe(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            dispatched = client.post("/v1/prompt-registry/runtime/dispatch", json={"execution_attempt_id": admitted["execution_attempt_id"], "payload": {"api_token": "super-secret-token"}}, headers=headers)
            self.assertEqual(200, dispatched.status_code, dispatched.text)
            self.assertNotIn("super-secret-token", dispatched.text)
            self.assertNotIn("api_token", dispatched.text)

    def test_ui_runtime_page_renders_status_timeline_controls_and_blocks_raw_secrets(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("UPDATE prompt_execution_attempts SET retryable_by_operator=1,state='FAILED_TERMINAL',result_code='FAILED',secret_safe_message='safe only' WHERE id=?", (admitted["execution_attempt_id"],))
                conn.execute("UPDATE prompt_execution_groups SET current_state='STALE_BLOCKED' WHERE id=?", (admitted["execution_group_id"],))
                conn.execute(
                    "INSERT INTO prompt_execution_lifecycle_events(execution_group_id,execution_attempt_id,state_before,state_after,result_code,actor,timestamp,event_payload_json) VALUES(?,?,?,?,?,?,?,?)",
                    (admitted["execution_group_id"], admitted["execution_attempt_id"], "ADMITTED", "STALE_BLOCKED", "STALE", "admin", "2027-01-01T00:00:00Z", '{"token":"raw-secret-token"}'),
                )
                conn.commit()
            finally:
                conn.close()
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            res = client.get(f"/ui/prompt-registry/runtime/{admitted['execution_group_id']}", headers=headers)
            self.assertEqual(200, res.status_code, res.text)
            self.assertIn("Prompt Registry Runtime", res.text)
            self.assertIn("STALE_BLOCKED", res.text)
            self.assertIn("Recheck required", res.text)
            self.assertIn("data-runtime-retry-hidden", res.text)
            self.assertIn("data-runtime-cancel-hidden", res.text)
            self.assertIn("Timeline events", res.text)
            self.assertIn("Usage summary", res.text)
            self.assertNotIn("raw-secret-token", res.text)

    def test_ui_retry_control_is_visible_for_async_retryable_failed_attempt(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env, capability_code="ENQUEUE_INTERNAL_PROMPT_JOB", target_id="async-ui-mf6", action_payload_hash="action-async-ui")
            conn = sqlite3.connect(env.db_path)
            try:
                conn.execute("UPDATE prompt_execution_attempts SET retryable_by_operator=1,state='FAILED_TERMINAL',result_code='FAILED',secret_safe_message='safe only' WHERE id=?", (admitted["execution_attempt_id"],))
                conn.execute("UPDATE prompt_execution_groups SET current_state='FAILED_TERMINAL' WHERE id=?", (admitted["execution_group_id"],))
                conn.commit()
            finally:
                conn.close()
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            res = client.get(f"/ui/prompt-registry/runtime/{admitted['execution_group_id']}", headers=headers)
            self.assertEqual(200, res.status_code, res.text)
            self.assertIn("data-runtime-retry-control", res.text)
            self.assertIn("data-runtime-cancel-hidden", res.text)


if __name__ == "__main__":
    unittest.main()
