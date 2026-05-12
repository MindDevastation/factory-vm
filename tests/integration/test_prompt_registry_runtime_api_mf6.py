from __future__ import annotations

import importlib
import sqlite3
import unittest
from unittest import mock

from fastapi.testclient import TestClient

from services.prompt_registry.runtime_adapters import RuntimeAdapterRegistry
from services.prompt_registry.runtime_execution import (
    claim_prompt_execution_async_work,
    complete_prompt_execution_async_work,
    confirm_prompt_execution,
    dispatch_prompt_execution,
    get_runtime_observability_snapshot,
    prepare_prompt_execution_preflight,
    reset_runtime_observability_for_tests,
)
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env
from tests._runtime_authority import seed_runtime_authorities


class TestPromptRegistryRuntimeApiMf6(unittest.TestCase):
    def _app_client(self):
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _seed_prompt(self, env) -> None:
        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                "INSERT INTO prompt_records(id,slug,code,title,record_type,status,validation_status,bridge_policy_hook,active_version_id,created_at,updated_at) "
                "VALUES(1,'runtime-mf6','runtime-mf6','Runtime MF6','prompt_template','active','VALID',NULL,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO prompt_versions(id,prompt_id,version_no,body_text,render_fingerprint,status,validation_status,is_active,created_at,updated_at) "
                "VALUES(1,1,1,'body','fp','active','VALID',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO prompt_bindings(prompt_id,binding_scope,channel_slug,binding_status,created_at,updated_at) "
                "VALUES(1,'channel','darkwood-reverie','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO prompt_bindings(prompt_id,binding_scope,channel_slug,binding_status,created_at,updated_at) "
                "VALUES(1,'channel','channel-b','active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
            )
            for cap in ("CREATE_BULK_JSON_DRAFT", "CREATE_METADATA_REQUEST", "CREATE_VISUAL_REQUEST", "CREATE_ANALYTICS_REQUEST", "ENQUEUE_INTERNAL_PROMPT_JOB", "GENERATE_OPERATOR_HANDOFF_EXPORT"):
                seed_runtime_authorities(conn, operator="admin", capability=cap, target_type="channel", binding_fingerprint="bind-mf6", render_hash="render-mf6", prompt_version_id=1)
            conn.commit()
        finally:
            conn.close()

    def _base_payload(self, **overrides):
        payload = {
            "capability_code": "CREATE_BULK_JSON_DRAFT",
            "target_type": "channel",
            "target_id": "darkwood-reverie",
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

    def _target_hash(self, env, *, target_type="channel", target_id="darkwood-reverie"):
        from services.prompt_registry.authoritative_gate import PromptRuntimeGateEvaluationService

        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        try:
            result = PromptRuntimeGateEvaluationService(conn).evaluate(
                operator_subject="admin",
                capability_code="CREATE_BULK_JSON_DRAFT",
                prompt_version_id=1,
                binding_fingerprint="bind-mf6",
                render_result_hash="render-mf6",
                target_type=target_type,
                target_ref=target_id,
            )
            return result.target_snapshot_hash
        finally:
            conn.close()

    def _preflight_api(self, env, client, headers, **overrides):
        target_type = overrides.get("target_type", "channel")
        target_id = overrides.get("target_id", "darkwood-reverie")
        overrides.setdefault("reviewed_target_state_hash", self._target_hash(env, target_type=target_type, target_id=target_id) or "missing-target")
        return client.post("/v1/prompt-registry/runtime/preflight", json=self._base_payload(**overrides), headers=headers)

    def _bulk_payload(self):
        return {
            "channel_slug": "darkwood-reverie",
            "title": "Runtime API Draft",
            "description": "Runtime API draft",
            "tags_csv": "runtime,test",
            "background_name": "background",
            "background_ext": ".png",
            "audio_ids_text": "audio-1",
        }

    def _preflight_service(self, env, **overrides):
        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        try:
            overrides.setdefault("reviewed_target_state_hash", self._target_hash(env, target_type=overrides.get("target_type", "channel"), target_id=overrides.get("target_id", "darkwood-reverie")) or "state-mf6")
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
                reviewed_target_state_hash=overrides.get("reviewed_target_state_hash") or self._target_hash(env, target_type=overrides.get("target_type", "channel"), target_id=overrides.get("target_id", "darkwood-reverie")) or "state-mf6",
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
            res = self._preflight_api(env, client, headers)
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

    def test_api_preflight_does_not_trust_request_gate_flags(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            res = self._preflight_api(
                env,
                client,
                headers,
                action_payload_hash="action-request-flags",
                capability_execution_enabled=False,
                target_exists=False,
                target_state_compatible=False,
                current_target_state_hash="caller-stale",
                rendered_payload_valid=False,
                _server_gate_context={
                    "binding_resolution_complete": False,
                    "target_exists": False,
                    "current_target_state_hash": "caller-stale",
                },
            )
            self.assertEqual(200, res.status_code, res.text)
            self.assertEqual("CONFIRMATION_REQUIRED", res.json()["state"])


    def test_api_preflight_fails_closed_without_server_binding_or_target_truth(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            conn = sqlite3.connect(env.db_path)
            try:
                conn.execute("DELETE FROM prompt_runtime_render_validation_ledger")
                conn.commit()
            finally:
                conn.close()
            missing_render_authority = self._preflight_api(env, client, headers, target_id="channel-c", action_payload_hash="action-missing-render")
            self.assertEqual(200, missing_render_authority.status_code, missing_render_authority.text)
            self.assertEqual("PREFLIGHT_REJECTED", missing_render_authority.json()["state"])
            self.assertEqual("missing_render_validation_authority", missing_render_authority.json()["failure_reason_code"])
            self.assertNotIn("confirmation_token", missing_render_authority.json())

            caller_context_cannot_override = self._preflight_api(
                env,
                client,
                headers,
                target_id="missing-channel",
                action_payload_hash="action-missing-target",
                _server_gate_context={
                    "binding_resolution_complete": True,
                    "target_exists": True,
                    "target_state_compatible": True,
                    "current_target_state_hash": "missing-target",
                },
                reviewed_target_state_hash="missing-target",
            )
            self.assertEqual(200, caller_context_cannot_override.status_code, caller_context_cannot_override.text)
            self.assertEqual("PREFLIGHT_REJECTED", caller_context_cannot_override.json()["state"])
            self.assertNotIn("confirmation_token", caller_context_cannot_override.json())

    def test_confirm_requires_token_and_admits_execution(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            pre = self._preflight_api(env, client, headers).json()
            missing = client.post("/v1/prompt-registry/runtime/confirm", json={"execution_attempt_id": pre["execution_attempt_id"], "reviewed_target_state_hash": self._target_hash(env)}, headers=headers)
            self.assertEqual(422, missing.status_code)
            admitted = client.post(
                "/v1/prompt-registry/runtime/confirm",
                json={"execution_attempt_id": pre["execution_attempt_id"], "confirmation_token": pre["confirmation_token"], "reviewed_target_state_hash": self._target_hash(env)},
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

    def test_dispatch_sync_uses_production_adapter_and_fails_closed_when_unavailable(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            pre = self._preflight_api(env, client, headers).json()
            non_admitted = client.post("/v1/prompt-registry/runtime/dispatch", json={"execution_attempt_id": pre["execution_attempt_id"], "adapter": "evil"}, headers=headers)
            self.assertEqual(422, non_admitted.status_code)
            client.post(
                "/v1/prompt-registry/runtime/confirm",
                json={"execution_attempt_id": pre["execution_attempt_id"], "confirmation_token": pre["confirmation_token"], "reviewed_target_state_hash": self._target_hash(env)},
                headers=headers,
            )

            dispatched = client.post("/v1/prompt-registry/runtime/dispatch", json={"execution_attempt_id": pre["execution_attempt_id"], "adapter": "ignored", "payload": self._bulk_payload()}, headers=headers)
            self.assertEqual(200, dispatched.status_code, dispatched.text)
            self.assertEqual("SUCCEEDED", dispatched.json()["state"])
            self.assertEqual("BULK_JSON_DRAFT_TARGET_UPDATED", dispatched.json()["result_code"])
            conn = sqlite3.connect(env.db_path)
            try:
                usage = conn.execute("SELECT artifact_ref,usage_payload_json FROM prompt_execution_usage WHERE latest_attempt_id=?", (pre["execution_attempt_id"],)).fetchone()
                self.assertIn("ui_job_drafts:", usage[0])
                self.assertIn("internal_product_target", usage[1])
            finally:
                conn.close()

        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            pre = self._preflight_api(env, client, headers, target_id="channel-b", action_payload_hash="action-fail-closed").json()
            client.post(
                "/v1/prompt-registry/runtime/confirm",
                json={"execution_attempt_id": pre["execution_attempt_id"], "confirmation_token": pre["confirmation_token"], "reviewed_target_state_hash": self._target_hash(env, target_id="channel-b")},
                headers=headers,
            )
            runtime_api = importlib.import_module("services.factory_api.prompt_registry_runtime")
            with mock.patch.object(runtime_api, "RUNTIME_ADAPTER_REGISTRY", RuntimeAdapterRegistry()):
                missing = client.post("/v1/prompt-registry/runtime/dispatch", json={"execution_attempt_id": pre["execution_attempt_id"], "adapter": "evil", "payload": self._bulk_payload()}, headers=headers)
            self.assertEqual(422, missing.status_code)
            self.assertEqual("PROMPT_RUNTIME_DISPATCH_REJECTED", missing.json()["error"]["code"])
            conn = sqlite3.connect(env.db_path)
            try:
                self.assertEqual("ADMITTED", conn.execute("SELECT state FROM prompt_execution_attempts WHERE id=?", (pre["execution_attempt_id"],)).fetchone()[0])
            finally:
                conn.close()
            self.assertIs(mod, importlib.import_module("services.factory_api.app"))

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
            runtime_api = importlib.import_module("services.factory_api.prompt_registry_runtime")
            with mock.patch.object(runtime_api, "RUNTIME_ADAPTER_REGISTRY", RuntimeAdapterRegistry()):
                dispatched = client.post("/v1/prompt-registry/runtime/dispatch", json={"execution_attempt_id": admitted["execution_attempt_id"], "payload": {"api_token": "super-secret-token"}}, headers=headers)
            self.assertEqual(422, dispatched.status_code, dispatched.text)
            self.assertNotIn("super-secret-token", dispatched.text)
            self.assertNotIn("api_token", dispatched.text)
            conn = sqlite3.connect(env.db_path)
            try:
                self.assertEqual("ADMITTED", conn.execute("SELECT state FROM prompt_execution_attempts WHERE id=?", (admitted["execution_attempt_id"],)).fetchone()[0])
            finally:
                conn.close()

    def test_required_read_only_runtime_detail_endpoints_are_auth_safe_and_non_mutating(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env, target_id="detail-mf6", action_payload_hash="action-detail")
            _mod, client = self._app_client()
            for path in (
                f"/v1/prompt-registry/runtime/attempts/{admitted['execution_group_id']}",
                f"/v1/prompt-registry/runtime/readiness/{admitted['execution_group_id']}",
                f"/v1/prompt-registry/runtime/audit-safe-detail/{admitted['execution_group_id']}",
            ):
                self.assertEqual(401, client.get(path).status_code)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            conn = sqlite3.connect(env.db_path)
            try:
                before = conn.execute("SELECT COUNT(*) FROM prompt_execution_lifecycle_events").fetchone()[0]
            finally:
                conn.close()
            attempts = client.get(f"/v1/prompt-registry/runtime/attempts/{admitted['execution_group_id']}", headers=headers)
            readiness = client.get(f"/v1/prompt-registry/runtime/readiness/{admitted['execution_group_id']}", headers=headers)
            audit = client.get(f"/v1/prompt-registry/runtime/audit-safe-detail/{admitted['execution_group_id']}", headers=headers)
            self.assertEqual(200, attempts.status_code, attempts.text)
            self.assertEqual(1, len(attempts.json()["items"]))
            self.assertEqual(200, readiness.status_code, readiness.text)
            self.assertTrue(readiness.json()["ready_for_dispatch"])
            self.assertEqual(200, audit.status_code, audit.text)
            self.assertIn("status", audit.json())
            self.assertNotIn("raw-secret", audit.text)
            conn = sqlite3.connect(env.db_path)
            try:
                after = conn.execute("SELECT COUNT(*) FROM prompt_execution_lifecycle_events").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(before, after)

    def test_async_worker_success_path_claims_and_completes_attempt(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env, capability_code="ENQUEUE_INTERNAL_PROMPT_JOB", target_id="async-success-mf6", action_payload_hash="action-async-success")
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                dispatch_prompt_execution(conn, execution_attempt_id=admitted["execution_attempt_id"], adapter_registry=RuntimeAdapterRegistry())
                claim = claim_prompt_execution_async_work(conn, lease_owner="worker-mf6", lease_seconds=60, now="2027-01-01T00:00:00Z")
                self.assertEqual("CLAIMED", claim["queue_state"])
                done = complete_prompt_execution_async_work(conn, execution_attempt_id=admitted["execution_attempt_id"], result_code="ASYNC_OK", secret_safe_message="async done", actor="worker-mf6")
                self.assertEqual("SUCCEEDED", done["state"])
                self.assertEqual("DONE", conn.execute("SELECT queue_state FROM prompt_execution_async_queue WHERE execution_attempt_id=?", (admitted["execution_attempt_id"],)).fetchone()[0])
            finally:
                conn.close()

    def test_runtime_observability_metrics_are_incremented_for_lifecycle(self):
        reset_runtime_observability_for_tests()
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env, target_id="obs-mf6", action_payload_hash="action-obs")
            _mod, client = self._app_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            client.post("/v1/prompt-registry/runtime/cancel", json={"execution_attempt_id": admitted["execution_attempt_id"]}, headers=headers)
            metrics = get_runtime_observability_snapshot()["metrics"]
            self.assertGreater(metrics.get("runtime.runtime_preflight_started", 0), 0)
            self.assertGreater(metrics.get("runtime.runtime_admitted", 0), 0)
            self.assertGreater(metrics.get("runtime.cancelled", 0), 0)

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

    def test_ui_cancel_form_post_requires_auth_accepts_form_and_cancels(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env, target_id="cancel-ui-mf6", action_payload_hash="action-cancel-ui")
            _mod, client = self._app_client()
            self.assertEqual(401, client.post("/ui/prompt-registry/runtime/cancel", data={"execution_attempt_id": admitted["execution_attempt_id"]}).status_code)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            res = client.post("/ui/prompt-registry/runtime/cancel", data={"execution_attempt_id": admitted["execution_attempt_id"]}, headers=headers, follow_redirects=False)
            self.assertEqual(303, res.status_code, res.text)
            self.assertEqual(f"/ui/prompt-registry/runtime/{admitted['execution_group_id']}", res.headers["location"])
            conn = sqlite3.connect(env.db_path)
            try:
                self.assertEqual("CANCELLED", conn.execute("SELECT state FROM prompt_execution_attempts WHERE id=?", (admitted["execution_attempt_id"],)).fetchone()[0])
            finally:
                conn.close()

    def test_ui_retry_form_post_requires_auth_accepts_form_and_redirects(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_prompt(env)
            admitted = self._admit_service(env, capability_code="ENQUEUE_INTERNAL_PROMPT_JOB", target_id="retry-post-ui-mf6", action_payload_hash="action-retry-post-ui")
            conn = sqlite3.connect(env.db_path)
            try:
                conn.execute("UPDATE prompt_execution_attempts SET retryable_by_operator=1,state='FAILED_TERMINAL',result_code='FAILED',secret_safe_message='safe only' WHERE id=?", (admitted["execution_attempt_id"],))
                conn.execute("UPDATE prompt_execution_groups SET current_state='FAILED_TERMINAL' WHERE id=?", (admitted["execution_group_id"],))
                conn.commit()
            finally:
                conn.close()
            _mod, client = self._app_client()
            self.assertEqual(401, client.post("/ui/prompt-registry/runtime/retry", data={"execution_attempt_id": admitted["execution_attempt_id"]}).status_code)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            res = client.post("/ui/prompt-registry/runtime/retry", data={"execution_attempt_id": admitted["execution_attempt_id"]}, headers=headers, follow_redirects=False)
            self.assertEqual(303, res.status_code, res.text)
            self.assertEqual(f"/ui/prompt-registry/runtime/{admitted['execution_group_id']}", res.headers["location"])
            conn = sqlite3.connect(env.db_path)
            try:
                self.assertEqual("ADMITTED", conn.execute("SELECT state FROM prompt_execution_attempts WHERE execution_group_id=? ORDER BY id DESC LIMIT 1", (admitted["execution_group_id"],)).fetchone()[0])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
