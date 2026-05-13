from __future__ import annotations

import importlib
import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import TARGET_SNAPSHOT_RESOLVER_REGISTRY
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


def payload_for_ref(target_ref: str) -> dict:
    return {
        "target_type": "channel",
        "target_ref": target_ref,
        "target_display_label": f"Channel {target_ref}",
        "target_state_code": "ready",
        "target_exists": True,
        "target_updated_at": "2026-01-01T00:00:00Z",
        "compatibility_inputs": {"kind": "LONG"},
        "resolver_metadata": {"note": "safe"},
    }


class TestPromptRuntimeAuthoritativeGateApiMf5(unittest.TestCase):
    def setUp(self) -> None:
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()

    def tearDown(self) -> None:
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()

    def _client(self):
        from fastapi.testclient import TestClient

        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_all(self, client, headers, *, operator_permission="runtime_execute", render_status="passed", compatibility_status="allowed", resolver_code="fake_resolver") -> None:
        client.put("/v1/prompt-runtime/capabilities/CREATE_BULK_JSON_DRAFT", json={"execution_enabled": True, "required_permission_class": "runtime_execute", "status": "active"}, headers=headers)
        client.put(f"/v1/prompt-runtime/operators/admin/permissions", json={"permission_class": operator_permission, "is_enabled": True}, headers=headers)
        client.put("/v1/prompt-runtime/resolvers/CREATE_BULK_JSON_DRAFT/channel", json={"resolver_code": resolver_code, "snapshot_schema_version": "v1", "is_enabled": True}, headers=headers)
        client.put("/v1/prompt-runtime/compatibility/CREATE_BULK_JSON_DRAFT/channel", json={"compatibility_status": compatibility_status, "policy_code": f"p_{compatibility_status}"}, headers=headers)
        # Render validation write helper is intentionally internal; seed through service directly.

    def _record_render(self, env, *, status="passed", detail: str | None = None) -> None:
        from services.prompt_registry.authoritative_gate import RenderValidationService

        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        try:
            RenderValidationService(conn).record_validation(prompt_record_id=1, prompt_version_id=10, binding_fingerprint="bind", render_result_hash="render", validation_status=status, validation_schema_version="v1", validator_code="api", invalid_reason_detail=detail)
            conn.commit()
        finally:
            conn.close()

    def _payload(self, **overrides):
        payload = {"capability_code": "CREATE_BULK_JSON_DRAFT", "prompt_version_id": 10, "binding_fingerprint": "bind", "render_result_hash": "render", "target_type": "channel", "target_ref": "target-1", "operator_subject": "spoofed"}
        payload.update(overrides)
        return payload

    def test_gate_evaluate_requires_basic_auth_and_get_does_not_mutate(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            self.assertEqual(client.post("/v1/prompt-runtime/gates/evaluate", json=self._payload()).status_code, 401)
            headers = basic_auth_header("admin", "testpass")
            before = self._counts(env)
            self.assertIn(client.get("/v1/prompt-runtime/gates/evaluate", headers=headers).status_code, (404, 405))
            self.assertEqual(self._counts(env), before)

    def test_all_sources_present_is_admissible_and_uses_basic_auth_operator(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            TARGET_SNAPSHOT_RESOLVER_REGISTRY.register("fake_resolver", lambda **kwargs: payload_for_ref(kwargs["target_ref"]) | {"resolver_metadata": {"secret_token": "abc"}})
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            self._seed_all(client, headers)
            self._record_render(env)
            response = client.post("/v1/prompt-runtime/gates/evaluate", json=self._payload(operator_subject="not-admin"), headers=headers)
            self.assertEqual(response.status_code, 200, response.text)
            body = response.json()
            self.assertEqual(body["admission_status"], "admissible")
            self.assertIsNone(body["failure_reason_code"])
            self.assertEqual(body["resolved_permission_class"], "runtime_execute")
            self.assertIsNotNone(body["target_snapshot_hash"])
            self.assertIn("target_snapshot", body["authoritative_source_summary"])
            self.assertNotIn("abc", response.text)

    def test_blocked_paths_and_no_execution_side_effects(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            before = self._counts(env)
            missing = client.post("/v1/prompt-runtime/gates/evaluate", json=self._payload(), headers=headers)
            self.assertEqual(missing.json()["admission_status"], "blocked_missing_authority")
            self._seed_all(client, headers, operator_permission="runtime_view")
            self._record_render(env)
            insufficient = client.post("/v1/prompt-runtime/gates/evaluate", json=self._payload(), headers=headers)
            self.assertEqual(insufficient.json()["admission_status"], "blocked_permission")
            client.put("/v1/prompt-runtime/operators/admin/permissions", json={"permission_class": "runtime_execute", "is_enabled": True}, headers=headers)
            conn = sqlite3.connect(env.db_path)
            conn.execute("DELETE FROM prompt_runtime_render_validation_ledger")
            conn.commit(); conn.close()
            self._record_render(env, status="failed", detail="secret token bad")
            invalid_render = client.post("/v1/prompt-runtime/gates/evaluate", json=self._payload(), headers=headers)
            self.assertEqual(invalid_render.json()["admission_status"], "blocked_invalid_render")
            self.assertNotIn("secret token bad", invalid_render.text)
            conn = sqlite3.connect(env.db_path)
            conn.execute("UPDATE prompt_runtime_render_validation_ledger SET validation_status='passed', invalid_reason_detail=NULL")
            conn.commit(); conn.close()
            client.put("/v1/prompt-runtime/compatibility/CREATE_BULK_JSON_DRAFT/channel", json={"compatibility_status": "blocked", "policy_code": "p_blocked"}, headers=headers)
            blocked_compat = client.post("/v1/prompt-runtime/gates/evaluate", json=self._payload(), headers=headers)
            self.assertEqual(blocked_compat.json()["admission_status"], "blocked_target_compatibility")
            client.put("/v1/prompt-runtime/compatibility/CREATE_BULK_JSON_DRAFT/channel", json={"compatibility_status": "allowed", "policy_code": "p_allowed"}, headers=headers)
            missing_impl = client.post("/v1/prompt-runtime/gates/evaluate", json=self._payload(), headers=headers)
            self.assertEqual(missing_impl.json()["failure_reason_code"], "target_resolver_implementation_missing")
            after = self._counts(env)
            self.assertEqual(after, before)

    def _counts(self, env) -> dict[str, int]:
        conn = sqlite3.connect(env.db_path)
        try:
            return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in ("prompt_execution_attempts", "prompt_execution_groups", "prompt_linked_action_dispatch_attempts")}
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
