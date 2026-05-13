from __future__ import annotations

import importlib
import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import RenderValidationService, TARGET_SNAPSHOT_RESOLVER_REGISTRY
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


def snapshot(target_ref: str) -> dict:
    return {
        "target_type": "channel",
        "target_ref": target_ref,
        "target_display_label": "Channel",
        "target_state_code": "ready",
        "target_exists": True,
        "target_updated_at": "2026-01-01T00:00:00Z",
        "compatibility_inputs": {"kind": "LONG"},
        "resolver_metadata": {"secret_token": "abc"},
    }


class TestPromptRuntimeAuthoritativeGateApiMf6(unittest.TestCase):
    def setUp(self) -> None:
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()

    def tearDown(self) -> None:
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()

    def _client(self):
        from fastapi.testclient import TestClient

        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed(self, env, client, headers) -> None:
        client.put("/v1/prompt-runtime/capabilities/CAP_A", json={"execution_enabled": True, "required_permission_class": "runtime_execute", "status": "active", "notes": "secret note"}, headers=headers)
        client.put("/v1/prompt-runtime/operators/admin/permissions", json={"permission_class": "runtime_execute", "is_enabled": True}, headers=headers)
        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        try:
            RenderValidationService(conn).record_validation(prompt_record_id=1, prompt_version_id=10, binding_fingerprint="bind", render_result_hash="render", validation_status="failed", validation_schema_version="v1", validator_code="api", invalid_reason_detail="password bad")
            conn.commit()
        finally:
            conn.close()
        client.put("/v1/prompt-runtime/resolvers/CAP_A/channel", json={"resolver_code": "fake_resolver", "snapshot_schema_version": "v1", "is_enabled": True}, headers=headers)
        client.put("/v1/prompt-runtime/compatibility/CAP_A/channel", json={"compatibility_status": "allowed", "policy_code": "p", "notes": "token note"}, headers=headers)
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.register("fake_resolver", lambda **kwargs: snapshot(kwargs["target_ref"]))
        client.post("/v1/prompt-runtime/targets/resolve-preview", json={"capability_code": "CAP_A", "target_type": "channel", "target_ref": "target-1"}, headers=headers)

    def test_summary_and_report_require_basic_auth(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            self.assertEqual(client.get("/v1/prompt-runtime/authority/summary").status_code, 401)
            self.assertEqual(client.get("/v1/prompt-runtime/authority/report").status_code, 401)

    def test_summary_report_filters_secret_safe_and_no_mutation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            self._seed(env, client, headers)
            before = self._counts(env)
            summary = client.get("/v1/prompt-runtime/authority/summary", headers=headers)
            self.assertEqual(summary.status_code, 200, summary.text)
            body = summary.json()
            for key in ("capabilities", "operator_permissions", "render_validations", "resolvers", "compatibility", "snapshots"):
                self.assertIn(key, body)
            self.assertEqual(body["capabilities"]["total"], 1)
            self.assertEqual(body["snapshots"]["total"], 1)

            report = client.get("/v1/prompt-runtime/authority/report", params={"capability_code": "CAP_A", "target_type": "channel", "operator_subject": "admin", "limit": 999}, headers=headers)
            self.assertEqual(report.status_code, 200, report.text)
            payload = report.json()
            self.assertEqual(payload["limit"], 500)
            self.assertEqual(payload["capabilities"][0]["capability_code"], "CAP_A")
            self.assertEqual(payload["operator_permissions"][0]["operator_subject"], "admin")
            self.assertEqual(payload["compatibility"][0]["notes"], "[redacted]")
            self.assertEqual(payload["render_validations"][0]["invalid_reason_detail"], "[redacted]")
            self.assertEqual(payload["snapshots"][0]["snapshot_payload"]["resolver_metadata"]["secret_token"], "[redacted]")
            self.assertNotIn("password bad", report.text)
            self.assertNotIn("token note", report.text)
            self.assertEqual(self._counts(env), before)

    def test_no_execution_controls_or_side_effects(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            before = self._counts(env)
            self.assertIn(client.get("/ui/prompt-runtime/authority", headers=headers).status_code, (404, 405))
            self.assertIn(client.post("/v1/prompt-runtime/authority/summary", json={}, headers=headers).status_code, (404, 405))
            self.assertIn(client.post("/v1/prompt-runtime/authority/report", json={}, headers=headers).status_code, (404, 405))
            self.assertEqual(self._counts(env), before)

    def _counts(self, env) -> dict[str, int]:
        conn = sqlite3.connect(env.db_path)
        try:
            tables = (
                "prompt_runtime_capability_registry",
                "prompt_runtime_operator_permissions",
                "prompt_runtime_render_validation_ledger",
                "prompt_runtime_target_resolver_registry",
                "prompt_runtime_target_compatibility_policy",
                "prompt_runtime_target_snapshot_ledger",
                "prompt_execution_attempts",
                "prompt_execution_groups",
                "prompt_linked_action_dispatch_attempts",
            )
            return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
