from __future__ import annotations

import importlib
import sqlite3
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPromptRuntimeAuthoritativeGateApiMf1(unittest.TestCase):
    def _client(self):
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return TestClient(mod.app)

    def test_api_basic_auth_required_and_put_uses_basic_auth_subject(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()

            self.assertEqual(client.get("/v1/prompt-runtime/capabilities").status_code, 401)
            headers = basic_auth_header("admin", "testpass")
            response = client.put(
                "/v1/prompt-runtime/capabilities/CREATE_BULK_JSON_DRAFT",
                json={
                    "execution_enabled": True,
                    "required_permission_class": "runtime_execute",
                    "status": "active",
                    "updated_by_operator": "spoofed-body",
                },
                headers=headers,
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["updated_by_operator"], "admin")

            detail = client.get("/v1/prompt-runtime/capabilities/CREATE_BULK_JSON_DRAFT", headers=headers)
            self.assertEqual(detail.status_code, 200, detail.text)
            self.assertTrue(detail.json()["admissible"])
            self.assertEqual(detail.json()["required_permission_class"], "runtime_execute")

            op_response = client.put(
                "/v1/prompt-runtime/operators/operator-a/permissions",
                json={"permission_class": "runtime_operate", "is_enabled": True, "updated_by_operator": "spoofed-body"},
                headers=headers,
            )
            self.assertEqual(op_response.status_code, 200, op_response.text)
            self.assertEqual(op_response.json()["updated_by_operator"], "admin")

            op_detail = client.get("/v1/prompt-runtime/operators/operator-a/permissions", headers=headers)
            self.assertEqual(op_detail.status_code, 200, op_detail.text)
            self.assertTrue(op_detail.json()["admissible"])

    def test_get_endpoints_do_not_mutate_or_execute_actions(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            before = self._counts(env)
            self.assertEqual(client.get("/v1/prompt-runtime/capabilities", headers=headers).status_code, 200)
            self.assertEqual(client.get("/v1/prompt-runtime/capabilities/MISSING", headers=headers).status_code, 404)
            self.assertEqual(client.get("/v1/prompt-runtime/operators/missing/permissions", headers=headers).status_code, 404)
            after = self._counts(env)
            self.assertEqual(after, before)

    def _counts(self, env) -> dict[str, int]:
        conn = sqlite3.connect(env.db_path)
        try:
            tables = (
                "prompt_runtime_capability_registry",
                "prompt_runtime_operator_permissions",
                "prompt_execution_attempts",
                "prompt_execution_groups",
                "prompt_linked_action_dispatch_attempts",
            )
            return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
