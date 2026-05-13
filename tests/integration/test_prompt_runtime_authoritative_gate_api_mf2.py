from __future__ import annotations

import importlib
import sqlite3
import unittest

from fastapi.testclient import TestClient

from services.prompt_registry.authoritative_gate import RenderValidationService
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPromptRuntimeAuthoritativeGateApiMf2(unittest.TestCase):
    def _client(self):
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _record(self, env, **overrides):
        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        try:
            payload = {
                "prompt_record_id": 1,
                "prompt_version_id": 77,
                "binding_fingerprint": "bind-api",
                "render_result_hash": "render-api",
                "validation_status": "passed",
                "validation_schema_version": "v1",
                "validator_code": "api-test",
                "validated_at": "2026-01-01T00:00:00Z",
            }
            payload.update(overrides)
            row = RenderValidationService(conn).record_validation(**payload)
            conn.commit()
            return row
        finally:
            conn.close()

    def test_render_validation_get_endpoints_require_basic_auth(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            self.assertEqual(client.get("/v1/prompt-runtime/render-validations").status_code, 401)
            self.assertEqual(
                client.get(
                    "/v1/prompt-runtime/render-validations/latest",
                    params={"prompt_version_id": 77, "binding_fingerprint": "bind-api", "render_result_hash": "render-api"},
                ).status_code,
                401,
            )

    def test_latest_endpoint_trusted_passed_and_missing_untrusted(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._record(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            trusted = client.get(
                "/v1/prompt-runtime/render-validations/latest",
                params={"prompt_version_id": 77, "binding_fingerprint": "bind-api", "render_result_hash": "render-api"},
                headers=headers,
            )
            self.assertEqual(trusted.status_code, 200, trusted.text)
            self.assertTrue(trusted.json()["trusted"])
            self.assertEqual(trusted.json()["verdict"], "trusted")

            missing = client.get(
                "/v1/prompt-runtime/render-validations/latest",
                params={"prompt_version_id": 77, "binding_fingerprint": "absent", "render_result_hash": "render-api"},
                headers=headers,
            )
            self.assertEqual(missing.status_code, 200, missing.text)
            self.assertFalse(missing.json()["trusted"])
            self.assertEqual(missing.json()["verdict"], "missing")
            self.assertEqual(missing.json()["failure_reason_code"], "missing_render_validation_authority")

    def test_list_filters_and_secret_safe_response(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._record(env, validation_status="passed", validated_at="2026-01-01T00:00:00Z")
            self._record(
                env,
                binding_fingerprint="bind-api-failed",
                validation_status="failed",
                invalid_reason_detail="secret token abc",
                validated_at="2026-01-02T00:00:00Z",
            )
            self._record(env, prompt_version_id=88, validation_status="error", validated_at="2026-01-03T00:00:00Z")
            client = self._client()
            headers = basic_auth_header("admin", "testpass")

            by_version = client.get("/v1/prompt-runtime/render-validations", params={"prompt_version_id": 77}, headers=headers)
            self.assertEqual(by_version.status_code, 200, by_version.text)
            self.assertEqual(len(by_version.json()["items"]), 2)

            failed = client.get("/v1/prompt-runtime/render-validations", params={"validation_status": "failed"}, headers=headers)
            self.assertEqual(failed.status_code, 200, failed.text)
            self.assertEqual(len(failed.json()["items"]), 1)
            self.assertEqual(failed.json()["items"][0]["invalid_reason_detail"], "[redacted]")
            self.assertNotIn("secret token abc", failed.text)

    def test_operator_mutation_endpoint_absent_and_gets_do_not_mutate_or_execute(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._record(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            before = self._counts(env)

            post = client.post(
                "/v1/prompt-runtime/render-validations",
                json={"validation_status": "passed"},
                headers=headers,
            )
            self.assertIn(post.status_code, (404, 405))
            self.assertEqual(client.get("/v1/prompt-runtime/render-validations", headers=headers).status_code, 200)
            self.assertEqual(
                client.get(
                    "/v1/prompt-runtime/render-validations/latest",
                    params={"prompt_version_id": 77, "binding_fingerprint": "bind-api", "render_result_hash": "render-api"},
                    headers=headers,
                ).status_code,
                200,
            )
            after = self._counts(env)
            self.assertEqual(after, before)

    def _counts(self, env) -> dict[str, int]:
        conn = sqlite3.connect(env.db_path)
        try:
            tables = (
                "prompt_runtime_render_validation_ledger",
                "prompt_execution_attempts",
                "prompt_execution_groups",
                "prompt_linked_action_dispatch_attempts",
            )
            return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
