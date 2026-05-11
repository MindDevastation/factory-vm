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


class TestPromptRuntimeAuthoritativeGateApiMf4(unittest.TestCase):
    def _client(self):
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return mod, mod.TestClient(mod.app) if hasattr(mod, "TestClient") else None

    def _test_client(self):
        from fastapi.testclient import TestClient

        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return TestClient(mod.app)

    def setUp(self) -> None:
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()

    def tearDown(self) -> None:
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()

    def _seed_authorities(self, client, headers, *, resolver_code="fake_resolver", compatibility_status="allowed") -> None:
        self.assertEqual(
            client.put(
                "/v1/prompt-runtime/resolvers/CREATE_BULK_JSON_DRAFT/channel",
                json={"resolver_code": resolver_code, "snapshot_schema_version": "v1", "is_enabled": True},
                headers=headers,
            ).status_code,
            200,
        )
        self.assertEqual(
            client.put(
                "/v1/prompt-runtime/compatibility/CREATE_BULK_JSON_DRAFT/channel",
                json={"compatibility_status": compatibility_status, "policy_code": f"policy_{compatibility_status}"},
                headers=headers,
            ).status_code,
            200,
        )

    def test_resolve_preview_requires_basic_auth(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._test_client()
            response = client.post("/v1/prompt-runtime/targets/resolve-preview", json={})
            self.assertEqual(response.status_code, 401)

    def test_successful_resolve_preview_persists_snapshot_and_is_secret_safe(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            TARGET_SNAPSHOT_RESOLVER_REGISTRY.register("fake_resolver", lambda **kwargs: payload_for_ref(kwargs["target_ref"]) | {"resolver_metadata": {"secret_token": "abc"}})
            client = self._test_client()
            headers = basic_auth_header("admin", "testpass")
            self._seed_authorities(client, headers)
            response = client.post(
                "/v1/prompt-runtime/targets/resolve-preview",
                json={"capability_code": "CREATE_BULK_JSON_DRAFT", "target_type": "channel", "target_ref": "target-1", "resolver_code": "evil_request_resolver"},
                headers=headers,
            )
            self.assertEqual(response.status_code, 200, response.text)
            body = response.json()
            self.assertEqual(body["admission_status"], "admissible")
            self.assertEqual(body["resolver_code"], "fake_resolver")
            self.assertIsNotNone(body["snapshot_hash"])
            self.assertIsNotNone(body["ledger_id"])
            self.assertEqual(body["snapshot_payload"]["resolver_metadata"]["secret_token"], "[redacted]")
            self.assertNotIn("abc", response.text)
            conn = sqlite3.connect(env.db_path)
            try:
                count = int(conn.execute("SELECT COUNT(*) FROM prompt_runtime_target_snapshot_ledger WHERE id=?", (body["ledger_id"],)).fetchone()[0])
                self.assertEqual(count, 1)
            finally:
                conn.close()

    def test_blocked_paths_and_no_execution_side_effects(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._test_client()
            headers = basic_auth_header("admin", "testpass")
            before = self._counts(env)
            missing = client.post(
                "/v1/prompt-runtime/targets/resolve-preview",
                json={"capability_code": "CREATE_BULK_JSON_DRAFT", "target_type": "channel", "target_ref": "target-1"},
                headers=headers,
            )
            self.assertEqual(missing.json()["failure_reason_code"], "missing_target_resolver_authority")
            self._seed_authorities(client, headers, resolver_code="missing_impl")
            missing_impl = client.post(
                "/v1/prompt-runtime/targets/resolve-preview",
                json={"capability_code": "CREATE_BULK_JSON_DRAFT", "target_type": "channel", "target_ref": "target-1"},
                headers=headers,
            )
            self.assertEqual(missing_impl.json()["failure_reason_code"], "target_resolver_implementation_missing")
            self._seed_authorities(client, headers, resolver_code="missing_impl", compatibility_status="blocked")
            blocked = client.post(
                "/v1/prompt-runtime/targets/resolve-preview",
                json={"capability_code": "CREATE_BULK_JSON_DRAFT", "target_type": "channel", "target_ref": "target-1"},
                headers=headers,
            )
            self.assertEqual(blocked.json()["failure_reason_code"], "target_compatibility_blocked")
            after = self._counts(env)
            self.assertEqual(after, before)

    def test_get_resolve_preview_does_not_mutate(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._test_client()
            headers = basic_auth_header("admin", "testpass")
            before = self._counts(env)
            response = client.get("/v1/prompt-runtime/targets/resolve-preview", headers=headers)
            self.assertIn(response.status_code, (404, 405))
            after = self._counts(env)
            self.assertEqual(after, before)

    def _counts(self, env) -> dict[str, int]:
        conn = sqlite3.connect(env.db_path)
        try:
            tables = (
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
