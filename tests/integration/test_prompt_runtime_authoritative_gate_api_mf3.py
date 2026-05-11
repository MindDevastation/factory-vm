from __future__ import annotations

import importlib
import sqlite3
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPromptRuntimeAuthoritativeGateApiMf3(unittest.TestCase):
    def _client(self):
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        return TestClient(mod.app)

    def test_all_mf3_endpoints_require_basic_auth(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            endpoints = [
                ("get", "/v1/prompt-runtime/resolvers"),
                ("get", "/v1/prompt-runtime/resolvers/CAP/channel"),
                ("put", "/v1/prompt-runtime/resolvers/CAP/channel"),
                ("get", "/v1/prompt-runtime/compatibility"),
                ("get", "/v1/prompt-runtime/compatibility/CAP/channel"),
                ("put", "/v1/prompt-runtime/compatibility/CAP/channel"),
            ]
            for method, path in endpoints:
                if method == "put":
                    response = client.put(path, json={})
                else:
                    response = client.get(path)
                self.assertEqual(response.status_code, 401, path)

    def test_puts_use_basic_auth_subject_and_detail_returns_evaluation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            resolver = client.put(
                "/v1/prompt-runtime/resolvers/CREATE_BULK_JSON_DRAFT/channel",
                json={
                    "resolver_code": "channel_db_resolver",
                    "snapshot_schema_version": "v1",
                    "is_enabled": True,
                    "updated_by_operator": "spoofed-body",
                },
                headers=headers,
            )
            self.assertEqual(resolver.status_code, 200, resolver.text)
            self.assertEqual(resolver.json()["updated_by_operator"], "admin")
            resolver_detail = client.get("/v1/prompt-runtime/resolvers/CREATE_BULK_JSON_DRAFT/channel", headers=headers)
            self.assertEqual(resolver_detail.status_code, 200, resolver_detail.text)
            self.assertTrue(resolver_detail.json()["admissible"])
            self.assertEqual(resolver_detail.json()["resolver_code"], "channel_db_resolver")

            compatibility = client.put(
                "/v1/prompt-runtime/compatibility/CREATE_BULK_JSON_DRAFT/channel",
                json={"compatibility_status": "allowed", "policy_code": "bulk_json_channel_allowed", "updated_by_operator": "spoofed-body"},
                headers=headers,
            )
            self.assertEqual(compatibility.status_code, 200, compatibility.text)
            self.assertEqual(compatibility.json()["updated_by_operator"], "admin")
            compatibility_detail = client.get("/v1/prompt-runtime/compatibility/CREATE_BULK_JSON_DRAFT/channel", headers=headers)
            self.assertEqual(compatibility_detail.status_code, 200, compatibility_detail.text)
            self.assertTrue(compatibility_detail.json()["admissible"])
            self.assertEqual(compatibility_detail.json()["compatibility_status"], "allowed")

    def test_list_filters_and_secret_safe_notes(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            client.put(
                "/v1/prompt-runtime/resolvers/CAP_A/channel",
                json={"resolver_code": "r1", "snapshot_schema_version": "v1", "is_enabled": True, "notes": "secret token resolver"},
                headers=headers,
            )
            client.put(
                "/v1/prompt-runtime/resolvers/CAP_A/release",
                json={"resolver_code": "r2", "snapshot_schema_version": "v1", "is_enabled": False},
                headers=headers,
            )
            client.put(
                "/v1/prompt-runtime/compatibility/CAP_A/channel",
                json={"compatibility_status": "allowed", "policy_code": "p1", "notes": "password policy detail"},
                headers=headers,
            )
            client.put(
                "/v1/prompt-runtime/compatibility/CAP_B/channel",
                json={"compatibility_status": "blocked", "policy_code": "p2"},
                headers=headers,
            )

            resolvers = client.get("/v1/prompt-runtime/resolvers", params={"capability_code": "CAP_A"}, headers=headers)
            self.assertEqual(resolvers.status_code, 200, resolvers.text)
            self.assertEqual(len(resolvers.json()["items"]), 2)
            enabled = client.get("/v1/prompt-runtime/resolvers", params={"target_type": "channel", "is_enabled": True}, headers=headers)
            self.assertEqual(enabled.status_code, 200, enabled.text)
            self.assertEqual(len(enabled.json()["items"]), 1)
            self.assertEqual(enabled.json()["items"][0]["notes"], "[redacted]")
            self.assertNotIn("secret token resolver", enabled.text)

            allowed = client.get("/v1/prompt-runtime/compatibility", params={"compatibility_status": "allowed"}, headers=headers)
            self.assertEqual(allowed.status_code, 200, allowed.text)
            self.assertEqual(len(allowed.json()["items"]), 1)
            self.assertEqual(allowed.json()["items"][0]["notes"], "[redacted]")
            self.assertNotIn("password policy detail", allowed.text)

    def test_gets_do_not_mutate_or_execute_and_no_resolve_preview_yet(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client()
            headers = basic_auth_header("admin", "testpass")
            client.put(
                "/v1/prompt-runtime/resolvers/CAP/channel",
                json={"resolver_code": "r", "snapshot_schema_version": "v1", "is_enabled": True},
                headers=headers,
            )
            client.put(
                "/v1/prompt-runtime/compatibility/CAP/channel",
                json={"compatibility_status": "allowed", "policy_code": "p"},
                headers=headers,
            )
            before = self._counts(env)
            self.assertEqual(client.get("/v1/prompt-runtime/resolvers", headers=headers).status_code, 200)
            self.assertEqual(client.get("/v1/prompt-runtime/resolvers/CAP/channel", headers=headers).status_code, 200)
            self.assertEqual(client.get("/v1/prompt-runtime/compatibility", headers=headers).status_code, 200)
            self.assertEqual(client.get("/v1/prompt-runtime/compatibility/CAP/channel", headers=headers).status_code, 200)
            # MF4 introduces POST resolve-preview; MF3 GET surfaces must still remain read-only.
            self.assertIn(client.get("/v1/prompt-runtime/targets/resolve-preview", headers=headers).status_code, (404, 405))
            after = self._counts(env)
            self.assertEqual(after, before)

    def _counts(self, env) -> dict[str, int]:
        conn = sqlite3.connect(env.db_path)
        try:
            tables = (
                "prompt_runtime_target_resolver_registry",
                "prompt_runtime_target_compatibility_policy",
                "prompt_execution_attempts",
                "prompt_execution_groups",
                "prompt_linked_action_dispatch_attempts",
            )
            return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
