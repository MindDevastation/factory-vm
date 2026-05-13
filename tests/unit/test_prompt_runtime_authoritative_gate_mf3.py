from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import TargetCompatibilityService, TargetResolverRegistryService
from tests._helpers import seed_minimal_db, temp_env


class TestPromptRuntimeAuthoritativeGateMf3(unittest.TestCase):
    def test_resolver_and_compatibility_schema_tables_columns_and_indexes_exist(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertIn("prompt_runtime_target_resolver_registry", tables)
                self.assertIn("prompt_runtime_target_compatibility_policy", tables)
                resolver_cols = {r["name"] for r in conn.execute("PRAGMA table_info(prompt_runtime_target_resolver_registry)").fetchall()}
                self.assertTrue(
                    {
                        "id",
                        "capability_code",
                        "target_type",
                        "resolver_code",
                        "snapshot_schema_version",
                        "is_enabled",
                        "notes",
                        "updated_by_operator",
                        "created_at",
                        "updated_at",
                    }.issubset(resolver_cols)
                )
                compatibility_cols = {r["name"] for r in conn.execute("PRAGMA table_info(prompt_runtime_target_compatibility_policy)").fetchall()}
                self.assertTrue(
                    {
                        "id",
                        "capability_code",
                        "target_type",
                        "compatibility_status",
                        "policy_code",
                        "notes",
                        "updated_by_operator",
                        "created_at",
                        "updated_at",
                    }.issubset(compatibility_cols)
                )
                indexes = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
                self.assertIn("idx_prompt_runtime_target_resolver_registry_key", indexes)
                self.assertIn("idx_prompt_runtime_target_compatibility_policy_key", indexes)
            finally:
                conn.close()

    def test_resolver_exact_lookup_enabled_pass_and_fail_closed_cases(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = TargetResolverRegistryService(conn)
                missing = service.evaluate("CREATE_BULK_JSON_DRAFT", "channel")
                self.assertFalse(missing.admissible)
                self.assertEqual(missing.failure_reason_code, "missing_target_resolver_authority")

                row = service.upsert(
                    "CREATE_BULK_JSON_DRAFT",
                    "channel",
                    {"resolver_code": "channel_db_resolver", "snapshot_schema_version": "v1", "is_enabled": True},
                    updated_by_operator="admin",
                )
                self.assertEqual(row["capability_code"], "CREATE_BULK_JSON_DRAFT")
                self.assertTrue(service.evaluate("CREATE_BULK_JSON_DRAFT", "channel").admissible)
                self.assertFalse(service.evaluate("CREATE_BULK_JSON_DRAFT", "release").admissible)

                service.upsert(
                    "CREATE_BULK_JSON_DRAFT",
                    "channel",
                    {"resolver_code": "channel_db_resolver", "snapshot_schema_version": "v1", "is_enabled": False},
                    updated_by_operator="admin",
                )
                disabled = service.evaluate("CREATE_BULK_JSON_DRAFT", "channel")
                self.assertFalse(disabled.admissible)
                self.assertEqual(disabled.failure_reason_code, "target_resolver_disabled")

                conn.execute(
                    "INSERT INTO prompt_runtime_target_resolver_registry(capability_code,target_type,resolver_code,snapshot_schema_version,is_enabled,updated_by_operator,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    ("CAP_BLANK_RESOLVER", "channel", "", "v1", 1, "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                )
                self.assertEqual(service.evaluate("CAP_BLANK_RESOLVER", "channel").failure_reason_code, "target_resolver_code_missing")
                conn.execute(
                    "INSERT INTO prompt_runtime_target_resolver_registry(capability_code,target_type,resolver_code,snapshot_schema_version,is_enabled,updated_by_operator,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    ("CAP_BLANK_SCHEMA", "channel", "resolver", "", 1, "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                )
                self.assertEqual(service.evaluate("CAP_BLANK_SCHEMA", "channel").failure_reason_code, "target_resolver_snapshot_schema_missing")
            finally:
                conn.close()

    def test_compatibility_allowed_pass_and_fail_closed_cases(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = TargetCompatibilityService(conn)
                missing = service.evaluate("CREATE_BULK_JSON_DRAFT", "channel")
                self.assertFalse(missing.admissible)
                self.assertEqual(missing.failure_reason_code, "missing_target_compatibility_authority")

                service.upsert(
                    "CREATE_BULK_JSON_DRAFT",
                    "channel",
                    {"compatibility_status": "allowed", "policy_code": "bulk_json_channel_allowed"},
                    updated_by_operator="admin",
                )
                self.assertTrue(service.evaluate("CREATE_BULK_JSON_DRAFT", "channel").admissible)
                self.assertFalse(service.evaluate("CREATE_BULK_JSON_DRAFT", "release").admissible)

                for status in ("blocked", "deprecated"):
                    service.upsert(
                        "CREATE_BULK_JSON_DRAFT",
                        "channel",
                        {"compatibility_status": status, "policy_code": f"policy_{status}"},
                        updated_by_operator="admin",
                    )
                    result = service.evaluate("CREATE_BULK_JSON_DRAFT", "channel")
                    self.assertFalse(result.admissible)
                    self.assertEqual(result.failure_reason_code, f"target_compatibility_{status}")

                conn.execute(
                    "INSERT INTO prompt_runtime_target_compatibility_policy(capability_code,target_type,compatibility_status,policy_code,updated_by_operator,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    ("CAP_BLANK_POLICY", "channel", "allowed", "", "admin", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                )
                self.assertEqual(service.evaluate("CAP_BLANK_POLICY", "channel").failure_reason_code, "target_compatibility_policy_code_missing")
                with self.assertRaises(ValueError):
                    service.upsert("CAP", "channel", {"compatibility_status": "enabled", "policy_code": "p"}, updated_by_operator="admin")
            finally:
                conn.close()

    def test_upserts_replace_exact_key_only_and_no_wildcard_fallback(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                resolvers = TargetResolverRegistryService(conn)
                compatibility = TargetCompatibilityService(conn)
                resolvers.upsert("CAP", "channel", {"resolver_code": "r1", "snapshot_schema_version": "v1", "is_enabled": True}, updated_by_operator="admin")
                resolvers.upsert("CAP", "release", {"resolver_code": "r2", "snapshot_schema_version": "v1", "is_enabled": True}, updated_by_operator="admin")
                resolvers.upsert("CAP", "channel", {"resolver_code": "r1b", "snapshot_schema_version": "v2", "is_enabled": True}, updated_by_operator="admin")
                self.assertEqual(resolvers.get_row("CAP", "channel")["resolver_code"], "r1b")
                self.assertEqual(resolvers.get_row("CAP", "release")["resolver_code"], "r2")
                self.assertIsNone(resolvers.get_row("*", "channel"))

                compatibility.upsert("CAP", "channel", {"compatibility_status": "allowed", "policy_code": "p1"}, updated_by_operator="admin")
                compatibility.upsert("CAP", "release", {"compatibility_status": "blocked", "policy_code": "p2"}, updated_by_operator="admin")
                compatibility.upsert("CAP", "channel", {"compatibility_status": "deprecated", "policy_code": "p3"}, updated_by_operator="admin")
                self.assertEqual(compatibility.get_row("CAP", "channel")["policy_code"], "p3")
                self.assertEqual(compatibility.get_row("CAP", "release")["policy_code"], "p2")
                self.assertIsNone(compatibility.get_row("*", "channel"))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
