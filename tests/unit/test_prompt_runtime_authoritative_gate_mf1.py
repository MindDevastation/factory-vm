from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import CapabilityGateService, OperatorPermissionService, permission_class_satisfies
from tests._helpers import seed_minimal_db, temp_env


class TestPromptRuntimeAuthoritativeGateMf1(unittest.TestCase):
    def test_schema_tables_columns_and_indexes(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertIn("prompt_runtime_capability_registry", tables)
                self.assertIn("prompt_runtime_operator_permissions", tables)

                cols = lambda t: {r["name"] for r in conn.execute(f"PRAGMA table_info({t})").fetchall()}
                self.assertTrue({"id", "capability_code", "execution_enabled", "required_permission_class", "status", "notes", "updated_by_operator", "created_at", "updated_at"}.issubset(cols("prompt_runtime_capability_registry")))
                self.assertTrue({"id", "operator_subject", "permission_class", "is_enabled", "notes", "updated_by_operator", "created_at", "updated_at"}.issubset(cols("prompt_runtime_operator_permissions")))

                indexes = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
                self.assertIn("idx_prompt_runtime_capability_registry_code", indexes)
                self.assertIn("idx_prompt_runtime_operator_permissions_subject", indexes)
            finally:
                conn.close()

    def test_capability_active_enabled_passes_and_missing_disabled_deprecated_fail_closed(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = CapabilityGateService(conn)
                self.assertFalse(service.evaluate("CREATE_BULK_JSON_DRAFT").admissible)
                self.assertEqual(service.evaluate("CREATE_BULK_JSON_DRAFT").failure_reason_code, "missing_capability_authority")

                service.upsert("CREATE_BULK_JSON_DRAFT", {"execution_enabled": True, "required_permission_class": "runtime_execute", "status": "active"}, updated_by_operator="admin")
                self.assertTrue(service.evaluate("CREATE_BULK_JSON_DRAFT").admissible)

                service.upsert("CREATE_BULK_JSON_DRAFT", {"execution_enabled": False, "required_permission_class": "runtime_execute", "status": "active"}, updated_by_operator="admin")
                self.assertFalse(service.evaluate("CREATE_BULK_JSON_DRAFT").admissible)
                self.assertEqual(service.evaluate("CREATE_BULK_JSON_DRAFT").failure_reason_code, "capability_execution_disabled")

                service.upsert("CREATE_BULK_JSON_DRAFT", {"execution_enabled": True, "required_permission_class": "runtime_execute", "status": "deprecated"}, updated_by_operator="admin")
                self.assertFalse(service.evaluate("CREATE_BULK_JSON_DRAFT").admissible)
                self.assertEqual(service.evaluate("CREATE_BULK_JSON_DRAFT").failure_reason_code, "capability_status_deprecated")
            finally:
                conn.close()

    def test_operator_permission_enabled_passes_and_missing_disabled_fail_closed(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = OperatorPermissionService(conn)
                self.assertFalse(service.evaluate("admin").admissible)
                self.assertEqual(service.evaluate("admin").failure_reason_code, "missing_operator_permission_authority")

                service.upsert("admin", {"permission_class": "runtime_execute", "is_enabled": True}, updated_by_operator="admin")
                self.assertTrue(service.evaluate("admin").admissible)

                service.upsert("admin", {"permission_class": "runtime_execute", "is_enabled": False}, updated_by_operator="admin")
                self.assertFalse(service.evaluate("admin").admissible)
                self.assertEqual(service.evaluate("admin").failure_reason_code, "operator_permission_disabled")
            finally:
                conn.close()

    def test_permission_hierarchy_is_monotonic(self) -> None:
        self.assertTrue(permission_class_satisfies("runtime_admin", "runtime_view"))
        self.assertTrue(permission_class_satisfies("runtime_operate", "runtime_execute"))
        self.assertTrue(permission_class_satisfies("runtime_execute", "runtime_execute"))
        self.assertFalse(permission_class_satisfies("runtime_view", "runtime_execute"))
        self.assertFalse(permission_class_satisfies("runtime_execute", "runtime_admin"))
        self.assertFalse(permission_class_satisfies(None, "runtime_view"))


if __name__ == "__main__":
    unittest.main()
