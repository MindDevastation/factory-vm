from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import (
    CapabilityGateService,
    OperatorPermissionService,
    PromptRuntimeGateEvaluationService,
    RenderValidationService,
    TargetCompatibilityService,
    TargetResolverRegistryService,
    TargetSnapshotResolverRegistry,
)
from tests._helpers import seed_minimal_db, temp_env


def snapshot_payload(target_ref: str = "target-1", *, valid: bool = True) -> dict:
    payload = {
        "target_type": "channel",
        "target_ref": target_ref,
        "target_display_label": f"Channel {target_ref}",
        "target_state_code": "ready",
        "target_exists": True,
        "target_updated_at": "2026-01-01T00:00:00Z",
        "compatibility_inputs": {"kind": "LONG"},
        "resolver_metadata": {"resolver": "fake"},
    }
    if not valid:
        payload.pop("target_display_label")
    return payload


class TestPromptRuntimeAuthoritativeGateMf5(unittest.TestCase):
    def _setup_all(self, conn: sqlite3.Connection, *, permission="runtime_execute", render_status="passed", resolver_enabled=True, compatibility_status="allowed", resolver_code="fake_resolver") -> TargetSnapshotResolverRegistry:
        CapabilityGateService(conn).upsert("CREATE_BULK_JSON_DRAFT", {"execution_enabled": True, "required_permission_class": "runtime_execute", "status": "active"}, updated_by_operator="admin")
        OperatorPermissionService(conn).upsert("operator-a", {"permission_class": permission, "is_enabled": True}, updated_by_operator="admin")
        RenderValidationService(conn).record_validation(prompt_record_id=1, prompt_version_id=10, binding_fingerprint="bind", render_result_hash="render", validation_status=render_status, validation_schema_version="v1", validator_code="unit")
        TargetResolverRegistryService(conn).upsert("CREATE_BULK_JSON_DRAFT", "channel", {"resolver_code": resolver_code, "snapshot_schema_version": "v1", "is_enabled": resolver_enabled}, updated_by_operator="admin")
        TargetCompatibilityService(conn).upsert("CREATE_BULK_JSON_DRAFT", "channel", {"compatibility_status": compatibility_status, "policy_code": f"policy_{compatibility_status}"}, updated_by_operator="admin")
        registry = TargetSnapshotResolverRegistry()
        registry.register("fake_resolver", lambda **kwargs: snapshot_payload(kwargs["target_ref"]))
        return registry

    def _evaluate(self, conn: sqlite3.Connection, registry: TargetSnapshotResolverRegistry | None = None):
        return PromptRuntimeGateEvaluationService(conn, registry).evaluate(
            operator_subject="operator-a",
            capability_code="CREATE_BULK_JSON_DRAFT",
            prompt_version_id=10,
            binding_fingerprint="bind",
            render_result_hash="render",
            target_type="channel",
            target_ref="target-1",
        )

    def test_all_sources_present_is_admissible_and_includes_snapshot_hash(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                registry = self._setup_all(conn)
                result = self._evaluate(conn, registry)
                self.assertEqual(result.admission_status, "admissible")
                self.assertIsNone(result.failure_reason_code)
                self.assertEqual(result.required_permission_class, "runtime_execute")
                self.assertEqual(result.resolved_permission_class, "runtime_execute")
                self.assertEqual(result.render_validation_status, "trusted")
                self.assertEqual(result.target_compatibility_status, "allowed")
                self.assertIsNotNone(result.target_snapshot_hash)
                self.assertIn("target_snapshot", result.authoritative_source_summary)
            finally:
                conn.close()

    def test_capability_operator_permission_blocks(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                registry = self._setup_all(conn)
                conn.execute("DELETE FROM prompt_runtime_capability_registry")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_missing_authority")
                self._setup_all(conn)
                CapabilityGateService(conn).upsert("CREATE_BULK_JSON_DRAFT", {"execution_enabled": False, "required_permission_class": "runtime_execute", "status": "active"}, updated_by_operator="admin")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_disabled_capability")
                CapabilityGateService(conn).upsert("CREATE_BULK_JSON_DRAFT", {"execution_enabled": True, "required_permission_class": "runtime_execute", "status": "deprecated"}, updated_by_operator="admin")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_disabled_capability")
                CapabilityGateService(conn).upsert("CREATE_BULK_JSON_DRAFT", {"execution_enabled": True, "required_permission_class": "runtime_execute", "status": "active"}, updated_by_operator="admin")
                conn.execute("DELETE FROM prompt_runtime_operator_permissions")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_missing_authority")
                OperatorPermissionService(conn).upsert("operator-a", {"permission_class": "runtime_execute", "is_enabled": False}, updated_by_operator="admin")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_permission")
                OperatorPermissionService(conn).upsert("operator-a", {"permission_class": "runtime_view", "is_enabled": True}, updated_by_operator="admin")
                self.assertEqual(self._evaluate(conn, registry).failure_reason_code, "operator_permission_insufficient")
            finally:
                conn.close()

    def test_render_resolver_compatibility_snapshot_blocks_and_no_execution_rows(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                registry = self._setup_all(conn)
                before = self._counts(conn)
                conn.execute("DELETE FROM prompt_runtime_render_validation_ledger")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_invalid_render")
                for status in ("failed", "error", "superseded"):
                    conn.execute("DELETE FROM prompt_runtime_render_validation_ledger")
                    RenderValidationService(conn).record_validation(prompt_record_id=1, prompt_version_id=10, binding_fingerprint="bind", render_result_hash="render", validation_status=status, validation_schema_version="v1", validator_code="unit")
                    self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_invalid_render")
                conn.execute("DELETE FROM prompt_runtime_render_validation_ledger")
                RenderValidationService(conn).record_validation(prompt_record_id=1, prompt_version_id=10, binding_fingerprint="bind", render_result_hash="render", validation_status="passed", validation_schema_version="v1", validator_code="unit")
                conn.execute("DELETE FROM prompt_runtime_target_resolver_registry")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_target_resolution")
                TargetResolverRegistryService(conn).upsert("CREATE_BULK_JSON_DRAFT", "channel", {"resolver_code": "fake_resolver", "snapshot_schema_version": "v1", "is_enabled": False}, updated_by_operator="admin")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_target_resolution")
                TargetResolverRegistryService(conn).upsert("CREATE_BULK_JSON_DRAFT", "channel", {"resolver_code": "missing_impl", "snapshot_schema_version": "v1", "is_enabled": True}, updated_by_operator="admin")
                self.assertEqual(self._evaluate(conn, registry).failure_reason_code, "target_resolver_implementation_missing")
                TargetResolverRegistryService(conn).upsert("CREATE_BULK_JSON_DRAFT", "channel", {"resolver_code": "fake_resolver", "snapshot_schema_version": "v1", "is_enabled": True}, updated_by_operator="admin")
                conn.execute("DELETE FROM prompt_runtime_target_compatibility_policy")
                self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_target_compatibility")
                for status in ("blocked", "deprecated"):
                    TargetCompatibilityService(conn).upsert("CREATE_BULK_JSON_DRAFT", "channel", {"compatibility_status": status, "policy_code": f"p_{status}"}, updated_by_operator="admin")
                    self.assertEqual(self._evaluate(conn, registry).admission_status, "blocked_target_compatibility")
                TargetCompatibilityService(conn).upsert("CREATE_BULK_JSON_DRAFT", "channel", {"compatibility_status": "allowed", "policy_code": "p"}, updated_by_operator="admin")
                bad_registry = TargetSnapshotResolverRegistry()
                bad_registry.register("fake_resolver", lambda **kwargs: snapshot_payload(kwargs["target_ref"], valid=False))
                self.assertEqual(self._evaluate(conn, bad_registry).admission_status, "blocked_target_resolution")
                after = self._counts(conn)
                self.assertEqual({k: after[k] for k in before}, before)
            finally:
                conn.close()

    def _counts(self, conn: sqlite3.Connection) -> dict[str, int]:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("prompt_execution_attempts", "prompt_execution_groups", "prompt_linked_action_dispatch_attempts")
        }


if __name__ == "__main__":
    unittest.main()
