from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import (
    CapabilityGateService,
    OperatorPermissionService,
    PromptRuntimeAuthorityInspectionService,
    RenderValidationService,
    TargetCompatibilityService,
    TargetResolverRegistryService,
    TargetSnapshotResolverRegistry,
    TargetSnapshotService,
)
from tests._helpers import seed_minimal_db, temp_env


def payload(target_ref="target-1"):
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


class TestPromptRuntimeAuthoritativeGateMf6(unittest.TestCase):
    def _seed(self, conn: sqlite3.Connection) -> None:
        CapabilityGateService(conn).upsert("CAP_A", {"execution_enabled": True, "required_permission_class": "runtime_execute", "status": "active", "notes": "secret note"}, updated_by_operator="admin")
        CapabilityGateService(conn).upsert("CAP_B", {"execution_enabled": False, "required_permission_class": "runtime_admin", "status": "disabled"}, updated_by_operator="admin")
        OperatorPermissionService(conn).upsert("admin", {"permission_class": "runtime_execute", "is_enabled": True}, updated_by_operator="admin")
        OperatorPermissionService(conn).upsert("disabled", {"permission_class": "runtime_view", "is_enabled": False}, updated_by_operator="admin")
        RenderValidationService(conn).record_validation(prompt_record_id=1, prompt_version_id=10, binding_fingerprint="bind", render_result_hash="render", validation_status="passed", validation_schema_version="v1", validator_code="unit")
        RenderValidationService(conn).record_validation(prompt_record_id=1, prompt_version_id=11, binding_fingerprint="bind", render_result_hash="render", validation_status="failed", validation_schema_version="v1", validator_code="unit", invalid_reason_detail="password bad")
        TargetResolverRegistryService(conn).upsert("CAP_A", "channel", {"resolver_code": "fake_resolver", "snapshot_schema_version": "v1", "is_enabled": True}, updated_by_operator="admin")
        TargetCompatibilityService(conn).upsert("CAP_A", "channel", {"compatibility_status": "allowed", "policy_code": "p", "notes": "api_key note"}, updated_by_operator="admin")
        registry = TargetSnapshotResolverRegistry()
        registry.register("fake_resolver", lambda **kwargs: payload(kwargs["target_ref"]))
        TargetSnapshotService(conn, registry).resolve_preview(capability_code="CAP_A", target_type="channel", target_ref="target-1")

    def test_summary_counts_report_filters_limit_redaction_and_no_writes(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                self._seed(conn)
                svc = PromptRuntimeAuthorityInspectionService(conn)
                before = self._counts(conn)
                summary = svc.summary()
                self.assertEqual(summary["capabilities"]["total"], 2)
                self.assertEqual(summary["capabilities"]["by"]["status"]["active"], 1)
                self.assertEqual(summary["operator_permissions"]["by"]["is_enabled"]["1"], 1)
                self.assertEqual(summary["render_validations"]["by"]["validation_status"]["failed"], 1)
                self.assertEqual(summary["resolvers"]["by"]["is_enabled"]["1"], 1)
                self.assertEqual(summary["compatibility"]["by"]["compatibility_status"]["allowed"], 1)
                self.assertEqual(summary["snapshots"]["total"], 1)

                report = svc.report(capability_code="CAP_A", target_type="channel", operator_subject="admin", limit=999)
                self.assertEqual(report["limit"], 500)
                self.assertEqual(len(report["capabilities"]), 1)
                self.assertEqual(report["capabilities"][0]["capability_code"], "CAP_A")
                self.assertEqual(report["operator_permissions"][0]["operator_subject"], "admin")
                self.assertEqual(report["compatibility"][0]["notes"], "[redacted]")
                self.assertEqual(report["snapshots"][0]["snapshot_payload"]["resolver_metadata"]["secret_token"], "[redacted]")
                failed_report = svc.report(limit=100)
                failed_rows = [row for row in failed_report["render_validations"] if row["validation_status"] == "failed"]
                self.assertEqual(failed_rows[0]["invalid_reason_detail"], "[redacted]")
                self.assertEqual(self._counts(conn), before)
            finally:
                conn.close()

    def _counts(self, conn: sqlite3.Connection) -> dict[str, int]:
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


if __name__ == "__main__":
    unittest.main()
