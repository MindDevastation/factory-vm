from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import RenderValidationService, TARGET_SNAPSHOT_RESOLVER_REGISTRY
from services.prompt_registry.runtime_execution import confirm_prompt_execution, prepare_prompt_execution_preflight
from tests._helpers import seed_minimal_db, temp_env
from tests._runtime_authority import register_runtime_resolver, seed_runtime_authorities


class TestPromptRegistryRuntimeAuthoritativeIntegration(unittest.TestCase):
    def setUp(self) -> None:
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()

    def tearDown(self) -> None:
        TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()

    def _conn(self):
        td = temp_env(); _td, env = td.__enter__(); seed_minimal_db(env)
        conn = sqlite3.connect(env.db_path); conn.row_factory = sqlite3.Row
        conn.execute("INSERT INTO prompt_records(id,slug,code,title,record_type,status,validation_status,bridge_policy_hook,active_version_id,created_at,updated_at) VALUES(1,'p','p','p','prompt_template','active','VALID',NULL,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
        conn.execute("INSERT INTO prompt_versions(id,prompt_id,version_no,body_text,render_fingerprint,status,validation_status,is_active,created_at,updated_at) VALUES(1,1,1,'body','fp','active','VALID',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
        conn.commit(); return td, conn

    def _base(self):
        return dict(capability_code="CREATE_BULK_JSON_DRAFT", target_type="workflow", target_id="wf-1", operator_id_or_system_actor="operator-1", prompt_record_id=1, prompt_version_id=1, binding_resolution_fingerprint="bf", rendered_payload_hash="rh", action_payload_hash="ah", reviewed_target_state_hash="caller-preview")

    def _seed(self, conn, **kw):
        seed_runtime_authorities(conn, operator="operator-1", capability="CREATE_BULK_JSON_DRAFT", target_type="workflow", binding_fingerprint="bf", render_hash="rh", **kw)

    def test_preflight_blocks_missing_and_disabled_authorities_without_attempts(self):
        cases = [
            (None, "missing_capability_authority"),
            ({"capability_enabled": False}, "capability_execution_disabled"),
            ({"capability_status": "deprecated"}, "capability_status_deprecated"),
            ({"operator_enabled": False}, "operator_permission_disabled"),
            ({"permission_class": "runtime_view"}, "operator_permission_insufficient"),
            ({"render_status": "failed"}, "render_validation_failed"),
            ({"resolver_enabled": False}, "target_resolver_disabled"),
            ({"compatibility_status": "blocked"}, "target_compatibility_blocked"),
            ({"register_resolver": False}, "target_resolver_implementation_missing"),
        ]
        for seed_kwargs, reason in cases:
            TARGET_SNAPSHOT_RESOLVER_REGISTRY.clear()
            td, conn = self._conn()
            try:
                if seed_kwargs is not None:
                    self._seed(conn, **seed_kwargs)
                out = prepare_prompt_execution_preflight(conn, **self._base())
                expected_state = "CONFLICT_BLOCKED" if reason.startswith("target_compatibility_") else "PREFLIGHT_REJECTED"
                self.assertEqual(expected_state, out["state"])
                self.assertEqual(reason, out["failure_reason_code"])
                self.assertNotIn("confirmation_token", out)
                self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM prompt_execution_attempts").fetchone()[0])
                self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM prompt_execution_groups").fetchone()[0])
            finally:
                td.__exit__(None, None, None)

    def test_admissible_preflight_uses_authoritative_snapshot_hash_and_confirm_rechecks(self):
        td, conn = self._conn()
        try:
            self._seed(conn)
            pre = prepare_prompt_execution_preflight(conn, **self._base())
            self.assertEqual("CONFIRMATION_REQUIRED", pre["state"])
            self.assertNotEqual("caller-preview", pre["reviewed_target_state_hash"])
            conn.execute("UPDATE prompt_runtime_operator_permissions SET is_enabled=0 WHERE operator_subject='operator-1'")
            conn.commit()
            blocked = confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash=pre["reviewed_target_state_hash"])
            self.assertEqual("CONFLICT_BLOCKED", blocked["state"])
            self.assertEqual("operator_permission_disabled", blocked["failure_reason_code"])
        finally:
            td.__exit__(None, None, None)

    def test_confirm_blocks_stale_when_authoritative_snapshot_hash_changes(self):
        td, conn = self._conn()
        try:
            self._seed(conn)
            pre = prepare_prompt_execution_preflight(conn, **self._base())
            register_runtime_resolver(state="changed")
            RenderValidationService(conn).record_validation(prompt_record_id=1, prompt_version_id=1, binding_fingerprint="bf", render_result_hash="rh", validation_status="passed", validation_schema_version="v1", validator_code="test2")
            stale = confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash=pre["reviewed_target_state_hash"])
            self.assertEqual("STALE_BLOCKED", stale["state"])
            self.assertEqual("STALE_TARGET_SNAPSHOT", stale["result_code"])
        finally:
            td.__exit__(None, None, None)


if __name__ == "__main__":
    unittest.main()
