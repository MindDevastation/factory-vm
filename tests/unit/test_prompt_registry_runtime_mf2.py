from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import RenderValidationService
from services.prompt_registry.runtime_execution import compute_dedup_key_hash, confirm_prompt_execution, prepare_prompt_execution_preflight
from tests._helpers import seed_minimal_db, temp_env
from tests._runtime_authority import register_runtime_resolver, seed_runtime_authorities


class TestPromptRegistryRuntimeMf2(unittest.TestCase):
    def setUp(self) -> None:
        self.base = dict(
            capability_code="CREATE_BULK_JSON_DRAFT",
            target_type="workflow",
            target_id="wf-1",
            operator_id_or_system_actor="operator-1",
            prompt_record_id=1,
            prompt_version_id=1,
            binding_resolution_fingerprint="bind-1",
            rendered_payload_hash="render-1",
            action_payload_hash="action-1",
            reviewed_target_state_hash="state-1",
        )

    def _conn(self):
        td = temp_env()
        _td, env = td.__enter__()
        seed_minimal_db(env)
        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("INSERT INTO prompt_records(id,slug,code,title,record_type,status,validation_status,bridge_policy_hook,active_version_id,created_at,updated_at) VALUES(1,'p','p','p','prompt_template','active','VALID',NULL,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
        conn.execute("INSERT INTO prompt_versions(id,prompt_id,version_no,body_text,render_fingerprint,status,validation_status,is_active,created_at,updated_at) VALUES(1,1,1,'body','fp','active','VALID',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
        seed_runtime_authorities(conn, operator="operator-1", target_type="workflow", binding_fingerprint="bind-1", render_hash="render-1")
        conn.commit()
        return td, conn

    def test_allowed_capability_reaches_confirmation_required(self):
        td, conn = self._conn()
        try:
            out = prepare_prompt_execution_preflight(conn, **self.base)
            self.assertEqual("CONFIRMATION_REQUIRED", out["state"])
        finally:
            td.__exit__(None, None, None)

    def test_forbidden_capability_rejected(self):
        td, conn = self._conn()
        try:
            with self.assertRaises(ValueError):
                prepare_prompt_execution_preflight(conn, **{**self.base, "capability_code": "NOPE"})
        finally:
            td.__exit__(None, None, None)

    def test_confirm_flow_and_idempotency_and_stale(self):
        td, conn = self._conn()
        try:
            pre = prepare_prompt_execution_preflight(conn, **self.base)
            with self.assertRaises(ValueError):
                confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=None, operator_id_or_system_actor="operator-1", reviewed_target_state_hash=pre["reviewed_target_state_hash"])
            adm = confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash=pre["reviewed_target_state_hash"])
            self.assertEqual("ADMITTED", adm["state"])
            adm2 = confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash=pre["reviewed_target_state_hash"])
            self.assertEqual(adm["execution_group_id"], adm2["execution_group_id"])
            stale = confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash="state-CHANGED")
            self.assertEqual("STALE_BLOCKED", stale["state"])
        finally:
            td.__exit__(None, None, None)

    def test_conflict_and_dedup(self):
        td, conn = self._conn()
        try:
            pre = prepare_prompt_execution_preflight(conn, **self.base)
            dup = prepare_prompt_execution_preflight(conn, **self.base)
            self.assertEqual(pre["execution_attempt_id"], dup["execution_attempt_id"])
            conflict = prepare_prompt_execution_preflight(conn, **{**self.base, "action_payload_hash": "action-2"})
            self.assertEqual("CONFLICT_BLOCKED", conflict["state"])
        finally:
            td.__exit__(None, None, None)

    def test_confirm_blocks_when_authoritative_snapshot_changes_after_preflight(self):
        td, conn = self._conn()
        try:
            pre = prepare_prompt_execution_preflight(conn, **self.base)
            register_runtime_resolver(state="changed-after-preflight")
            RenderValidationService(conn).record_validation(
                prompt_record_id=1,
                prompt_version_id=1,
                binding_fingerprint="bind-1",
                render_result_hash="render-1",
                validation_status="passed",
                validation_schema_version="v1",
                validator_code="mf2-stale",
            )
            stale = confirm_prompt_execution(
                conn,
                execution_attempt_id=pre["execution_attempt_id"],
                confirmation_token=pre["confirmation_token"],
                operator_id_or_system_actor="operator-1",
                reviewed_target_state_hash=pre["reviewed_target_state_hash"],
            )
            self.assertEqual("STALE_BLOCKED", stale["state"])
            self.assertEqual("STALE_TARGET_SNAPSHOT", stale["result_code"])
            self.assertEqual("stale_authoritative_target_snapshot", stale["failure_reason_code"])
        finally:
            td.__exit__(None, None, None)

    def test_dedup_hash_stability_and_change(self):
        common = {k: v for k, v in self.base.items() if k != "operator_id_or_system_actor"}
        d1 = compute_dedup_key_hash(**common)
        d2 = compute_dedup_key_hash(**common)
        d3 = compute_dedup_key_hash(**{**common, "reviewed_target_state_hash": "state-2"})
        self.assertEqual(d1, d2)
        self.assertNotEqual(d1, d3)

    def test_usage_created_at_admission_before_execution(self):
        td, conn = self._conn()
        try:
            pre = prepare_prompt_execution_preflight(conn, **self.base)
            confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash=pre["reviewed_target_state_hash"])
            usage = conn.execute("SELECT COUNT(*) FROM prompt_execution_usage").fetchone()[0]
            self.assertEqual(1, usage)
        finally:
            td.__exit__(None, None, None)

    def test_db_active_lock_rejects_second_active_group(self):
        td, conn = self._conn()
        try:
            conn.execute("INSERT INTO prompt_execution_groups(capability_code,target_type,target_id,dedup_lineage_key,current_state,execution_mode,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", ("CREATE_BULK_JSON_DRAFT", "workflow", "wf-1", "k1", "CONFIRMATION_REQUIRED", "SYNC", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"))
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("INSERT INTO prompt_execution_groups(capability_code,target_type,target_id,dedup_lineage_key,current_state,execution_mode,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", ("CREATE_BULK_JSON_DRAFT", "workflow", "wf-1", "k2", "PREPARED", "SYNC", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"))
        finally:
            td.__exit__(None, None, None)

    def test_db_active_lock_allows_terminal_then_new_active(self):
        td, conn = self._conn()
        try:
            conn.execute("INSERT INTO prompt_execution_groups(capability_code,target_type,target_id,dedup_lineage_key,current_state,execution_mode,created_at,updated_at,closed_at) VALUES(?,?,?,?,?,?,?,?,?)", ("CREATE_BULK_JSON_DRAFT", "workflow", "wf-1", "k1", "SUCCEEDED", "SYNC", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2026-01-01T00:00:10Z"))
            conn.execute("INSERT INTO prompt_execution_groups(capability_code,target_type,target_id,dedup_lineage_key,current_state,execution_mode,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", ("CREATE_BULK_JSON_DRAFT", "workflow", "wf-1", "k2", "PREPARED", "SYNC", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"))
        finally:
            td.__exit__(None, None, None)

    def test_no_duplicate_active_groups_for_same_capability_target(self):
        td, conn = self._conn()
        try:
            prepare_prompt_execution_preflight(conn, **self.base)
            cnt = conn.execute("SELECT COUNT(*) FROM prompt_execution_groups WHERE capability_code=? AND target_type=? AND target_id=? AND current_state IN ('PREPARED','CONFIRMATION_REQUIRED','ADMITTED','DISPATCHED','RUNNING','RETRY_PENDING')", ("CREATE_BULK_JSON_DRAFT", "workflow", "wf-1")).fetchone()[0]
            self.assertEqual(1, cnt)
        finally:
            td.__exit__(None, None, None)

    def test_preflight_ignores_caller_server_gate_context_and_uses_authoritative_snapshot(self):
        td, conn = self._conn()
        try:
            out = prepare_prompt_execution_preflight(
                conn,
                **{
                    **self.base,
                    "reviewed_target_state_hash": "caller-preview-hash",
                    "_server_gate_context": {
                        "capability_execution_enabled": False,
                        "operator_allowed_capabilities": [],
                        "operator_allowed_permission_classes": [],
                        "binding_resolution_complete": False,
                        "binding_resolution_ambiguous": True,
                        "rendered_payload_valid": False,
                        "target_exists": False,
                        "target_state_compatible": False,
                        "current_target_state_hash": "caller-stale-hash",
                    },
                },
            )
            self.assertEqual("CONFIRMATION_REQUIRED", out["state"])
            self.assertNotEqual("caller-preview-hash", out["reviewed_target_state_hash"])
            stored = conn.execute("SELECT reviewed_target_state_hash FROM prompt_execution_attempts WHERE id=?", (out["execution_attempt_id"],)).fetchone()[0]
            self.assertEqual(out["reviewed_target_state_hash"], stored)
        finally:
            td.__exit__(None, None, None)

    def test_preflight_blocks_from_persisted_authority_sources_not_caller_context(self):
        cases = [
            ("missing capability authority", "DELETE FROM prompt_runtime_capability_registry", "PREFLIGHT_REJECTED", "missing_capability_authority"),
            ("disabled capability", "UPDATE prompt_runtime_capability_registry SET execution_enabled=0", "PREFLIGHT_REJECTED", "capability_execution_disabled"),
            ("deprecated capability", "UPDATE prompt_runtime_capability_registry SET status='deprecated'", "PREFLIGHT_REJECTED", "capability_status_deprecated"),
            ("insufficient operator permission", "UPDATE prompt_runtime_operator_permissions SET permission_class='runtime_view'", "PREFLIGHT_REJECTED", "operator_permission_insufficient"),
            ("invalid render validation", "UPDATE prompt_runtime_render_validation_ledger SET validation_status='failed'", "PREFLIGHT_REJECTED", "render_validation_failed"),
            ("missing resolver authority", "DELETE FROM prompt_runtime_target_resolver_registry", "PREFLIGHT_REJECTED", "missing_target_resolver_authority"),
            ("missing compatibility authority", "DELETE FROM prompt_runtime_target_compatibility_policy", "CONFLICT_BLOCKED", "missing_target_compatibility_authority"),
            ("blocked compatibility", "UPDATE prompt_runtime_target_compatibility_policy SET compatibility_status='blocked'", "CONFLICT_BLOCKED", "target_compatibility_blocked"),
        ]
        for label, mutation, expected_state, expected_reason in cases:
            td, conn = self._conn()
            try:
                with self.subTest(label=label):
                    conn.execute(mutation)
                    conn.commit()
                    out = prepare_prompt_execution_preflight(
                        conn,
                        **{
                            **self.base,
                            "_server_gate_context": {
                                "capability_execution_enabled": True,
                                "operator_allowed_capabilities": ["CREATE_BULK_JSON_DRAFT"],
                                "operator_allowed_permission_classes": ["runtime_execute"],
                                "binding_resolution_complete": True,
                                "rendered_payload_valid": True,
                                "target_exists": True,
                                "target_state_compatible": True,
                            },
                        },
                    )
                    self.assertEqual(expected_state, out["state"])
                    self.assertEqual(expected_reason, out["failure_reason_code"])
                    self.assertNotIn("confirmation_token", out)
                    self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM prompt_execution_attempts").fetchone()[0])
            finally:
                td.__exit__(None, None, None)

    def test_preflight_rejects_secret_unsafe_payload_before_authority(self):
        td, conn = self._conn()
        try:
            with self.assertRaisesRegex(ValueError, "secret-unsafe"):
                prepare_prompt_execution_preflight(conn, **{**self.base, "adapter_precheck_payload": {"api_token": "secret-token"}})
        finally:
            td.__exit__(None, None, None)


