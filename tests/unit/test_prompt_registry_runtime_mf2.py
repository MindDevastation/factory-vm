from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.runtime_execution import compute_dedup_key_hash, confirm_prompt_execution, prepare_prompt_execution_preflight
from tests._helpers import seed_minimal_db, temp_env


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
                confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=None, operator_id_or_system_actor="operator-1", reviewed_target_state_hash="state-1")
            adm = confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash="state-1")
            self.assertEqual("ADMITTED", adm["state"])
            adm2 = confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash="state-1")
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

    def test_dedup_hash_stability_and_change(self):
        common = {k: v for k, v in self.base.items() if k != "operator_id_or_system_actor"}
        d1 = compute_dedup_key_hash(**common)
        d2 = compute_dedup_key_hash(**common)
        d3 = compute_dedup_key_hash(**{**common, "reviewed_target_state_hash": "state-2"})
        self.assertEqual(d1, d2)
        self.assertNotEqual(d1, d3)

    def test_no_usage_terminal_before_execution(self):
        td, conn = self._conn()
        try:
            pre = prepare_prompt_execution_preflight(conn, **self.base)
            confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash="state-1")
            usage = conn.execute("SELECT COUNT(*) FROM prompt_execution_usage").fetchone()[0]
            self.assertEqual(0, usage)
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

if __name__ == "__main__":
    unittest.main()
