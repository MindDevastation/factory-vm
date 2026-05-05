from __future__ import annotations

import sqlite3
import unittest

from tests._helpers import seed_minimal_db, temp_env
from services.prompt_registry.runtime_states import is_allowed_runtime_transition, is_runtime_state, is_terminal_runtime_state


class TestPromptRegistryRuntimeMf1(unittest.TestCase):
    def test_runtime_tables_columns_and_indexes_smoke(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row

            tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            for name in (
                "prompt_execution_groups",
                "prompt_execution_attempts",
                "prompt_execution_lifecycle_events",
                "prompt_execution_usage",
                "prompt_execution_async_queue",
            ):
                self.assertIn(name, tables)

            cols = lambda t: {r["name"] for r in conn.execute(f"PRAGMA table_info({t})").fetchall()}
            self.assertTrue({"id","capability_code","target_type","target_id","dedup_lineage_key","current_state","created_at","updated_at","closed_at"}.issubset(cols("prompt_execution_groups")))
            self.assertTrue({"id","execution_group_id","attempt_number","state","correlation_id","operator_id_or_system_actor","prompt_record_id","prompt_version_id","binding_resolution_fingerprint","rendered_payload_hash","action_payload_hash","reviewed_target_state_hash","dedup_key_hash","retryable_by_operator","cancellable","admitted_at","running_at","terminal_at","result_code","secret_safe_message","lease_expires_at"}.issubset(cols("prompt_execution_attempts")))
            self.assertTrue({"id","execution_group_id","execution_attempt_id","state_before","state_after","result_code","actor","timestamp"}.issubset(cols("prompt_execution_lifecycle_events")))
            self.assertTrue({"execution_group_id","first_admitted_attempt_id","latest_attempt_id","prompt_record_id","prompt_version_id","rendered_payload_hash","binding_resolution_fingerprint","capability_code","target_type","target_id","operator_id","first_admitted_at","terminal_outcome","terminal_at","artifact_ref"}.issubset(cols("prompt_execution_usage")))

            indexes = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name LIKE 'prompt_execution_%'").fetchall()}
            for name in (
                "idx_prompt_execution_groups_dedup",
                "idx_prompt_execution_groups_active",
                "idx_prompt_execution_groups_active_target_lock",
                "idx_prompt_execution_attempts_retryable_async",
                "idx_prompt_execution_attempts_lease_reclaim",
                "idx_prompt_execution_lifecycle_events_timeline",
                "idx_prompt_execution_async_queue_lookup",
                "idx_prompt_execution_async_queue_lease_reclaim",
            ):
                self.assertIn(name, indexes)

    def test_check_constraints_reject_invalid_state_and_json(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO prompt_execution_groups(capability_code,target_type,target_id,dedup_lineage_key,current_state,execution_mode,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    ("CREATE_BULK_JSON_DRAFT", "workflow", "x", "k1", "BAD_STATE", "SYNC", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                )
            conn.execute(
                "INSERT INTO prompt_execution_groups(capability_code,target_type,target_id,dedup_lineage_key,current_state,execution_mode,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                ("CREATE_BULK_JSON_DRAFT", "workflow", "x", "k2", "PREPARED", "ASYNC", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            )
            gid = int(conn.execute("SELECT id FROM prompt_execution_groups WHERE dedup_lineage_key='k2'").fetchone()[0])
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO prompt_execution_attempts(execution_group_id,attempt_number,state,correlation_id,operator_id_or_system_actor,prompt_record_id,prompt_version_id,binding_resolution_fingerprint,rendered_payload_hash,action_payload_hash,reviewed_target_state_hash,dedup_key_hash,retryable_by_operator,cancellable,execution_mode,dispatch_payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (gid,1,"PREPARED","c1","op",1,None,"bf","rh","ah","th","dh",0,0,"SYNC","{","2026-01-01T00:00:00Z","2026-01-01T00:00:00Z"),
                )

    def test_runtime_state_helpers(self) -> None:
        self.assertTrue(is_runtime_state("PREPARED"))
        self.assertFalse(is_runtime_state("UNKNOWN"))
        self.assertTrue(is_terminal_runtime_state("FAILED_TERMINAL"))
        self.assertFalse(is_terminal_runtime_state("RUNNING"))
        with self.assertRaises(ValueError):
            is_terminal_runtime_state("oops")
        self.assertTrue(is_allowed_runtime_transition("PREPARED", "CONFIRMATION_REQUIRED"))
        self.assertFalse(is_allowed_runtime_transition("PREPARED", "RUNNING"))


if __name__ == "__main__":
    unittest.main()
