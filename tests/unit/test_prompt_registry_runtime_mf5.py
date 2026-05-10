from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.runtime_adapters import RuntimeAdapterRegistry
from services.prompt_registry.runtime_execution import (
    admit_due_prompt_execution_retries,
    cancel_prompt_execution,
    claim_prompt_execution_async_work,
    confirm_prompt_execution,
    dispatch_prompt_execution,
    get_prompt_execution_status,
    prepare_prompt_execution_preflight,
    reclaim_expired_prompt_execution_leases,
    recover_stale_runtime_executions,
    schedule_prompt_execution_retry,
)
from tests._helpers import seed_minimal_db, temp_env


class TestPromptRegistryRuntimeMf5(unittest.TestCase):
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

    def _pre(self, conn, capability="CREATE_BULK_JSON_DRAFT", action_hash="ah"):
        return prepare_prompt_execution_preflight(conn, capability_code=capability, target_type="workflow", target_id="wf-1", operator_id_or_system_actor="operator-1", prompt_record_id=1, prompt_version_id=1, binding_resolution_fingerprint="bf", rendered_payload_hash="rh", action_payload_hash=action_hash, reviewed_target_state_hash="sh")

    def _admit(self, conn, capability="CREATE_BULK_JSON_DRAFT", action_hash="ah"):
        pre = self._pre(conn, capability=capability, action_hash=action_hash)
        confirm_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], confirmation_token=pre["confirmation_token"], operator_id_or_system_actor="operator-1", reviewed_target_state_hash="sh")
        return pre

    def test_retry_allowed_for_retryable_failed_terminal_creates_next_attempt_and_preserves_lineage(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn)
            conn.execute("UPDATE prompt_execution_attempts SET retryable_by_operator=1 WHERE id=?", (pre["execution_attempt_id"],))
            reg = RuntimeAdapterRegistry(); reg.register("CREATE_BULK_JSON_DRAFT", lambda _: (_ for _ in ()).throw(RuntimeError("boom")))
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=reg)
            retry = schedule_prompt_execution_retry(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1", retry_after="2026-01-01T00:10:00Z")
            self.assertEqual("ADMITTED", retry["state"])
            self.assertEqual(2, retry["attempt_number"])
            old = conn.execute("SELECT binding_resolution_fingerprint,rendered_payload_hash,action_payload_hash,reviewed_target_state_hash,dedup_key_hash FROM prompt_execution_attempts WHERE id=?", (pre["execution_attempt_id"],)).fetchone()
            new = conn.execute("SELECT binding_resolution_fingerprint,rendered_payload_hash,action_payload_hash,reviewed_target_state_hash,dedup_key_hash FROM prompt_execution_attempts WHERE id=?", (retry["execution_attempt_id"],)).fetchone()
            self.assertEqual(tuple(old), tuple(new))
            dup = schedule_prompt_execution_retry(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1")
            self.assertEqual(retry["execution_attempt_id"], dup["execution_attempt_id"])
        finally:
            td.__exit__(None, None, None)

    def test_retry_rejected_for_non_retryable_terminal_states(self):
        for state in ("SUCCEEDED", "CANCELLED", "STALE_BLOCKED", "CONFLICT_BLOCKED"):
            td, conn = self._conn()
            try:
                pre = self._admit(conn)
                conn.execute("UPDATE prompt_execution_attempts SET state=?,retryable_by_operator=1 WHERE id=?", (state, pre["execution_attempt_id"]))
                conn.execute("UPDATE prompt_execution_groups SET current_state=? WHERE id=?", (state, pre["execution_group_id"]))
                conn.commit()
                with self.assertRaises(ValueError):
                    schedule_prompt_execution_retry(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1")
            finally:
                td.__exit__(None, None, None)

    def test_cancel_confirmation_required_and_admitted_and_dispatched(self):
        td, conn = self._conn()
        try:
            pre = self._pre(conn)
            out = cancel_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1")
            self.assertEqual("CANCELLED", out["state"])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM prompt_execution_usage WHERE execution_group_id=?", (pre["execution_group_id"],)).fetchone()[0])
        finally:
            td.__exit__(None, None, None)

        td, conn = self._conn()
        try:
            pre = self._admit(conn)
            out = cancel_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1")
            self.assertEqual("CANCELLED", out["state"])
            self.assertEqual("CANCELLED", conn.execute("SELECT terminal_outcome FROM prompt_execution_usage WHERE execution_group_id=?", (pre["execution_group_id"],)).fetchone()[0])
            dup = cancel_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1")
            self.assertTrue(dup["idempotent"])
        finally:
            td.__exit__(None, None, None)

        td, conn = self._conn()
        try:
            pre = self._admit(conn, capability="ENQUEUE_INTERNAL_PROMPT_JOB")
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=RuntimeAdapterRegistry())
            cancel_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1")
            q = conn.execute("SELECT queue_state FROM prompt_execution_async_queue WHERE execution_attempt_id=?", (pre["execution_attempt_id"],)).fetchone()[0]
            self.assertEqual("FAILED", q)
            self.assertEqual("CANCELLED", conn.execute("SELECT terminal_outcome FROM prompt_execution_usage WHERE execution_group_id=?", (pre["execution_group_id"],)).fetchone()[0])
        finally:
            td.__exit__(None, None, None)

    def test_claim_queued_async_work_sets_claimed_lease_and_running(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn, capability="ENQUEUE_INTERNAL_PROMPT_JOB")
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=RuntimeAdapterRegistry())
            claim = claim_prompt_execution_async_work(conn, lease_owner="worker-1", lease_seconds=30, now="2027-01-01T00:00:00Z")
            self.assertEqual("CLAIMED", claim["queue_state"])
            row = conn.execute("SELECT a.state,g.current_state,q.lease_owner FROM prompt_execution_attempts a JOIN prompt_execution_groups g ON g.id=a.execution_group_id JOIN prompt_execution_async_queue q ON q.execution_attempt_id=a.id WHERE a.id=?", (pre["execution_attempt_id"],)).fetchone()
            self.assertEqual(("RUNNING", "RUNNING", "worker-1"), tuple(row))
        finally:
            td.__exit__(None, None, None)

    def test_claim_ignores_future_queue_items(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn, capability="ENQUEUE_INTERNAL_PROMPT_JOB")
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=RuntimeAdapterRegistry())
            conn.execute("UPDATE prompt_execution_async_queue SET available_at='2027-01-01T01:00:00Z' WHERE execution_attempt_id=?", (pre["execution_attempt_id"],))
            conn.commit()
            self.assertIsNone(claim_prompt_execution_async_work(conn, lease_owner="worker-1", now="2027-01-01T00:00:00Z"))
        finally:
            td.__exit__(None, None, None)

    def test_reclaim_expired_lease_retryable_or_failed_terminal(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn, capability="ENQUEUE_INTERNAL_PROMPT_JOB")
            conn.execute("UPDATE prompt_execution_attempts SET retryable_by_operator=1 WHERE id=?", (pre["execution_attempt_id"],))
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=RuntimeAdapterRegistry())
            claim_prompt_execution_async_work(conn, lease_owner="worker-1", lease_seconds=1, now="2027-01-01T00:00:00Z")
            out = reclaim_expired_prompt_execution_leases(conn, now="2027-01-01T00:00:02Z")
            self.assertEqual("RETRY_PENDING", out[0]["state"])
            self.assertEqual("QUEUED", conn.execute("SELECT queue_state FROM prompt_execution_async_queue WHERE execution_attempt_id=?", (pre["execution_attempt_id"],)).fetchone()[0])
        finally:
            td.__exit__(None, None, None)

        td, conn = self._conn()
        try:
            pre = self._admit(conn, capability="ENQUEUE_INTERNAL_PROMPT_JOB")
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=RuntimeAdapterRegistry())
            claim_prompt_execution_async_work(conn, lease_owner="worker-1", lease_seconds=1, now="2027-01-01T00:00:00Z")
            out = reclaim_expired_prompt_execution_leases(conn, now="2027-01-01T00:00:02Z")
            self.assertEqual("FAILED_TERMINAL", out[0]["state"])
            self.assertEqual("FAILED_TERMINAL", get_prompt_execution_status(conn, execution_attempt_id=pre["execution_attempt_id"])["current_state"])
        finally:
            td.__exit__(None, None, None)

    def test_recovery_does_not_touch_terminal_executions_and_writes_recovery_events(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn)
            reg = RuntimeAdapterRegistry(); reg.register("CREATE_BULK_JSON_DRAFT", lambda _: {"result_code": "OK", "secret_safe_message": "done"})
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=reg)
            recovered = recover_stale_runtime_executions(conn, now="2027-01-01T00:00:00Z")
            self.assertEqual([], recovered)
            self.assertEqual("SUCCEEDED", get_prompt_execution_status(conn, execution_attempt_id=pre["execution_attempt_id"])["current_state"])
        finally:
            td.__exit__(None, None, None)


    def test_future_retry_after_creates_retry_pending_and_dispatch_rejects_until_due(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn)
            conn.execute("UPDATE prompt_execution_attempts SET retryable_by_operator=1 WHERE id=?", (pre["execution_attempt_id"],))
            reg = RuntimeAdapterRegistry(); reg.register("CREATE_BULK_JSON_DRAFT", lambda _: (_ for _ in ()).throw(RuntimeError("boom")))
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=reg)
            retry = schedule_prompt_execution_retry(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1", retry_after="2999-01-01T00:00:00Z")
            self.assertEqual("RETRY_PENDING", retry["state"])
            row = conn.execute("SELECT state,lease_expires_at FROM prompt_execution_attempts WHERE id=?", (retry["execution_attempt_id"],)).fetchone()
            self.assertEqual(("RETRY_PENDING", "2999-01-01T00:00:00Z"), tuple(row))
            with self.assertRaises(ValueError):
                dispatch_prompt_execution(conn, execution_attempt_id=retry["execution_attempt_id"], adapter_registry=reg)
            admitted = admit_due_prompt_execution_retries(conn, now="2999-01-01T00:00:00Z", actor="system")
            self.assertEqual(retry["execution_attempt_id"], admitted[0]["execution_attempt_id"])
            self.assertEqual("ADMITTED", conn.execute("SELECT state FROM prompt_execution_attempts WHERE id=?", (retry["execution_attempt_id"],)).fetchone()[0])
        finally:
            td.__exit__(None, None, None)

    def test_due_retry_after_creates_admitted(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn)
            conn.execute("UPDATE prompt_execution_attempts SET state='FAILED_TERMINAL',retryable_by_operator=1 WHERE id=?", (pre["execution_attempt_id"],))
            conn.execute("UPDATE prompt_execution_groups SET current_state='FAILED_TERMINAL' WHERE id=?", (pre["execution_group_id"],))
            conn.commit()
            retry = schedule_prompt_execution_retry(conn, execution_attempt_id=pre["execution_attempt_id"], actor="operator-1", retry_after="2000-01-01T00:00:00Z")
            self.assertEqual("ADMITTED", retry["state"])
        finally:
            td.__exit__(None, None, None)

    def test_reclaim_retry_limit_prevents_infinite_retry_loop(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn, capability="ENQUEUE_INTERNAL_PROMPT_JOB")
            conn.execute("UPDATE prompt_execution_attempts SET retryable_by_operator=1 WHERE id=?", (pre["execution_attempt_id"],))
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=RuntimeAdapterRegistry())
            claim_prompt_execution_async_work(conn, lease_owner="worker-1", lease_seconds=1, now="2027-01-01T00:00:00Z")
            first = reclaim_expired_prompt_execution_leases(conn, now="2027-01-01T00:00:02Z", max_retries=1)
            self.assertEqual("RETRY_PENDING", first[0]["state"])
            claim_prompt_execution_async_work(conn, lease_owner="worker-2", lease_seconds=1, now="2027-01-01T00:00:03Z")
            second = reclaim_expired_prompt_execution_leases(conn, now="2027-01-01T00:00:05Z", max_retries=1)
            self.assertEqual("FAILED_TERMINAL", second[0]["state"])
            self.assertEqual("RETRY_LIMIT_REACHED", second[0]["result_code"])
            self.assertEqual("FAILED_TERMINAL", conn.execute("SELECT terminal_outcome FROM prompt_execution_usage WHERE execution_group_id=?", (pre["execution_group_id"],)).fetchone()[0])
        finally:
            td.__exit__(None, None, None)

    def test_recovery_requeues_missing_queue_coverage_and_audits(self):
        td, conn = self._conn()
        try:
            pre = self._admit(conn, capability="ENQUEUE_INTERNAL_PROMPT_JOB")
            dispatch_prompt_execution(conn, execution_attempt_id=pre["execution_attempt_id"], adapter_registry=RuntimeAdapterRegistry())
            conn.execute("DELETE FROM prompt_execution_async_queue WHERE execution_attempt_id=?", (pre["execution_attempt_id"],))
            conn.commit()
            recovered = recover_stale_runtime_executions(conn, now="2027-01-01T00:00:00Z")
            self.assertEqual(pre["execution_attempt_id"], recovered[0]["execution_attempt_id"])
            self.assertEqual("QUEUED", conn.execute("SELECT queue_state FROM prompt_execution_async_queue WHERE execution_attempt_id=?", (pre["execution_attempt_id"],)).fetchone()[0])
            lifecycle = conn.execute("SELECT COUNT(*) FROM prompt_execution_lifecycle_events WHERE execution_group_id=? AND result_code='RECOVERY_REQUEUED'", (pre["execution_group_id"],)).fetchone()[0]
            audit = conn.execute("SELECT COUNT(*) FROM prompt_audit_events WHERE event_type='runtime_recovery_requeued'").fetchone()[0]
            self.assertEqual(1, lifecycle)
            self.assertEqual(1, audit)
        finally:
            td.__exit__(None, None, None)

if __name__ == "__main__":
    unittest.main()
