from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.runtime_adapters import RuntimeAdapterRegistry
from services.prompt_registry.runtime_execution import (
    confirm_prompt_execution,
    dispatch_prompt_execution,
    get_prompt_execution_status,
    list_prompt_execution_timeline,
    prepare_prompt_execution_preflight,
)
from tests._helpers import seed_minimal_db, temp_env


class TestPromptRegistryRuntimeMf4(unittest.TestCase):
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

    def _pre(self, conn, capability='CREATE_BULK_JSON_DRAFT', action_hash='ah'):
        return prepare_prompt_execution_preflight(conn, capability_code=capability, target_type='workflow', target_id='wf-1', operator_id_or_system_actor='operator-1', prompt_record_id=1, prompt_version_id=1, binding_resolution_fingerprint='bf', rendered_payload_hash='rh', action_payload_hash=action_hash, reviewed_target_state_hash='sh')

    def test_lifecycle_and_usage_on_confirm_and_sync_success(self):
        td, conn = self._conn()
        try:
            pre = self._pre(conn)
            confirm_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], confirmation_token=pre['confirmation_token'], operator_id_or_system_actor='operator-1', reviewed_target_state_hash='sh')
            usage = conn.execute("SELECT first_admitted_attempt_id,terminal_outcome FROM prompt_execution_usage WHERE execution_group_id=?", (pre['execution_group_id'],)).fetchone()
            self.assertEqual(pre['execution_attempt_id'], usage[0])
            reg = RuntimeAdapterRegistry(); reg.register('CREATE_BULK_JSON_DRAFT', lambda _: {'result_code': 'OK', 'secret_safe_message': 'done'})
            out = dispatch_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], adapter_registry=reg, payload={'safe': 'ok'})
            self.assertEqual('SUCCEEDED', out['state'])
            usage2 = conn.execute("SELECT terminal_outcome FROM prompt_execution_usage WHERE execution_group_id=?", (pre['execution_group_id'],)).fetchone()
            self.assertEqual('SUCCEEDED', usage2[0])
            ev = list_prompt_execution_timeline(conn, execution_group_id=pre['execution_group_id'])
            states = [e['state_after'] for e in ev]
            self.assertIn('CONFIRMATION_REQUIRED', states)
            self.assertIn('ADMITTED', states)
            self.assertIn('RUNNING', states)
            self.assertIn('SUCCEEDED', states)
        finally:
            td.__exit__(None, None, None)

    def test_sync_failed_terminal_writes_lifecycle_and_audit_secret_safe(self):
        td, conn = self._conn()
        try:
            pre = self._pre(conn)
            confirm_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], confirmation_token=pre['confirmation_token'], operator_id_or_system_actor='operator-1', reviewed_target_state_hash='sh')
            reg = RuntimeAdapterRegistry(); reg.register('CREATE_BULK_JSON_DRAFT', lambda _: {'result_code': 'token-leak', 'secret_safe_message': 'secret=bad'})
            out = dispatch_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], adapter_registry=reg, payload={'safe': 'ok'})
            self.assertEqual('FAILED_TERMINAL', out['state'])
            aud = conn.execute("SELECT event_type,payload_json FROM prompt_audit_events WHERE prompt_id=1 ORDER BY id ASC").fetchall()
            self.assertTrue(any(r[0] == 'runtime_failed_terminal' for r in aud))
            self.assertFalse(any('secret=bad' in r[1] for r in aud))
        finally:
            td.__exit__(None, None, None)

    def test_no_usage_for_pre_admission_conflict_and_status_helpers(self):
        td, conn = self._conn()
        try:
            self._pre(conn)
            blocked = self._pre(conn, action_hash='ah-2')
            self.assertEqual('CONFLICT_BLOCKED', blocked['state'])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM prompt_execution_usage").fetchone()[0])
            pre2 = self._pre(conn, capability='ENQUEUE_INTERNAL_PROMPT_JOB')
            confirm_prompt_execution(conn, execution_attempt_id=pre2['execution_attempt_id'], confirmation_token=pre2['confirmation_token'], operator_id_or_system_actor='operator-1', reviewed_target_state_hash='sh')
            dispatch_prompt_execution(conn, execution_attempt_id=pre2['execution_attempt_id'], adapter_registry=RuntimeAdapterRegistry())
            status = get_prompt_execution_status(conn, execution_attempt_id=pre2['execution_attempt_id'])
            self.assertEqual('DISPATCHED', status['current_state'])
            self.assertTrue(len(status['lifecycle_events']) >= 3)
        finally:
            td.__exit__(None, None, None)


    def test_preflight_confirmation_required_streams_persist_after_reopen(self):
        td = temp_env()
        _td, env = td.__enter__()
        try:
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("INSERT INTO prompt_records(id,slug,code,title,record_type,status,validation_status,bridge_policy_hook,active_version_id,created_at,updated_at) VALUES(1,'p','p','p','prompt_template','active','VALID',NULL,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
            conn.execute("INSERT INTO prompt_versions(id,prompt_id,version_no,body_text,render_fingerprint,status,validation_status,is_active,created_at,updated_at) VALUES(1,1,1,'body','fp','active','VALID',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
            conn.commit()
            pre = prepare_prompt_execution_preflight(conn, capability_code='CREATE_BULK_JSON_DRAFT', target_type='workflow', target_id='wf-1', operator_id_or_system_actor='operator-1', prompt_record_id=1, prompt_version_id=1, binding_resolution_fingerprint='bf', rendered_payload_hash='rh', action_payload_hash='ah', reviewed_target_state_hash='sh')
            conn.close()
            reopened = sqlite3.connect(env.db_path)
            reopened.row_factory = sqlite3.Row
            event_count = reopened.execute("SELECT COUNT(*) FROM prompt_execution_lifecycle_events WHERE execution_group_id=? AND state_after='CONFIRMATION_REQUIRED'", (pre['execution_group_id'],)).fetchone()[0]
            audit_count = reopened.execute("SELECT COUNT(*) FROM prompt_audit_events WHERE event_type='runtime_confirmation_required'").fetchone()[0]
            self.assertEqual(1, event_count)
            self.assertEqual(1, audit_count)
        finally:
            td.__exit__(None, None, None)

    def test_unsafe_sync_payload_terminalizes_streams_and_usage(self):
        td, conn = self._conn()
        called = {"v": False}
        try:
            pre = self._pre(conn)
            confirm_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], confirmation_token=pre['confirmation_token'], operator_id_or_system_actor='operator-1', reviewed_target_state_hash='sh')
            reg = RuntimeAdapterRegistry()
            def adapter(_):
                called["v"] = True
                return {'result_code': 'OK', 'secret_safe_message': 'done'}
            reg.register('CREATE_BULK_JSON_DRAFT', adapter)
            out = dispatch_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], adapter_registry=reg, payload={'token': 'raw-secret'})
            self.assertEqual('FAILED_TERMINAL', out['state'])
            self.assertFalse(called["v"])
            states = [e['state_after'] for e in list_prompt_execution_timeline(conn, execution_group_id=pre['execution_group_id'])]
            self.assertIn('FAILED_TERMINAL', states)
            audit_count = conn.execute("SELECT COUNT(*) FROM prompt_audit_events WHERE event_type='runtime_failed_terminal'").fetchone()[0]
            self.assertEqual(1, audit_count)
            usage = conn.execute("SELECT terminal_outcome FROM prompt_execution_usage WHERE execution_group_id=?", (pre['execution_group_id'],)).fetchone()[0]
            self.assertEqual('FAILED_TERMINAL', usage)
        finally:
            td.__exit__(None, None, None)

    def test_async_queue_admission_failure_terminalizes_streams_and_usage(self):
        td, conn = self._conn()
        try:
            pre = self._pre(conn, capability='ENQUEUE_INTERNAL_PROMPT_JOB')
            confirm_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], confirmation_token=pre['confirmation_token'], operator_id_or_system_actor='operator-1', reviewed_target_state_hash='sh')
            conn.execute("CREATE TRIGGER prompt_execution_async_queue_fail BEFORE INSERT ON prompt_execution_async_queue BEGIN SELECT RAISE(ABORT, 'queue admission failed'); END")
            out = dispatch_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], adapter_registry=RuntimeAdapterRegistry())
            self.assertEqual('FAILED_TERMINAL', out['state'])
            states = [e['state_after'] for e in list_prompt_execution_timeline(conn, execution_group_id=pre['execution_group_id'])]
            self.assertIn('FAILED_TERMINAL', states)
            audit_count = conn.execute("SELECT COUNT(*) FROM prompt_audit_events WHERE event_type='runtime_failed_terminal'").fetchone()[0]
            self.assertEqual(1, audit_count)
            usage = conn.execute("SELECT terminal_outcome FROM prompt_execution_usage WHERE execution_group_id=?", (pre['execution_group_id'],)).fetchone()[0]
            self.assertEqual('FAILED_TERMINAL', usage)
        finally:
            td.__exit__(None, None, None)

    def test_stale_confirmation_persists_blocked_state_and_status(self):
        td, conn = self._conn()
        try:
            pre = self._pre(conn)
            out = confirm_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], confirmation_token=pre['confirmation_token'], operator_id_or_system_actor='operator-1', reviewed_target_state_hash='changed')
            self.assertEqual('STALE_BLOCKED', out['state'])
            row = conn.execute("SELECT a.state,g.current_state FROM prompt_execution_attempts a JOIN prompt_execution_groups g ON g.id=a.execution_group_id WHERE a.id=?", (pre['execution_attempt_id'],)).fetchone()
            self.assertEqual('STALE_BLOCKED', row[0])
            self.assertEqual('STALE_BLOCKED', row[1])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM prompt_execution_usage WHERE execution_group_id=?", (pre['execution_group_id'],)).fetchone()[0])
            status = get_prompt_execution_status(conn, execution_attempt_id=pre['execution_attempt_id'])
            self.assertEqual('STALE_BLOCKED', status['current_state'])
        finally:
            td.__exit__(None, None, None)

if __name__ == '__main__':
    unittest.main()
