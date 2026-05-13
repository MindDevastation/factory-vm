from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.runtime_adapters import RuntimeAdapterRegistry, build_default_runtime_adapter_registry
from services.prompt_registry.runtime_execution import compute_action_payload_hash, confirm_prompt_execution, dispatch_prompt_execution, prepare_prompt_execution_preflight
from tests._helpers import seed_minimal_db, temp_env
from tests._runtime_authority import seed_runtime_authorities


class TestPromptRegistryRuntimeMf3(unittest.TestCase):
    def _conn(self):
        td = temp_env()
        _td, env = td.__enter__()
        seed_minimal_db(env)
        conn = sqlite3.connect(env.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("INSERT INTO prompt_records(id,slug,code,title,record_type,status,validation_status,bridge_policy_hook,active_version_id,created_at,updated_at) VALUES(1,'p','p','p','prompt_template','active','VALID',NULL,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
        conn.execute("INSERT INTO prompt_versions(id,prompt_id,version_no,body_text,render_fingerprint,status,validation_status,is_active,created_at,updated_at) VALUES(1,1,1,'body','fp','active','VALID',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
        for cap in ("CREATE_BULK_JSON_DRAFT", "CREATE_METADATA_REQUEST", "CREATE_VISUAL_REQUEST", "CREATE_ANALYTICS_REQUEST", "ENQUEUE_INTERNAL_PROMPT_JOB", "GENERATE_OPERATOR_HANDOFF_EXPORT"):
            seed_runtime_authorities(conn, operator="operator-1", capability=cap, target_type="workflow", binding_fingerprint="bf", render_hash="rh")
        conn.commit()
        return td, conn

    def _admitted_attempt(self, conn: sqlite3.Connection, capability: str, dispatch_payload: dict | None = None):
        dispatch_payload = dict(dispatch_payload or {})
        base = dict(capability_code=capability,target_type='workflow',target_id='wf-1',operator_id_or_system_actor='operator-1',prompt_record_id=1,prompt_version_id=1,binding_resolution_fingerprint='bf',rendered_payload_hash='rh',action_payload_hash=compute_action_payload_hash(dispatch_payload),dispatch_payload=dispatch_payload,reviewed_target_state_hash='sh')
        pre = prepare_prompt_execution_preflight(conn, **base)
        confirm_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], confirmation_token=pre['confirmation_token'], operator_id_or_system_actor='operator-1', reviewed_target_state_hash=pre['reviewed_target_state_hash'])
        return pre['execution_attempt_id']


    def _bulk_payload(self):
        return {
            "channel_slug": "darkwood-reverie",
            "title": "Runtime Draft",
            "description": "Runtime-created draft",
            "tags_csv": "runtime,test",
            "background_name": "background",
            "background_ext": ".png",
            "audio_ids_text": "audio-1",
        }

    def _release_id(self, conn: sqlite3.Connection) -> int:
        channel_id = conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()[0]
        cur = conn.execute(
            "INSERT INTO releases(channel_id,title,description,tags_json,created_at) VALUES(?,?,?,?,?)",
            (channel_id, "Runtime Release", "desc", "[]", 1.0),
        )
        conn.commit()
        return int(cur.lastrowid)

    def test_sync_dispatch_success_and_no_async_enqueue(self):
        td, conn = self._conn()
        try:
            aid = self._admitted_attempt(conn, 'CREATE_BULK_JSON_DRAFT', self._bulk_payload())
            reg = build_default_runtime_adapter_registry()
            out = dispatch_prompt_execution(conn, execution_attempt_id=aid, adapter_registry=reg)
            self.assertEqual('SUCCEEDED', out['state'])
            cnt = conn.execute('SELECT COUNT(*) FROM prompt_execution_async_queue').fetchone()[0]
            self.assertEqual(0, cnt)
        finally:
            td.__exit__(None, None, None)

    def test_default_sync_adapter_records_internal_product_target(self):
        td, conn = self._conn()
        try:
            aid = self._admitted_attempt(conn, 'CREATE_BULK_JSON_DRAFT', self._bulk_payload())
            out = dispatch_prompt_execution(conn, execution_attempt_id=aid, adapter_registry=build_default_runtime_adapter_registry())
            self.assertEqual('SUCCEEDED', out['state'])
            self.assertEqual('BULK_JSON_DRAFT_TARGET_UPDATED', out['result_code'])
            usage = conn.execute('SELECT artifact_ref,usage_payload_json FROM prompt_execution_usage WHERE latest_attempt_id=?', (aid,)).fetchone()
            self.assertIn('ui_job_drafts:', usage[0])
            job_id = int(str(usage[0]).split(':', 1)[1])
            self.assertIsNotNone(conn.execute('SELECT 1 FROM ui_job_drafts WHERE job_id=?', (job_id,)).fetchone())
            self.assertIn('internal_product_target', usage[1])
        finally:
            td.__exit__(None, None, None)

    def test_default_sync_adapter_fails_closed_without_internal_target_write(self):
        td, conn = self._conn()
        try:
            aid = self._admitted_attempt(conn, 'CREATE_METADATA_REQUEST', {'selected_item_ids': ['release-1']})
            conn.execute('DELETE FROM prompt_execution_usage WHERE latest_attempt_id=?', (aid,))
            conn.commit()
            out = dispatch_prompt_execution(conn, execution_attempt_id=aid, adapter_registry=build_default_runtime_adapter_registry())
            self.assertEqual('FAILED_TERMINAL', out['state'])
            self.assertEqual('ADAPTER_ERROR', out['result_code'])
        finally:
            td.__exit__(None, None, None)


    def test_default_metadata_visual_and_analytics_adapters_create_real_targets(self):
        td, conn = self._conn()
        try:
            metadata_aid = self._admitted_attempt(conn, 'CREATE_METADATA_REQUEST', {'selected_item_ids': ['item-1'], 'requested_fields': ['title']})
            metadata = dispatch_prompt_execution(conn, execution_attempt_id=metadata_aid, adapter_registry=build_default_runtime_adapter_registry())
            self.assertEqual('SUCCEEDED', metadata['state'])
            metadata_ref = conn.execute('SELECT artifact_ref FROM prompt_execution_usage WHERE latest_attempt_id=?', (metadata_aid,)).fetchone()[0]
            self.assertTrue(str(metadata_ref).startswith('metadata_bulk_preview_sessions:'))
            self.assertIsNotNone(conn.execute('SELECT 1 FROM metadata_bulk_preview_sessions WHERE id=?', (str(metadata_ref).split(':', 1)[1],)).fetchone())

            release_id = self._release_id(conn)
            visual_aid = self._admitted_attempt(conn, 'CREATE_VISUAL_REQUEST', {'selected_release_ids': [release_id], 'action_type': 'BULK_GENERATE_PREVIEWS'})
            visual = dispatch_prompt_execution(conn, execution_attempt_id=visual_aid, adapter_registry=build_default_runtime_adapter_registry())
            self.assertEqual('SUCCEEDED', visual['state'])
            visual_ref = conn.execute('SELECT artifact_ref FROM prompt_execution_usage WHERE latest_attempt_id=?', (visual_aid,)).fetchone()[0]
            self.assertTrue(str(visual_ref).startswith('release_visual_batch_preview_sessions:'))
            self.assertIsNotNone(conn.execute('SELECT 1 FROM release_visual_batch_preview_sessions WHERE id=?', (str(visual_ref).split(':', 1)[1],)).fetchone())

            analytics_aid = self._admitted_attempt(conn, 'CREATE_ANALYTICS_REQUEST', {'report_scope_type': 'OVERVIEW', 'artifact_type': 'API_REPORT'})
            analytics = dispatch_prompt_execution(conn, execution_attempt_id=analytics_aid, adapter_registry=build_default_runtime_adapter_registry())
            self.assertEqual('SUCCEEDED', analytics['state'])
            analytics_ref = conn.execute('SELECT artifact_ref FROM prompt_execution_usage WHERE latest_attempt_id=?', (analytics_aid,)).fetchone()[0]
            self.assertTrue(str(analytics_ref).startswith('analytics_report_records:'))
            self.assertIsNotNone(conn.execute('SELECT 1 FROM analytics_report_records WHERE id=? AND generation_status=\'PENDING\'', (int(str(analytics_ref).split(':', 1)[1]),)).fetchone())
        finally:
            td.__exit__(None, None, None)

    def test_default_sync_adapter_fails_closed_when_real_target_payload_missing(self):
        td, conn = self._conn()
        try:
            aid = self._admitted_attempt(conn, 'CREATE_BULK_JSON_DRAFT', {'channel_slug': 'darkwood-reverie'})
            out = dispatch_prompt_execution(conn, execution_attempt_id=aid, adapter_registry=build_default_runtime_adapter_registry())
            self.assertEqual('FAILED_TERMINAL', out['state'])
            self.assertEqual('ADAPTER_ERROR', out['result_code'])
            self.assertEqual(0, conn.execute('SELECT COUNT(*) FROM ui_job_drafts').fetchone()[0])
        finally:
            td.__exit__(None, None, None)

    def test_sync_adapter_failure_failed_terminal(self):
        td, conn = self._conn()
        try:
            aid = self._admitted_attempt(conn, 'CREATE_METADATA_REQUEST')
            reg = RuntimeAdapterRegistry()
            def boom(_):
                raise RuntimeError('x')
            reg.register('CREATE_METADATA_REQUEST', boom)
            out = dispatch_prompt_execution(conn, execution_attempt_id=aid, adapter_registry=reg)
            self.assertEqual('FAILED_TERMINAL', out['state'])
        finally:
            td.__exit__(None, None, None)

    def test_async_dispatch_enqueues_and_transitions_dispatched(self):
        td, conn = self._conn()
        try:
            aid = self._admitted_attempt(conn, 'ENQUEUE_INTERNAL_PROMPT_JOB')
            out = dispatch_prompt_execution(conn, execution_attempt_id=aid, adapter_registry=RuntimeAdapterRegistry())
            self.assertEqual('DISPATCHED', out['state'])
            q = conn.execute('SELECT execution_attempt_id,queue_state FROM prompt_execution_async_queue WHERE execution_attempt_id=?', (aid,)).fetchone()
            self.assertEqual(aid, q[0])
            self.assertEqual('QUEUED', q[1])
            payload_json = conn.execute('SELECT payload_json FROM prompt_execution_async_queue WHERE execution_attempt_id=?', (aid,)).fetchone()[0]
            self.assertEqual('{}', payload_json)
        finally:
            td.__exit__(None, None, None)

    def test_dispatch_rejects_non_admitted_and_mode_mismatch_and_missing_adapter(self):
        td, conn = self._conn()
        try:
            pre = prepare_prompt_execution_preflight(conn, capability_code='CREATE_VISUAL_REQUEST',target_type='workflow',target_id='wf-1',operator_id_or_system_actor='operator-1',prompt_record_id=1,prompt_version_id=1,binding_resolution_fingerprint='bf',rendered_payload_hash='rh',action_payload_hash=compute_action_payload_hash({}),reviewed_target_state_hash='sh')
            with self.assertRaises(ValueError):
                dispatch_prompt_execution(conn, execution_attempt_id=pre['execution_attempt_id'], adapter_registry=RuntimeAdapterRegistry())

            aid = self._admitted_attempt(conn, 'GENERATE_OPERATOR_HANDOFF_EXPORT')
            conn.execute("UPDATE prompt_execution_attempts SET execution_mode='SYNC' WHERE id=?", (aid,))
            conn.commit()
            with self.assertRaises(ValueError):
                dispatch_prompt_execution(conn, execution_attempt_id=aid, adapter_registry=RuntimeAdapterRegistry())

            aid2 = self._admitted_attempt(conn, 'CREATE_ANALYTICS_REQUEST')
            with self.assertRaises(ValueError):
                dispatch_prompt_execution(conn, execution_attempt_id=aid2, adapter_registry=RuntimeAdapterRegistry())
        finally:
            td.__exit__(None, None, None)


    def test_sync_unsafe_payload_rejected_before_adapter_call(self):
        td, conn = self._conn()
        called = {"v": False}
        try:
            with self.assertRaises(ValueError):
                self._admitted_attempt(conn, 'CREATE_BULK_JSON_DRAFT', {"token": "abc"})
            self.assertFalse(called["v"])
            self.assertEqual(0, conn.execute('SELECT COUNT(*) FROM prompt_execution_attempts').fetchone()[0])
        finally:
            td.__exit__(None, None, None)

    def test_sync_unsafe_nested_payload_rejected(self):
        td, conn = self._conn()
        called = {"v": False}
        try:
            with self.assertRaises(ValueError):
                self._admitted_attempt(conn, 'CREATE_METADATA_REQUEST', {"meta": {"authorization": "Bearer X"}})
            self.assertFalse(called["v"])
            self.assertEqual(0, conn.execute('SELECT COUNT(*) FROM prompt_execution_attempts').fetchone()[0])
        finally:
            td.__exit__(None, None, None)

    def test_adapter_result_secret_like_message_rejected(self):
        td, conn = self._conn()
        try:
            aid = self._admitted_attempt(conn, 'CREATE_ANALYTICS_REQUEST', {"safe": "ok"})
            reg = RuntimeAdapterRegistry()
            reg.register('CREATE_ANALYTICS_REQUEST', lambda _ : {'result_code': 'OK', 'secret_safe_message': 'bearer token=123'})
            out = dispatch_prompt_execution(conn, execution_attempt_id=aid, adapter_registry=reg)
            self.assertEqual('FAILED_TERMINAL', out['state'])
            row = conn.execute('SELECT secret_safe_message FROM prompt_execution_attempts WHERE id=?', (aid,)).fetchone()
            self.assertEqual('Adapter result failed secret-safety precheck.', row[0])
            self.assertNotIn('token=123', row[0])
        finally:
            td.__exit__(None, None, None)

if __name__ == '__main__':
    unittest.main()
