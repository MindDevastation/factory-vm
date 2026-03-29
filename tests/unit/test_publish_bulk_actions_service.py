from __future__ import annotations

import unittest

from services.common import db as dbm
from services.factory_api import publish_bulk_actions as svc
from tests._helpers import seed_minimal_db, temp_env


class TestPublishBulkActionsService(unittest.TestCase):
    def _seed_job(self, env, *, publish_state: str = "retry_pending", state: str = "UPLOADED") -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch
            ts = dbm.now_ts()
            cur = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                (int(ch["id"]), "r", "d", "[]", ts),
            )
            release_id = int(cur.lastrowid)
            job_id = dbm.insert_job_with_lineage_defaults(
                conn,
                release_id=release_id,
                job_type="UI",
                state=state,
                stage="PUBLISH",
                priority=1,
                attempt=0,
                created_at=ts,
                updated_at=ts,
            )
            conn.execute(
                "UPDATE jobs SET publish_state = ?, publish_retry_at = ? WHERE id = ?",
                (publish_state, (ts + 60 if publish_state == "retry_pending" else None), job_id),
            )
            conn.commit()
            return job_id
        finally:
            conn.close()

    def test_fingerprint_is_deterministic(self) -> None:
        a = svc._build_selection_fingerprint(action="retry", selected_job_ids=[1, 2, 3], action_payload={})
        b = svc._build_selection_fingerprint(action="retry", selected_job_ids=[1, 2, 3], action_payload={})
        c = svc._build_selection_fingerprint(action="retry", selected_job_ids=[1, 3, 2], action_payload={})
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)

    def test_execute_rejects_expired_session(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env)
            conn = dbm.connect(env)
            try:
                out = svc.create_bulk_preview_session(
                    conn, action="retry", selected_job_ids=[job_id], scheduled_at=None, created_by="u", ttl_seconds=1
                )
                conn.execute(
                    "UPDATE publish_bulk_action_sessions SET expires_at = '2000-01-01T00:00:00+00:00' WHERE id = ?",
                    (out["preview_session_id"],),
                )
                conn.commit()
                with self.assertRaises(svc.PublishBulkActionError) as ctx:
                    svc.execute_bulk_preview_session(
                        conn,
                        preview_session_id=out["preview_session_id"],
                        selected_job_ids=None,
                        selection_fingerprint=None,
                        executed_by="u",
                    )
                self.assertEqual(ctx.exception.code, "PBA_SESSION_EXPIRED")
                self.assertEqual(ctx.exception.details["invalidation"]["kind"], "expired")
            finally:
                conn.close()

    def test_execute_rejects_invalidated_session(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env)
            conn = dbm.connect(env)
            try:
                out = svc.create_bulk_preview_session(
                    conn, action="retry", selected_job_ids=[job_id], scheduled_at=None, created_by="u", ttl_seconds=30
                )
                conn.execute(
                    "UPDATE publish_bulk_action_sessions SET preview_status = 'INVALIDATED', invalidation_reason_code = 'TEST' WHERE id = ?",
                    (out["preview_session_id"],),
                )
                conn.commit()
                with self.assertRaises(svc.PublishBulkActionError) as ctx:
                    svc.execute_bulk_preview_session(
                        conn,
                        preview_session_id=out["preview_session_id"],
                        selected_job_ids=None,
                        selection_fingerprint=None,
                        executed_by="u",
                    )
                self.assertEqual(ctx.exception.code, "PBA_SESSION_INVALIDATED")
                self.assertEqual(ctx.exception.details["invalidation"]["kind"], "invalidated")
            finally:
                conn.close()

    def test_execute_rejects_scope_mismatch(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env)
            conn = dbm.connect(env)
            try:
                out = svc.create_bulk_preview_session(
                    conn, action="retry", selected_job_ids=[job_id], scheduled_at=None, created_by="u", ttl_seconds=30
                )
                with self.assertRaises(svc.PublishBulkActionError) as ctx:
                    svc.execute_bulk_preview_session(
                        conn,
                        preview_session_id=out["preview_session_id"],
                        selected_job_ids=[job_id],
                        selection_fingerprint="bad",
                        executed_by="u",
                    )
                self.assertEqual(ctx.exception.code, "PBA_SCOPE_MISMATCH")
                self.assertEqual(ctx.exception.details["invalidation"]["kind"], "scope_mismatch")
            finally:
                conn.close()

    def test_reschedule_requires_explicit_future_iso_datetime(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env, publish_state="ready_to_publish")
            conn = dbm.connect(env)
            try:
                with self.assertRaises(svc.PublishBulkActionError) as missing:
                    svc.create_bulk_preview_session(
                        conn, action="reschedule", selected_job_ids=[job_id], scheduled_at=None, created_by="u", ttl_seconds=30
                    )
                self.assertEqual(missing.exception.code, "PJA_INVALID_DATETIME")

                with self.assertRaises(svc.PublishBulkActionError) as past:
                    svc.create_bulk_preview_session(
                        conn,
                        action="reschedule",
                        selected_job_ids=[job_id],
                        scheduled_at="2000-01-01T00:00:00Z",
                        created_by="u",
                        ttl_seconds=30,
                    )
                self.assertEqual(past.exception.code, "PJA_RESCHEDULE_NOT_FUTURE")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
