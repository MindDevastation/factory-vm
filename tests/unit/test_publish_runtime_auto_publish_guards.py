from __future__ import annotations

import unittest

from services.common import db as dbm
from services.factory_api.publish_audit_status import ALLOWED_AUDIT_STATUSES
from services.workers.uploader import _resolve_auto_publish_suppression_reason

from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestAutoPublishGuards(unittest.TestCase):
    def _seed_defaults(self, conn: dbm.sqlite3.Connection) -> None:  # type: ignore[attr-defined]
        conn.execute(
            """
            INSERT INTO publish_policy_project_defaults(
                singleton_key, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
            ) VALUES(1, 'auto', 'public', 'policy_requires_manual', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-policy')
            """
        )
        conn.execute(
            """
            INSERT INTO publish_audit_status_project_defaults(
                singleton_key, status, created_at, updated_at, updated_by, last_reason, last_request_id
            ) VALUES(1, 'approved', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-audit')
            """
        )
        conn.execute(
            """
            INSERT INTO publish_global_controls(singleton_key, auto_publish_paused, reason, updated_at, updated_by)
            VALUES(1, 0, NULL, '2026-01-01T00:00:00Z', 'admin')
            """
        )

    def test_manual_only_mode_is_suppressed(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            conn = dbm.connect(env)
            try:
                self._seed_defaults(conn)
                conn.execute("UPDATE publish_policy_project_defaults SET publish_mode = 'manual_only' WHERE singleton_key = 1")
                job = dict(dbm.get_job(conn, job_id) or {})
                self.assertEqual(_resolve_auto_publish_suppression_reason(conn, job=job), "policy_requires_manual")
            finally:
                conn.close()

    def test_global_pause_is_suppressed(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            conn = dbm.connect(env)
            try:
                self._seed_defaults(conn)
                conn.execute("UPDATE publish_global_controls SET auto_publish_paused = 1, reason = 'paused' WHERE singleton_key = 1")
                job = dict(dbm.get_job(conn, job_id) or {})
                self.assertEqual(_resolve_auto_publish_suppression_reason(conn, job=job), "global_pause_active")
            finally:
                conn.close()

    def test_hold_active_is_suppressed(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            conn = dbm.connect(env)
            try:
                self._seed_defaults(conn)
                conn.execute(
                    "UPDATE jobs SET publish_hold_active = 1, publish_hold_reason_code = 'operator_forced_manual' WHERE id = ?",
                    (job_id,),
                )
                job = dict(dbm.get_job(conn, job_id) or {})
                self.assertEqual(_resolve_auto_publish_suppression_reason(conn, job=job), "operator_forced_manual")
            finally:
                conn.close()

    def test_audit_not_approved_is_suppressed(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            conn = dbm.connect(env)
            try:
                self._seed_defaults(conn)
                for status in ALLOWED_AUDIT_STATUSES:
                    if status == "approved":
                        continue
                    conn.execute("UPDATE publish_audit_status_project_defaults SET status = ? WHERE singleton_key = 1", (status,))
                    job = dict(dbm.get_job(conn, job_id) or {})
                    self.assertEqual(_resolve_auto_publish_suppression_reason(conn, job=job), "audit_not_approved")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
