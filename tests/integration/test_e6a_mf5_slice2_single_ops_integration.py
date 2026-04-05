from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_inbox import execute_single_ops_action
from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestE6AMf5Slice2SingleOpsIntegration(unittest.TestCase):
    def test_safe_retry_and_ack_actions(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            conn = dbm.connect(env)
            try:
                job_id = int(conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()["id"])
                conn.execute("UPDATE jobs SET publish_state='retry_pending', publish_retry_at=? WHERE id=?", (dbm.now_ts()+60, job_id,))
                conn.commit()
                retry = execute_single_ops_action(conn, job_id=job_id, action="retry", actor="telegram:1", confirm=True, reason="retry", request_id="mf5-s2-r")
                self.assertEqual(retry["status"], "SUCCESS")

                conn.execute("UPDATE jobs SET publish_state='manual_handoff_pending' WHERE id=?", (job_id,))
                conn.commit()
                ack = execute_single_ops_action(conn, job_id=job_id, action="acknowledge", actor="telegram:1", confirm=True, reason="ack", request_id="mf5-s2-a")
                self.assertEqual(ack["status"], "SUCCESS")
            finally:
                conn.close()

    def test_blocked_state_returns_failed(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="CANCELLED", stage="CANCELLED")
            conn = dbm.connect(env)
            try:
                job_id = int(conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()["id"])
                out = execute_single_ops_action(conn, job_id=job_id, action="retry", actor="telegram:1", confirm=True, reason="retry", request_id="mf5-s2-b")
                self.assertEqual(out["status"], "FAILED")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
