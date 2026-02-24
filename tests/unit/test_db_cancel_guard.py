from __future__ import annotations

import unittest

from services.common import db as dbm

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job


class TestDbCancelledGuard(unittest.TestCase):
    def test_update_job_state_does_not_override_cancelled(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            jid = insert_release_and_job(env, state="READY_FOR_RENDER", stage="FETCH")

            conn = dbm.connect(env)
            try:
                dbm.cancel_job(conn, jid, reason="x")
                dbm.update_job_state(conn, jid, state="RENDERING", stage="RENDER", progress_pct=50.0)
                job = dbm.get_job(conn, jid)
            finally:
                conn.close()

            assert job is not None
            self.assertEqual(job["state"], "CANCELLED")
            self.assertEqual(job["stage"], "CANCELLED")

    def test_update_job_state_updates_non_cancelled(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            jid = insert_release_and_job(env, state="READY_FOR_RENDER", stage="FETCH")
            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, jid, state="RENDERING", stage="RENDER", progress_pct=12.5, progress_text="rendering")
                job = dbm.get_job(conn, jid)
            finally:
                conn.close()

            assert job is not None
            self.assertEqual(job["state"], "RENDERING")
            self.assertEqual(job["stage"], "RENDER")
            self.assertAlmostEqual(float(job["progress_pct"]), 12.5)


if __name__ == "__main__":
    unittest.main()
