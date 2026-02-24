from __future__ import annotations

import os
import unittest
from pathlib import Path

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir, preview_path, workspace_dir
from services.workers.cleanup import cleanup_cycle

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job


class TestCleanupCycle(unittest.TestCase):
    def test_cleanup_deletes_due_mp4_and_marks_cleaned(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="PUBLISHED", stage="APPROVAL")

            conn = dbm.connect(env)
            try:
                ts = dbm.now_ts()
                # set delete_mp4_at in the past
                dbm.update_job_state(conn, job_id, state="PUBLISHED", stage="APPROVAL", delete_mp4_at=ts - 10)
            finally:
                conn.close()

            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            pv = preview_path(env, job_id)
            pv.parent.mkdir(parents=True, exist_ok=True)
            pv.write_bytes(b"pv")

            cleanup_cycle(env=env, worker_id="t-clean")

            self.assertFalse(mp4.exists())
            self.assertFalse(pv.exists())

            conn2 = dbm.connect(env)
            try:
                job = dbm.get_job(conn2, job_id)
            finally:
                conn2.close()

            assert job is not None
            self.assertEqual(job["state"], "CLEANED")
            self.assertEqual(job["stage"], "CLEANUP")

    def test_cleanup_removes_workspace_for_non_rendering_jobs(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="APPROVED", stage="APPROVAL")
            ws = workspace_dir(env, job_id)
            (ws / "x").mkdir(parents=True, exist_ok=True)
            self.assertTrue(ws.exists())

            cleanup_cycle(env=env, worker_id="t-clean")
            self.assertFalse(ws.exists())


if __name__ == "__main__":
    unittest.main()
