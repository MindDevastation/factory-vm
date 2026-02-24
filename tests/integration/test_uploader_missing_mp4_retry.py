from __future__ import annotations

import os
import unittest

from services.common import db as dbm
from services.common.env import Env
from services.workers.uploader import uploader_cycle

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job


class TestUploaderMissingMp4Retry(unittest.TestCase):
    def test_missing_mp4_schedules_retry(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            os.environ["RETRY_BACKOFF_SEC"] = "1"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")

            uploader_cycle(env=env, worker_id="t-upl")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
            finally:
                conn.close()

            assert job is not None
            # should remain UPLOADING but have retry_at set
            self.assertEqual(job["state"], "UPLOADING")
            self.assertIsNotNone(job["retry_at"])
            self.assertIn("missing mp4", str(job.get("error_reason") or ""))


if __name__ == "__main__":
    unittest.main()
