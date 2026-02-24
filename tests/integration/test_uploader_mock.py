from __future__ import annotations

import os
import unittest

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir
from services.workers.uploader import uploader_cycle

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job


class TestUploaderMock(unittest.TestCase):
    def test_uploader_mock_sets_wait_approval_and_youtube_upload(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            uploader_cycle(env=env, worker_id="t-upl")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                yt = conn.execute("SELECT * FROM youtube_uploads WHERE job_id=?", (job_id,)).fetchone()
            finally:
                conn.close()

            assert job is not None
            self.assertEqual(job["state"], "WAIT_APPROVAL")
            self.assertIsNotNone(yt)
            assert yt is not None
            self.assertTrue(str(yt["video_id"]).startswith("mock-"))

    def test_uploader_is_idempotent_if_youtube_upload_exists(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            conn = dbm.connect(env)
            try:
                dbm.set_youtube_upload(conn, job_id, video_id="already", url="u", studio_url="s", privacy="private")
            finally:
                conn.close()

            uploader_cycle(env=env, worker_id="t-upl")

            conn2 = dbm.connect(env)
            try:
                job = dbm.get_job(conn2, job_id)
            finally:
                conn2.close()

            assert job is not None
            self.assertEqual(job["state"], "WAIT_APPROVAL")


if __name__ == "__main__":
    unittest.main()
