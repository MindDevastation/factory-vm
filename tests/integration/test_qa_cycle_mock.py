from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir, qa_path
from services.workers.qa import qa_cycle

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job


class TestQaCycleMock(unittest.TestCase):
    def test_qa_cycle_passes_and_transitions_to_uploading(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="QA_RUNNING", stage="QA")
            # create mp4
            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            probe = {
                "streams": [
                    {"codec_type": "video", "codec_name": "h264", "avg_frame_rate": "24/1", "width": 1920, "height": 1080, "duration": "30.0"},
                    {"codec_type": "audio", "codec_name": "aac", "sample_rate": "48000", "channels": 2, "duration": "30.0"},
                ]
            }

            with patch("services.workers.qa.ffprobe_json", lambda p: probe), patch("services.workers.qa.volumedetect", lambda p, seconds: (-30.0, -2.0, None)):
                qa_cycle(env=env, worker_id="t-qa")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
            finally:
                conn.close()

            assert job is not None
            self.assertEqual(job["state"], "UPLOADING")
            self.assertEqual(job["stage"], "UPLOAD")
            self.assertTrue(qa_path(env, job_id).exists())

    def test_qa_cycle_fails_when_ffprobe_raises(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="QA_RUNNING", stage="QA")
            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            def _boom(_p: Path):
                raise RuntimeError("ffprobe fail")

            with patch("services.workers.qa.ffprobe_json", _boom):
                qa_cycle(env=env, worker_id="t-qa")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
            finally:
                conn.close()

            assert job is not None
            self.assertEqual(job["state"], "QA_FAILED")


if __name__ == "__main__":
    unittest.main()
