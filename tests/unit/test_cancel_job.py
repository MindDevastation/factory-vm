from __future__ import annotations

import os
import tempfile
import unittest

from services.common.env import Env
from services.common import db as dbm


class TestCancelJob(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        os.environ["FACTORY_DB_PATH"] = os.path.join(self.td.name, "db.sqlite3")
        os.environ["FACTORY_STORAGE_ROOT"] = os.path.join(self.td.name, "storage")
        os.environ["FACTORY_BASIC_AUTH_PASS"] = "x"
        self.env = Env.load()

        conn = dbm.connect(self.env)
        try:
            dbm.migrate(conn)
            ts = dbm.now_ts()
            conn.execute(
                "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                ("ch", "Ch", "X", 1.0, "rp", 0),
            )
            conn.execute(
                "INSERT INTO render_profiles(name, video_w, video_h, fps, vcodec_required, audio_sr, audio_ch, acodec_required) VALUES(?,?,?,?,?,?,?,?)",
                ("rp", 1920, 1080, 24.0, "h264", 48000, 2, "aac"),
            )
            cur = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
                (1, "t", "d", "[]", None, None, f"m_{int(ts)}", ts),
            )
            self.release_id = int(cur.lastrowid)

            cur2 = conn.execute(
                "INSERT INTO jobs(release_id, job_type, state, stage, priority, attempt, locked_by, locked_at, retry_at, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (self.release_id, "RENDER_LONG", "RENDERING", "RENDER", 1, 0, "w1", ts, ts + 3600, ts, ts),
            )
            self.job_id = int(cur2.lastrowid)
        finally:
            conn.close()

    def tearDown(self) -> None:
        self.td.cleanup()

    def test_cancel_clears_lock_and_retry(self) -> None:
        conn = dbm.connect(self.env)
        try:
            dbm.cancel_job(conn, self.job_id, reason="cancelled for test")
            job = dbm.get_job(conn, self.job_id)
        finally:
            conn.close()

        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job["state"], "CANCELLED")
        self.assertEqual(job["stage"], "CANCELLED")
        self.assertIsNone(job["locked_by"])
        self.assertIsNone(job["locked_at"])
        self.assertIsNone(job["retry_at"])
        self.assertEqual(job["error_reason"], "cancelled for test")


if __name__ == "__main__":
    unittest.main()
