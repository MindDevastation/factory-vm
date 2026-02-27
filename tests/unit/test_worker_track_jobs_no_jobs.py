from __future__ import annotations

import os
import tempfile
import unittest

from services.common import db as dbm
from services.common.env import Env
from services.workers.track_jobs import track_jobs_cycle


class TestTrackJobsWorkerNoJobs(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        os.environ["FACTORY_DB_PATH"] = os.path.join(self.td.name, "db.sqlite3")
        os.environ["FACTORY_STORAGE_ROOT"] = os.path.join(self.td.name, "storage")
        os.environ["FACTORY_BASIC_AUTH_PASS"] = "x"
        os.environ["GDRIVE_LIBRARY_ROOT_ID"] = "library-root"
        self.env = Env.load()

        conn = dbm.connect(self.env)
        try:
            dbm.migrate(conn)
        finally:
            conn.close()

    def tearDown(self) -> None:
        self.td.cleanup()

    def test_track_jobs_cycle_empty_queue(self) -> None:
        track_jobs_cycle(env=self.env, worker_id="t-track-jobs")


if __name__ == "__main__":
    unittest.main()
