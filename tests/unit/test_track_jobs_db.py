from __future__ import annotations

import unittest

from services.common import db as dbm
from services.track_analyzer import track_jobs_db as tjdb
from tests._helpers import temp_env


class TrackJobsDbTests(unittest.TestCase):
    def test_already_running_detection(self) -> None:
        with temp_env() as (_, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)

                self.assertFalse(tjdb.has_already_running(conn, job_type="SCAN_TRACKS", channel_slug="ch-a"))

                tjdb.enqueue_job(conn, job_type="SCAN_TRACKS", channel_slug="ch-a")
                self.assertTrue(tjdb.has_already_running(conn, job_type="SCAN_TRACKS", channel_slug="ch-a"))

                self.assertFalse(tjdb.has_already_running(conn, job_type="SCAN_TRACKS", channel_slug="ch-b"))
                self.assertFalse(tjdb.has_already_running(conn, job_type="ANALYZE_TRACKS", channel_slug="ch-a"))
            finally:
                conn.close()

    def test_fifo_claim_selects_earliest_queued(self) -> None:
        with temp_env() as (_, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)

                job1 = tjdb.enqueue_job(conn, job_type="SCAN_TRACKS", channel_slug="ch-a")
                job2 = tjdb.enqueue_job(conn, job_type="SCAN_TRACKS", channel_slug="ch-b")

                claimed = tjdb.claim_queued_job(conn)
                self.assertIsNotNone(claimed)
                self.assertEqual(int(claimed["id"]), job1)
                self.assertEqual(claimed["status"], "RUNNING")

                second_row = tjdb.get_job(conn, job2)
                assert second_row is not None
                self.assertEqual(second_row["status"], "QUEUED")
            finally:
                conn.close()

    def test_atomic_claim_no_double_claim(self) -> None:
        with temp_env() as (_, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)

                job_id = tjdb.enqueue_job(conn, job_type="SCAN_TRACKS", channel_slug="ch-a")

                first = tjdb.claim_queued_job(conn)
                second = tjdb.claim_queued_job(conn)

                self.assertIsNotNone(first)
                self.assertEqual(int(first["id"]), job_id)
                self.assertIsNone(second)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
