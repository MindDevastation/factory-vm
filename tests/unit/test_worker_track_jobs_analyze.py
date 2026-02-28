from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest import mock

from services.common import db as dbm
from services.common.env import Env
from services.track_analyzer import track_jobs_db as tjdb
from services.workers.track_jobs import track_jobs_cycle
from tests._helpers import seed_minimal_db, temp_env


class TestTrackJobsWorkerAnalyze(unittest.TestCase):
    def test_track_analyze_job_runs_to_done_and_updates_progress(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = "/secure/gdrive/client_secret.json"
            os.environ["GDRIVE_TOKENS_DIR"] = str(Path(env.storage_root) / "gdrive_tokens")
            env = Env.load()
            seed_minimal_db(env)
            token_path = Path(env.gdrive_tokens_dir) / "darkwood-reverie" / "token.json"
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")
            conn = dbm.connect(env)
            try:
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
                for idx in range(1, 3):
                    conn.execute(
                        """
                        INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        ("darkwood-reverie", f"{idx:03d}", f"fid-{idx}", "GDRIVE", f"{idx:03d}_A.wav", "A", None, None, dbm.now_ts(), None),
                    )
                job_id = tjdb.enqueue_job(
                    conn,
                    job_type="ANALYZE_TRACKS",
                    channel_slug="darkwood-reverie",
                    payload={"scope": "pending", "force": False, "max_tracks": 2},
                )
            finally:
                conn.close()

            with mock.patch("services.workers.track_jobs.DriveClient"), mock.patch(
                "services.workers.track_jobs.analyze_tracks",
                return_value=type("S", (), {"selected": 2, "processed": 2, "failed": 0})(),
            ) as analyze_mock:
                track_jobs_cycle(env=env, worker_id="t-track-jobs-analyze")

            conn = dbm.connect(env)
            try:
                job = tjdb.get_job(conn, job_id)
                assert job is not None
                payload = dbm.json_loads(job["payload_json"])
                self.assertEqual(job["status"], "DONE")
                self.assertEqual(payload.get("processed_count"), 1)
                self.assertEqual(payload.get("total_count"), 1)
                self.assertEqual(payload.get("last_message"), "DONE")

                logs = tjdb.list_logs(conn, job_id=job_id)
                self.assertTrue(any("analyze started channel=darkwood-reverie" in (row.get("message") or "") for row in logs))
                self.assertTrue(any("analyze done channel=darkwood-reverie selected=2 processed=2 failed=0" in (row.get("message") or "") for row in logs))
                self.assertEqual(analyze_mock.call_count, 1)
            finally:
                conn.close()

    def test_track_analyze_failure_is_sanitized_and_marks_failed(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = "/secure/gdrive/client_secret.json"
            os.environ["GDRIVE_TOKENS_DIR"] = str(Path(env.storage_root) / "gdrive_tokens")
            env = Env.load()
            seed_minimal_db(env)
            token_path = Path(env.gdrive_tokens_dir) / "darkwood-reverie" / "token.json"
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")
            conn = dbm.connect(env)
            try:
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    ("darkwood-reverie", "001", "fid-1", "GDRIVE", "001_A.wav", "A", None, None, dbm.now_ts(), None),
                )
                job_id = tjdb.enqueue_job(conn, job_type="TRACK_ANALYZE", channel_slug="darkwood-reverie", payload={})
            finally:
                conn.close()

            with mock.patch("services.workers.track_jobs.DriveClient"), mock.patch(
                "services.workers.track_jobs.analyze_tracks",
                side_effect=RuntimeError(f"failed with token {env.basic_pass}"),
            ):
                track_jobs_cycle(env=env, worker_id="t-track-jobs-analyze-fail")

            conn = dbm.connect(env)
            try:
                job = tjdb.get_job(conn, job_id)
                assert job is not None
                payload = dbm.json_loads(job["payload_json"])
                self.assertEqual(job["status"], "FAILED")
                self.assertIn("job failed:", str(payload.get("last_message") or ""))
                self.assertNotIn(env.basic_pass, str(payload.get("last_message") or ""))

                logs = tjdb.list_logs(conn, job_id=job_id)
                last_error = [row for row in logs if row.get("level") == "ERROR"][-1]
                self.assertNotIn(env.basic_pass, str(last_error.get("message") or ""))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
