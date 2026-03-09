from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from tests._helpers import temp_env


class TestJobsRetryMigration(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_env_ctx = temp_env()
        self._td, self.env = self._temp_env_ctx.__enter__()
        self.conn = dbm.connect(self.env)

    def tearDown(self) -> None:
        self.conn.close()
        self._temp_env_ctx.__exit__(None, None, None)

    def _create_legacy_jobs_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                kind TEXT NOT NULL,
                weight REAL NOT NULL DEFAULT 1.0,
                render_profile TEXT NOT NULL,
                autopublish_enabled INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE releases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                planned_at TEXT,
                origin_release_folder_id TEXT,
                origin_meta_file_id TEXT UNIQUE,
                created_at REAL NOT NULL,
                FOREIGN KEY(channel_id) REFERENCES channels(id)
            );

            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                release_id INTEGER NOT NULL,
                job_type TEXT NOT NULL,
                state TEXT NOT NULL,
                stage TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                attempt INTEGER NOT NULL DEFAULT 0,
                locked_by TEXT,
                locked_at REAL,
                retry_at REAL,
                progress_pct REAL NOT NULL DEFAULT 0.0,
                progress_text TEXT,
                progress_updated_at REAL,
                error_reason TEXT,
                approval_notified_at REAL,
                published_at REAL,
                delete_mp4_at REAL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(release_id) REFERENCES releases(id)
            );

            CREATE INDEX idx_jobs_state_priority ON jobs(state, priority, created_at);
            """
        )

    def test_migration_adds_retry_lineage_columns_and_indexes(self) -> None:
        dbm.migrate(self.conn)

        cols = self.conn.execute("PRAGMA table_info(jobs)").fetchall()
        col_names = {str(c["name"]) for c in cols}
        self.assertIn("retry_of_job_id", col_names)
        self.assertIn("root_job_id", col_names)
        self.assertIn("attempt_no", col_names)
        self.assertIn("force_refetch_inputs", col_names)

        indexes = self.conn.execute("PRAGMA index_list(jobs)").fetchall()
        idx_names = {str(i["name"]) for i in indexes}
        self.assertIn("idx_jobs_retry_of_job_id", idx_names)
        self.assertIn("idx_jobs_root_job_id_attempt_no", idx_names)

    def test_migration_backfills_existing_job_lineage_defaults(self) -> None:
        self._create_legacy_jobs_schema()
        self.conn.execute(
            "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
            ("channel-a", "Channel A", "LONG", 1.0, "long_1080p24", 0),
        )
        channel_id = int(self.conn.execute("SELECT id FROM channels").fetchone()["id"])
        self.conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (channel_id, "Legacy", "desc", "[]", None, None, "meta-legacy", 1.0),
        )
        release_id = int(self.conn.execute("SELECT id FROM releases").fetchone()["id"])
        cur = self.conn.execute(
            "INSERT INTO jobs(release_id, job_type, state, stage, priority, attempt, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (release_id, "RENDER_LONG", "FAILED", "RENDER", 0, 0, 1.0, 1.0),
        )
        legacy_job_id = int(cur.lastrowid)

        dbm.migrate(self.conn)

        row = self.conn.execute(
            "SELECT id, retry_of_job_id, root_job_id, attempt_no, force_refetch_inputs FROM jobs WHERE id = ?",
            (legacy_job_id,),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row["retry_of_job_id"])
        self.assertEqual(int(row["root_job_id"]), legacy_job_id)
        self.assertEqual(int(row["attempt_no"]), 1)
        self.assertEqual(int(row["force_refetch_inputs"]), 0)

    def test_unique_retry_of_job_id_is_enforced(self) -> None:
        dbm.migrate(self.conn)
        self.conn.execute(
            "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
            ("channel-a", "Channel A", "LONG", 1.0, "long_1080p24", 0),
        )
        channel_id = int(self.conn.execute("SELECT id FROM channels").fetchone()["id"])

        for i in range(3):
            self.conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
                (channel_id, f"R{i}", "desc", "[]", None, None, f"meta-{i}", 1.0),
            )

        release_rows = self.conn.execute("SELECT id FROM releases ORDER BY id").fetchall()
        release_ids = [int(r["id"]) for r in release_rows]

        base_id = int(
            self.conn.execute(
                "INSERT INTO jobs(id, release_id, job_type, state, stage, root_job_id, attempt_no, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (1001, release_ids[0], "RENDER_LONG", "FAILED", "RENDER", 1001, 1, 1.0, 1.0),
            ).lastrowid
        )

        self.conn.execute(
            "INSERT INTO jobs(release_id, job_type, state, stage, retry_of_job_id, root_job_id, attempt_no, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (release_ids[1], "RENDER_LONG", "PLANNED", "FETCH", base_id, base_id, 2, 1.0, 1.0),
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO jobs(release_id, job_type, state, stage, retry_of_job_id, root_job_id, attempt_no, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (release_ids[2], "RENDER_LONG", "PLANNED", "FETCH", base_id, base_id, 3, 1.0, 1.0),
            )

    def test_attempt_no_check_is_enforced(self) -> None:
        dbm.migrate(self.conn)
        self.conn.execute(
            "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
            ("channel-a", "Channel A", "LONG", 1.0, "long_1080p24", 0),
        )
        channel_id = int(self.conn.execute("SELECT id FROM channels").fetchone()["id"])
        self.conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (channel_id, "R", "desc", "[]", None, None, "meta-check", 1.0),
        )
        release_id = int(self.conn.execute("SELECT id FROM releases").fetchone()["id"])
        self.conn.execute(
            "INSERT INTO jobs(id, release_id, job_type, state, stage, root_job_id, attempt_no, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (2001, release_id, "RENDER_LONG", "FAILED", "RENDER", 2001, 1, 1.0, 1.0),
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO jobs(release_id, job_type, state, stage, root_job_id, attempt_no, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (release_id, "RENDER_LONG", "PLANNED", "FETCH", 2001, 0, 1.0, 1.0),
            )


if __name__ == "__main__":
    unittest.main()
