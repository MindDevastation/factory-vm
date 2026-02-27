from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from services.common.env import Env
from services.common import db as dbm
from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job


class TestDbMoreCoverage(unittest.TestCase):
    def test_json_loads_invalid_returns_none(self):
        self.assertIsNone(dbm.json_loads("{not-json"))

    def test_list_jobs_state_filter_branch(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="READY_FOR_RENDER")
            conn = dbm.connect(env)
            try:
                rows = dbm.list_jobs(conn, state="READY_FOR_RENDER", limit=10)
                self.assertTrue(any(int(r["id"]) == job_id for r in rows))
            finally:
                conn.close()

    def test_update_job_state_sets_approval_notified_at(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env)
            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, job_id, state="WAIT_APPROVAL", stage="APPROVAL", approval_notified_at=123.0)
                row = dbm.get_job(conn, job_id)
                self.assertEqual(float(row["approval_notified_at"]), 123.0)
            finally:
                conn.close()

    def test_set_youtube_error_upsert(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            conn = dbm.connect(env)
            try:
                dbm.set_youtube_error(conn, job_id, "boom")
                row = conn.execute("SELECT error FROM youtube_uploads WHERE job_id=?", (job_id,)).fetchone()
                self.assertEqual(row["error"], "boom")
            finally:
                conn.close()

    def test_pending_reply_roundtrip(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                dbm.set_pending_reply(conn, user_id=1, job_id=2, kind="APPROVE")
                row = dbm.pop_pending_reply(conn, user_id=1)
                self.assertIsNotNone(row)
                self.assertEqual(int(row["job_id"]), 2)
                self.assertIsNone(dbm.pop_pending_reply(conn, user_id=1))
            finally:
                conn.close()

    def test_upsert_tg_message(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env)
            conn = dbm.connect(env)
            try:
                dbm.upsert_tg_message(conn, job_id=job_id, chat_id=100, message_id=200)
                dbm.upsert_tg_message(conn, job_id=job_id, chat_id=101, message_id=201)
                row = conn.execute("SELECT chat_id, message_id FROM tg_messages WHERE job_id=?", (job_id,)).fetchone()
                self.assertEqual(int(row["chat_id"]), 101)
                self.assertEqual(int(row["message_id"]), 201)
            finally:
                conn.close()

    def test_reclaim_stale_render_jobs_retry_and_terminal(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                job_id = insert_release_and_job(env, state="RENDERING", stage="RENDER")
                conn.execute(
                    "UPDATE jobs SET locked_by='w', locked_at=?, attempt=0 WHERE id=?",
                    (dbm.now_ts() - 999999, job_id),
                )
                n = dbm.reclaim_stale_render_jobs(conn, lock_ttl_sec=1, backoff_sec=1, max_attempts=3)
                self.assertEqual(n, 1)
                row = dbm.get_job(conn, job_id)
                self.assertEqual(str(row["state"]), "READY_FOR_RENDER")

                conn.execute(
                    "UPDATE jobs SET state='RENDERING', stage='RENDER', locked_by='w', locked_at=?, attempt=10 WHERE id=?",
                    (dbm.now_ts() - 999999, job_id),
                )
                n2 = dbm.reclaim_stale_render_jobs(conn, lock_ttl_sec=1, backoff_sec=1, max_attempts=3)
                self.assertEqual(n2, 1)
                row2 = dbm.get_job(conn, job_id)
                self.assertEqual(str(row2["state"]), "RENDER_FAILED")
            finally:
                conn.close()

    def test_force_unlock_clears_lock(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="RENDERING", stage="RENDER")
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE jobs SET locked_by='w', locked_at=? WHERE id=?", (dbm.now_ts(), job_id))
                dbm.force_unlock(conn, job_id)
                row = dbm.get_job(conn, job_id)
                self.assertIsNone(row["locked_by"])
            finally:
                conn.close()

    def test_get_job_includes_channel_identifiers(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            conn = dbm.connect(env)
            try:
                row = dbm.get_job(conn, job_id)
                self.assertEqual(str(row["channel_slug"]), "darkwood-reverie")
                self.assertIsInstance(int(row["channel_id"]), int)
            finally:
                conn.close()

    def test_migration_adds_retry_at_for_older_db(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "old.sqlite3"
            storage_root = Path(td) / "storage"

            old = os.environ.copy()
            try:
                os.environ["FACTORY_DB_PATH"] = str(db_path)
                os.environ["FACTORY_STORAGE_ROOT"] = str(storage_root)
                os.environ["FACTORY_BASIC_AUTH_USER"] = "a"
                os.environ["FACTORY_BASIC_AUTH_PASS"] = "b"
                os.environ["ORIGIN_BACKEND"] = "local"
                os.environ["UPLOAD_BACKEND"] = "mock"
                os.environ["TELEGRAM_ENABLED"] = "0"
                os.environ["TG_ADMIN_CHAT_ID"] = "0"

                env = Env.load()
                conn = sqlite3.connect(env.db_path)
                conn.row_factory = dbm._dict_factory  # type: ignore[attr-defined]
                try:
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS jobs (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            release_id INTEGER NOT NULL,
                            job_type TEXT NOT NULL,
                            state TEXT NOT NULL,
                            stage TEXT NOT NULL,
                            priority INTEGER NOT NULL DEFAULT 0,
                            attempt INTEGER NOT NULL DEFAULT 0,
                            locked_by TEXT,
                            locked_at REAL,
                            created_at REAL NOT NULL,
                            updated_at REAL NOT NULL
                        );
                        """
                    )
                    conn.commit()
                    dbm.migrate(conn)
                    cols = {r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()}
                    self.assertIn("retry_at", cols)
                finally:
                    conn.close()
            finally:
                os.environ.clear()
                os.environ.update(old)


    def test_channels_unique_youtube_channel_id(self):
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                dbm.create_channel(conn, slug="yt-a", display_name="YT A", youtube_channel_id="UC_DUP")
                with self.assertRaises(sqlite3.IntegrityError):
                    dbm.create_channel(conn, slug="yt-b", display_name="YT B", youtube_channel_id="UC_DUP")
                row = dbm.get_channel_by_youtube_channel_id(conn, "UC_DUP")
                self.assertIsNotNone(row)
                self.assertEqual(str(row["slug"]), "yt-a")
            finally:
                conn.close()


    def test_migrate_renames_legacy_track_tables_non_destructively(self):
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                conn.execute("CREATE TABLE canon_channels (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL UNIQUE, legacy_col TEXT)")
                dbm.migrate(conn)

                names = {
                    str(r["name"])
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                self.assertIn("canon_channels", names)
                self.assertTrue(any(n == "canon_channels__legacy" or n.startswith("canon_channels__legacy_") for n in names))

                canon_cols = {r["name"] for r in conn.execute("PRAGMA table_info(canon_channels)").fetchall()}
                self.assertEqual(canon_cols, {"id", "value"})

            finally:
                conn.close()
    def test_migrate_creates_canon_tables(self):
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                names = {
                    str(r["name"])
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                self.assertTrue(
                    {
                        "canon_channels",
                        "canon_tags",
                        "canon_forbidden",
                        "canon_palettes",
                        "canon_thresholds",
                        "tracks",
                        "track_features",
                        "track_tags",
                        "track_scores",
                        "track_jobs",
                        "track_job_logs",
                    }.issubset(names)
                )
            finally:
                conn.close()
