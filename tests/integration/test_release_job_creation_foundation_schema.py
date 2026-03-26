from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestReleaseJobCreationFoundationSchema(unittest.TestCase):
    def _insert_release(self, conn: sqlite3.Connection, *, title: str) -> int:
        channel = conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()
        assert channel is not None
        return int(
            conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                VALUES(?, ?, 'd', '[]', NULL, NULL, ?, NULL, 1.0)
                """,
                (int(channel["id"]), title, f"meta-{title}"),
            ).lastrowid
        )

    def _insert_job(self, conn: sqlite3.Connection, *, release_id: int, state: str = "DRAFT") -> int:
        ts = dbm.now_ts()
        return dbm.insert_job_with_lineage_defaults(
            conn,
            release_id=release_id,
            job_type="UI",
            state=state,
            stage="DRAFT",
            priority=0,
            attempt=0,
            created_at=ts,
            updated_at=ts,
        )

    def test_migration_adds_current_open_job_id_column(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(releases)").fetchall()}
                self.assertIn("current_open_job_id", cols)
            finally:
                conn.close()

    def test_fk_behavior_for_current_open_job_id(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="fk")
                job_id = self._insert_job(conn, release_id=release_id)
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))

                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (999999, release_id))
            finally:
                conn.close()

    def test_partial_unique_support_for_current_open_job_id(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                r1 = self._insert_release(conn, title="r1")
                r2 = self._insert_release(conn, title="r2")
                job_id = self._insert_job(conn, release_id=r1)

                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, r1))
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, r2))
            finally:
                conn.close()

    def test_jobs_release_id_remains_present_and_canonical(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
                self.assertIn("release_id", cols)

                release_id = self._insert_release(conn, title="canon")
                job_id = self._insert_job(conn, release_id=release_id)
                row = conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()
                self.assertEqual(int(row["release_id"]), release_id)
            finally:
                conn.close()

    def test_no_auto_heal_on_inconsistent_state(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="no-heal")
                job_id = self._insert_job(conn, release_id=release_id)
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))
                conn.execute("UPDATE jobs SET state = 'FAILED' WHERE id = ?", (job_id,))

                row = conn.execute("SELECT current_open_job_id FROM releases WHERE id = ?", (release_id,)).fetchone()
                self.assertEqual(int(row["current_open_job_id"]), job_id)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
