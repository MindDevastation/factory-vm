from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from services.planner.materialization_foundation import get_planned_release_by_id, validate_binding_invariants
from tests._helpers import seed_minimal_db, temp_env


class TestPlannerMaterializationBindingSchema(unittest.TestCase):
    def _insert_planned_release(
        self,
        conn,
        *,
        title: str = "Planned",
        publish_at: str = "2026-01-01T00:00:00Z",
    ) -> int:
        cur = conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES('darkwood-reverie', 'LONG', ?, ?, 'notes', 'PLANNED', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (title, publish_at),
        )
        return int(cur.lastrowid)

    def _insert_release(self, conn, *, meta_id: str = "meta-prm-int") -> int:
        channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
        cur = conn.execute(
            """
            INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
            VALUES(?, 'R', 'D', '[]', NULL, NULL, ?, 1.0)
            """,
            (channel_id, meta_id),
        )
        return int(cur.lastrowid)

    def test_migration_adds_materialized_release_id_column(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                cols = {
                    str(r["name"])
                    for r in conn.execute("PRAGMA table_info(planned_releases)").fetchall()
                }
                self.assertIn("materialized_release_id", cols)
            finally:
                conn.close()

    def test_fk_works_for_materialized_release_id(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_release_id = self._insert_planned_release(conn)
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        "UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?",
                        (999999, planned_release_id),
                    )
            finally:
                conn.close()

    def test_partial_unique_index_prevents_double_binding_to_same_release(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn)
                first = self._insert_planned_release(conn, title="one", publish_at="2026-01-01T00:00:00Z")
                second = self._insert_planned_release(conn, title="two", publish_at="2026-01-02T00:00:00Z")
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (release_id, first))
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (release_id, second))
            finally:
                conn.close()

    def test_no_side_effects_on_jobs_or_orchestration(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                before_jobs = int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
                pr_id = self._insert_planned_release(conn)
                planned = get_planned_release_by_id(conn, planned_release_id=pr_id)
                assert planned is not None
                validate_binding_invariants(conn, planned_release=planned)
                after_jobs = int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
                self.assertEqual(before_jobs, after_jobs)
            finally:
                conn.close()

    def test_no_auto_heal_for_inconsistent_binding_state(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                pr_id = self._insert_planned_release(conn)
                release_id = self._insert_release(conn)
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (release_id, pr_id))
                conn.execute("DELETE FROM releases WHERE id = ?", (release_id,))
                conn.execute("PRAGMA foreign_keys=ON")

                planned = get_planned_release_by_id(conn, planned_release_id=pr_id)
                assert planned is not None
                result = validate_binding_invariants(conn, planned_release=planned)
                self.assertEqual(result.invariant_status, "INCONSISTENT")
                current = conn.execute(
                    "SELECT materialized_release_id FROM planned_releases WHERE id = ?",
                    (pr_id,),
                ).fetchone()
                self.assertEqual(int(current["materialized_release_id"]), release_id)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
