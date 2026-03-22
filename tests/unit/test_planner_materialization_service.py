from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from services.planner.materialization_service import PlannerMaterializationError, PlannerMaterializationService
from tests._helpers import seed_minimal_db, temp_env


class TestPlannerMaterializationService(unittest.TestCase):
    def _insert_planner_item(self, conn: sqlite3.Connection, *, status: str = "PLANNED", channel_slug: str = "darkwood-reverie") -> int:
        cur = conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES(?, 'LONG', 'Planned title', '2026-01-01T00:00:00Z', 'seed notes', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (channel_slug, status),
        )
        return int(cur.lastrowid)

    def test_first_materialization_creates_release_link_and_locks_planner(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planner_item_id = self._insert_planner_item(conn)
                svc = PlannerMaterializationService(conn)
                out = svc.materialize_or_get(planner_item_id=planner_item_id, created_by="tester")

                self.assertEqual(out.materialization_status, "CREATED")
                self.assertEqual(out.planner_status, "LOCKED")

                planner = conn.execute("SELECT status FROM planned_releases WHERE id = ?", (planner_item_id,)).fetchone()
                self.assertEqual(planner["status"], "LOCKED")

                link = conn.execute(
                    "SELECT planned_release_id, release_id FROM planner_release_links WHERE planned_release_id = ?",
                    (planner_item_id,),
                ).fetchone()
                self.assertIsNotNone(link)
                self.assertEqual(int(link["release_id"]), out.release_id)
            finally:
                conn.close()

    def test_repeated_materialization_is_idempotent(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planner_item_id = self._insert_planner_item(conn)
                svc = PlannerMaterializationService(conn)
                first = svc.materialize_or_get(planner_item_id=planner_item_id, created_by="tester")
                second = svc.materialize_or_get(planner_item_id=planner_item_id, created_by="tester")

                self.assertEqual(second.materialization_status, "EXISTING_BINDING")
                self.assertEqual(second.release_id, first.release_id)

                release_count = conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"]
                link_count = conn.execute("SELECT COUNT(*) AS c FROM planner_release_links").fetchone()["c"]
                self.assertEqual(int(release_count), 1)
                self.assertEqual(int(link_count), 1)
            finally:
                conn.close()

    def test_invalid_status_rejected(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planner_item_id = self._insert_planner_item(conn, status="FAILED")
                svc = PlannerMaterializationService(conn)
                with self.assertRaises(PlannerMaterializationError) as ctx:
                    svc.materialize_or_get(planner_item_id=planner_item_id, created_by="tester")
                self.assertEqual(ctx.exception.code, "PLM_INVALID_STATUS")
            finally:
                conn.close()

    def test_locked_without_link_is_inconsistent(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planner_item_id = self._insert_planner_item(conn, status="LOCKED")
                svc = PlannerMaterializationService(conn)
                with self.assertRaises(PlannerMaterializationError) as ctx:
                    svc.materialize_or_get(planner_item_id=planner_item_id, created_by="tester")
                self.assertEqual(ctx.exception.code, "PLM_INCONSISTENT_STATE")
            finally:
                conn.close()

    def test_existing_link_with_missing_release_is_inconsistent(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planner_item_id = self._insert_planner_item(conn, status="LOCKED")
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                release_cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'tmp', 'tmp', '[]', NULL, NULL, 'tmp-missing-release-link', 1.0)
                    """,
                    (channel_id,),
                )
                release_id = int(release_cur.lastrowid)
                conn.execute(
                    """
                    INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by)
                    VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')
                    """,
                    (planner_item_id, release_id),
                )
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute("DELETE FROM releases WHERE id = ?", (release_id,))
                conn.execute("PRAGMA foreign_keys=ON")

                svc = PlannerMaterializationService(conn)
                with self.assertRaises(PlannerMaterializationError) as ctx:
                    svc.materialize_or_get(planner_item_id=planner_item_id, created_by="tester")
                self.assertEqual(ctx.exception.code, "PLM_INCONSISTENT_STATE")
            finally:
                conn.close()

    def test_rollback_atomicity_when_failure_occurs_after_release_insert(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planner_item_id = self._insert_planner_item(conn)
                svc = PlannerMaterializationService(conn)

                original_now_iso = svc._now_iso
                try:
                    svc._now_iso = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
                    with self.assertRaises(PlannerMaterializationError) as ctx:
                        svc.materialize_or_get(planner_item_id=planner_item_id, created_by="tester")
                    self.assertEqual(ctx.exception.code, "PLM_INTERNAL")
                finally:
                    svc._now_iso = original_now_iso

                releases = conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"]
                links = conn.execute("SELECT COUNT(*) AS c FROM planner_release_links").fetchone()["c"]
                status = conn.execute("SELECT status FROM planned_releases WHERE id = ?", (planner_item_id,)).fetchone()["status"]
                self.assertEqual(int(releases), 0)
                self.assertEqual(int(links), 0)
                self.assertEqual(status, "PLANNED")
            finally:
                conn.close()
