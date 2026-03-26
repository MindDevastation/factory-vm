from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from services.planner.release_job_creation_service import ReleaseJobCreationError, ReleaseJobCreationService
from tests._helpers import seed_minimal_db, temp_env


class TestReleaseJobCreationService(unittest.TestCase):
    def _insert_release(self, conn, *, title: str, origin_meta_file_id: str | None) -> int:
        row = conn.execute("SELECT id FROM channels WHERE slug = ?", ("darkwood-reverie",)).fetchone()
        self.assertIsNotNone(row)
        return int(
            conn.execute(
                """
                INSERT INTO releases(
                    channel_id, title, description, tags_json, planned_at,
                    origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at
                )
                VALUES(?, ?, 'd', '[]', NULL, NULL, ?, NULL, 1.0)
                """,
                (int(row["id"]), title, origin_meta_file_id),
            ).lastrowid
        )

    def _insert_job(self, conn, *, release_id: int, state: str = "DRAFT") -> int:
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

    def test_release_not_found(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = ReleaseJobCreationService(conn)
                with self.assertRaises(ReleaseJobCreationError) as ctx:
                    svc.create_or_select(release_id=99999)
                self.assertEqual(ctx.exception.code, "PRJ_RELEASE_NOT_FOUND")
            finally:
                conn.close()

    def test_non_materialized_release_not_eligible(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="plain", origin_meta_file_id=None)
                svc = ReleaseJobCreationService(conn)
                with self.assertRaises(ReleaseJobCreationError) as ctx:
                    svc.create_or_select(release_id=release_id)
                self.assertEqual(ctx.exception.code, "PRJ_RELEASE_NOT_ELIGIBLE")
            finally:
                conn.close()

    def test_structural_invalid_release(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="ok", origin_meta_file_id="planned-release-1")
                svc = ReleaseJobCreationService(conn)

                import services.planner.release_job_creation_service as module

                original = module.get_release_by_id
                module.get_release_by_id = lambda _conn, release_id: {"id": release_id, "channel_id": None, "channel_slug": ""}
                self.addCleanup(setattr, module, "get_release_by_id", original)

                with self.assertRaises(ReleaseJobCreationError) as ctx:
                    svc.create_or_select(release_id=release_id)
                self.assertEqual(ctx.exception.code, "PRJ_RELEASE_STATE_INVALID")
            finally:
                conn.close()

    def test_create_new_when_no_open_jobs(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="new", origin_meta_file_id="planned-release-2")
                svc = ReleaseJobCreationService(conn)

                out = svc.create_or_select(release_id=release_id)
                self.assertEqual(out.result, "CREATED_NEW_JOB")
                self.assertEqual(out.job["status"], "DRAFT")
                self.assertEqual(out.job_creation_state_summary["job_creation_state"], "HAS_OPEN_JOB")
                self.assertEqual(out.open_job_diagnostics["invariant_status"], "HAS_OPEN_JOB")

                release_row = conn.execute("SELECT current_open_job_id FROM releases WHERE id = ?", (release_id,)).fetchone()
                self.assertEqual(int(release_row["current_open_job_id"]), int(out.job["id"]))
            finally:
                conn.close()

    def test_return_existing_open_job(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="existing", origin_meta_file_id="planned-release-3")
                job_id = self._insert_job(conn, release_id=release_id, state="DRAFT")
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))
                svc = ReleaseJobCreationService(conn)

                out = svc.create_or_select(release_id=release_id)
                self.assertEqual(out.result, "RETURNED_EXISTING_OPEN_JOB")
                self.assertEqual(int(out.job["id"]), job_id)
            finally:
                conn.close()

    def test_terminal_history_only_allows_create(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="terminal", origin_meta_file_id="planned-release-4")
                self._insert_job(conn, release_id=release_id, state="FAILED")
                svc = ReleaseJobCreationService(conn)
                out = svc.create_or_select(release_id=release_id)
                self.assertEqual(out.result, "CREATED_NEW_JOB")
                self.assertEqual(out.job["status"], "DRAFT")
            finally:
                conn.close()

    def test_invariant_errors_propagate(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="bad", origin_meta_file_id="planned-release-5")
                self._insert_job(conn, release_id=release_id, state="DRAFT")
                self._insert_job(conn, release_id=release_id, state="READY_FOR_RENDER")
                svc = ReleaseJobCreationService(conn)
                with self.assertRaises(ReleaseJobCreationError) as ctx:
                    svc.create_or_select(release_id=release_id)
                self.assertEqual(ctx.exception.code, "PRJ_MULTIPLE_OPEN_JOBS")
            finally:
                conn.close()

    def test_concurrency_recovery_returns_existing(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="race", origin_meta_file_id="planned-release-6")
                job_id = self._insert_job(conn, release_id=release_id, state="DRAFT")
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))
                svc = ReleaseJobCreationService(conn)

                original = svc._create_or_select_in_tx

                def _raise_integrity(*, release_id: int):
                    raise sqlite3.IntegrityError("forced")

                svc._create_or_select_in_tx = _raise_integrity  # type: ignore[method-assign]
                self.addCleanup(setattr, svc, "_create_or_select_in_tx", original)

                out = svc.create_or_select(release_id=release_id)
                self.assertEqual(out.result, "RETURNED_EXISTING_OPEN_JOB")
                self.assertEqual(int(out.job["id"]), job_id)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
