from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner.release_job_creation_foundation import (
    ReleaseJobCreationFoundationError,
    build_release_job_create_payload,
    derive_job_creation_state_summary_inputs,
    derive_open_job_diagnostics_inputs,
    get_release_by_id,
    map_job_status_to_category,
    validate_open_job_invariants,
)
from tests._helpers import seed_minimal_db, temp_env


class TestReleaseJobCreationFoundation(unittest.TestCase):
    def _insert_release(self, conn, *, channel_slug: str = "darkwood-reverie", title: str = "r") -> int:
        row = conn.execute("SELECT id FROM channels WHERE slug = ?", (channel_slug,)).fetchone()
        self.assertIsNotNone(row)
        return int(
            conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                VALUES(?, ?, 'd', '[]', NULL, NULL, ?, NULL, 1.0)
                """,
                (int(row["id"]), title, f"meta-{title}"),
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

    def test_status_mapping_open_and_terminal(self) -> None:
        self.assertEqual(map_job_status_to_category("DRAFT"), "OPEN")
        self.assertEqual(map_job_status_to_category("APPROVED"), "OPEN")
        self.assertEqual(map_job_status_to_category("FAILED"), "TERMINAL")
        self.assertEqual(map_job_status_to_category("PUBLISHED"), "TERMINAL")

    def test_status_mapping_unknown_raises_invalid_state(self) -> None:
        with self.assertRaises(ReleaseJobCreationFoundationError) as ctx:
            map_job_status_to_category("BROKEN_STATE")
        self.assertEqual(ctx.exception.code, "PRJ_RELEASE_STATE_INVALID")

    def test_current_pointer_happy_path(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="ok")
                job_id = self._insert_job(conn, release_id=release_id, state="DRAFT")
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None

                diagnostics = validate_open_job_invariants(conn, release=release)
                self.assertEqual(diagnostics.invariant_status, "HAS_OPEN_JOB")
                self.assertEqual(diagnostics.open_jobs_count, 1)
                self.assertEqual(diagnostics.current_open_job_id, job_id)
            finally:
                conn.close()

    def test_missing_current_open_target_detected(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="missing")
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute("UPDATE releases SET current_open_job_id = 9999 WHERE id = ?", (release_id,))
                conn.execute("PRAGMA foreign_keys=ON")
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None

                with self.assertRaises(ReleaseJobCreationFoundationError) as ctx:
                    validate_open_job_invariants(conn, release=release)
                self.assertEqual(ctx.exception.code, "PRJ_OPEN_JOB_NOT_FOUND")
            finally:
                conn.close()

    def test_wrong_release_id_behind_pointer_detected(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="r1")
                other_release_id = self._insert_release(conn, title="r2")
                other_job_id = self._insert_job(conn, release_id=other_release_id, state="DRAFT")
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (other_job_id, release_id))
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None

                with self.assertRaises(ReleaseJobCreationFoundationError) as ctx:
                    validate_open_job_invariants(conn, release=release)
                self.assertEqual(ctx.exception.code, "PRJ_OPEN_JOB_RELATION_INCONSISTENT")
            finally:
                conn.close()

    def test_terminal_current_open_target_invalid(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="term")
                terminal_job_id = self._insert_job(conn, release_id=release_id, state="FAILED")
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (terminal_job_id, release_id))
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None

                with self.assertRaises(ReleaseJobCreationFoundationError) as ctx:
                    validate_open_job_invariants(conn, release=release)
                self.assertEqual(ctx.exception.code, "PRJ_OPEN_JOB_STATUS_INVALID")
            finally:
                conn.close()

    def test_current_open_target_unknown_state_raises_invalid_state(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="invalid-current")
                job_id = self._insert_job(conn, release_id=release_id, state="DRAFT")
                conn.execute("UPDATE jobs SET state = 'BROKEN_STATE' WHERE id = ?", (job_id,))
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None

                with self.assertRaises(ReleaseJobCreationFoundationError) as ctx:
                    validate_open_job_invariants(conn, release=release)
                self.assertEqual(ctx.exception.code, "PRJ_RELEASE_STATE_INVALID")
            finally:
                conn.close()

    def test_multiple_open_jobs_detected(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="many")
                self._insert_job(conn, release_id=release_id, state="DRAFT")
                self._insert_job(conn, release_id=release_id, state="READY_FOR_RENDER")
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None

                with self.assertRaises(ReleaseJobCreationFoundationError) as ctx:
                    validate_open_job_invariants(conn, release=release)
                self.assertEqual(ctx.exception.code, "PRJ_MULTIPLE_OPEN_JOBS")
            finally:
                conn.close()

    def test_one_open_job_and_null_pointer_is_inconsistent(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="null-pointer")
                self._insert_job(conn, release_id=release_id, state="DRAFT")
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None

                with self.assertRaises(ReleaseJobCreationFoundationError) as ctx:
                    validate_open_job_invariants(conn, release=release)
                self.assertEqual(ctx.exception.code, "PRJ_OPEN_JOB_RELATION_INCONSISTENT")
            finally:
                conn.close()

    def test_find_open_jobs_path_unknown_state_raises_invalid_state(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="invalid-open-scan")
                self._insert_job(conn, release_id=release_id, state="DRAFT")
                conn.execute("UPDATE jobs SET state = 'BROKEN_STATE' WHERE release_id = ?", (release_id,))
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None

                with self.assertRaises(ReleaseJobCreationFoundationError) as ctx:
                    validate_open_job_invariants(conn, release=release)
                self.assertEqual(ctx.exception.code, "PRJ_RELEASE_STATE_INVALID")
            finally:
                conn.close()

    def test_payload_builder_uses_only_canonical_existing_fields(self) -> None:
        payload = build_release_job_create_payload(release={"id": 123, "channel_slug": "darkwood-reverie"})
        self.assertEqual(payload["job"]["release_id"], 123)
        self.assertEqual(payload["job"]["state"], "DRAFT")
        self.assertEqual(payload["job"]["stage"], "DRAFT")
        self.assertEqual(payload["context"]["channel_slug"], "darkwood-reverie")

    def test_state_summary_and_diagnostics_derivation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="summary")
                release = get_release_by_id(conn, release_id=release_id)
                assert release is not None
                diagnostics = validate_open_job_invariants(conn, release=release)

                summary = derive_job_creation_state_summary_inputs(release=release, diagnostics=diagnostics, action_enabled=True)
                diag_payload = derive_open_job_diagnostics_inputs(diagnostics=diagnostics)

                self.assertEqual(summary["job_creation_state"], "NO_OPEN_JOB")
                self.assertEqual(diag_payload["release_id"], release_id)
                self.assertEqual(diag_payload["open_jobs_count"], 0)
                self.assertEqual(diag_payload["invariant_status"], "NO_OPEN_JOB")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
