from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestReleaseJobCreationEndpoint(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _insert_release(self, conn, *, title: str, origin_meta_file_id: str | None) -> int:
        row = conn.execute("SELECT id FROM channels WHERE slug = ?", ("darkwood-reverie",)).fetchone()
        assert row is not None
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

    def test_create_then_repeat_returns_existing(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="m1", origin_meta_file_id="planned-release-20")
            finally:
                conn.close()

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            first = client.post(f"/v1/releases/{release_id}/jobs/create-or-select", headers=h)
            self.assertEqual(first.status_code, 200)
            first_body = first.json()
            self.assertEqual(first_body["result"], "CREATED_NEW_JOB")
            self.assertIn("job_creation_state_summary", first_body)
            self.assertIn("open_job_diagnostics", first_body)

            second = client.post(f"/v1/releases/{release_id}/jobs/create-or-select", headers=h)
            self.assertEqual(second.status_code, 200)
            second_body = second.json()
            self.assertEqual(second_body["result"], "RETURNED_EXISTING_OPEN_JOB")
            self.assertEqual(second_body["job"]["id"], first_body["job"]["id"])

            conn = dbm.connect(env)
            try:
                open_ptr = conn.execute("SELECT current_open_job_id FROM releases WHERE id = ?", (release_id,)).fetchone()
                self.assertEqual(int(open_ptr["current_open_job_id"]), int(first_body["job"]["id"]))
                created_job = conn.execute("SELECT job_type, state FROM jobs WHERE id = ?", (int(first_body["job"]["id"]),)).fetchone()
                self.assertEqual(str(created_job["job_type"]), "RELEASE")

                # Explicitly verify no ui_job_drafts/downstream side effects were created.
                draft = conn.execute("SELECT job_id FROM ui_job_drafts WHERE job_id = ?", (int(first_body["job"]["id"]),)).fetchone()
                self.assertIsNone(draft)
                input_rows = conn.execute("SELECT COUNT(*) AS c FROM job_inputs WHERE job_id = ?", (int(first_body["job"]["id"]),)).fetchone()
                output_rows = conn.execute("SELECT COUNT(*) AS c FROM job_outputs WHERE job_id = ?", (int(first_body["job"]["id"]),)).fetchone()
                self.assertEqual(int(input_rows["c"]), 0)
                self.assertEqual(int(output_rows["c"]), 0)

                ui_render_all_selector_row = conn.execute(
                    """
                    SELECT id
                    FROM jobs
                    WHERE job_type='UI' AND state='DRAFT' AND id = ?
                    """,
                    (int(first_body["job"]["id"]),),
                ).fetchone()
                self.assertIsNone(ui_render_all_selector_row)
            finally:
                conn.close()

    def test_non_eligible_release_returns_explicit_error(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="m2", origin_meta_file_id=None)
            finally:
                conn.close()

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/releases/{release_id}/jobs/create-or-select", headers=h)
            self.assertEqual(resp.status_code, 422)
            body = resp.json()
            self.assertEqual(body["result"], "FAILED")
            self.assertEqual(body["error"]["code"], "PRJ_RELEASE_NOT_ELIGIBLE")

    def test_inconsistent_pointer_error_mapping(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="m3", origin_meta_file_id="planned-release-22")
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute("UPDATE releases SET current_open_job_id = 999999 WHERE id = ?", (release_id,))
                conn.execute("PRAGMA foreign_keys=ON")
            finally:
                conn.close()

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/releases/{release_id}/jobs/create-or-select", headers=h)
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "PRJ_OPEN_JOB_NOT_FOUND")

    def test_multiple_open_jobs_error_mapping(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn, title="m4", origin_meta_file_id="planned-release-23")
                self._insert_job(conn, release_id=release_id, state="DRAFT")
                self._insert_job(conn, release_id=release_id, state="READY_FOR_RENDER")
            finally:
                conn.close()

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/releases/{release_id}/jobs/create-or-select", headers=h)
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "PRJ_MULTIPLE_OPEN_JOBS")


if __name__ == "__main__":
    unittest.main()
