from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class TestJobsListRetryFields(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_env_ctx = temp_env()
        self._td, self.env = self._temp_env_ctx.__enter__()
        seed_minimal_db(self.env)
        self.conn = dbm.connect(self.env)
        mod = importlib.import_module("services.factory_api.app")
        self.app_mod = importlib.reload(mod)
        self.client = TestClient(self.app_mod.app)

    def tearDown(self) -> None:
        self.conn.close()
        self._temp_env_ctx.__exit__(None, None, None)

    def _list_jobs(self) -> list[dict]:
        response = self.client.get(
            "/v1/jobs",
            headers=basic_auth_header(self.env.basic_user, self.env.basic_pass),
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("jobs", body)
        return body["jobs"]

    def _job_row(self, job_id: int) -> dict:
        for row in self._list_jobs():
            if int(row["id"]) == job_id:
                return row
        self.fail(f"job {job_id} not found")

    def _insert_job(self, state: str, *, retry_of_job_id: int | None = None) -> int:
        job_id = insert_release_and_job(self.env, state=state, stage="RENDER", job_type="UI")
        if retry_of_job_id is not None:
            source = self.conn.execute("SELECT root_job_id, attempt_no FROM jobs WHERE id = ?", (retry_of_job_id,)).fetchone()
            assert source is not None
            self.conn.execute(
                """
                UPDATE jobs
                SET retry_of_job_id = ?, root_job_id = ?, attempt_no = ?
                WHERE id = ?
                """,
                (retry_of_job_id, int(source["root_job_id"]), int(source["attempt_no"]) + 1, job_id),
            )
        return job_id

    def test_failed_without_retry_child_allows_retry(self) -> None:
        failed_job_id = self._insert_job("FAILED")

        row = self._job_row(failed_job_id)

        self.assertEqual(row["id"], failed_job_id)
        self.assertEqual(row["status"], "FAILED")
        self.assertEqual(int(row["attempt_no"]), 1)
        self.assertIsNone(row["retry_child_job_id"])
        self.assertTrue(row["actions"]["retry_allowed"])

    def test_failed_with_retry_child_disables_retry_and_sets_child(self) -> None:
        failed_job_id = self._insert_job("FAILED")
        child_job_id = self._insert_job("DRAFT", retry_of_job_id=failed_job_id)

        row = self._job_row(failed_job_id)

        self.assertEqual(row["status"], "FAILED")
        self.assertEqual(row["retry_child_job_id"], child_job_id)
        self.assertFalse(row["actions"]["retry_allowed"])

    def test_non_failed_disables_retry(self) -> None:
        running_job_id = self._insert_job("RUNNING")

        row = self._job_row(running_job_id)

        self.assertEqual(row["status"], "RUNNING")
        self.assertFalse(row["actions"]["retry_allowed"])


if __name__ == "__main__":
    unittest.main()
