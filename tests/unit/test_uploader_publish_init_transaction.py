from __future__ import annotations

import unittest
from unittest import mock

from services.common import db as dbm
from services.workers.uploader import _initialize_publish_runtime_after_private_upload
from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestUploaderPublishInitTransaction(unittest.TestCase):
    def test_init_publish_runtime_rolls_back_when_resolution_fails(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            conn = dbm.connect(env)
            try:
                before = conn.execute(
                    "SELECT publish_state, publish_last_transition_at FROM jobs WHERE id = ?",
                    (job_id,),
                ).fetchone()
                self.assertIsNone(before["publish_state"])
                self.assertIsNone(before["publish_last_transition_at"])

                with mock.patch("services.workers.uploader._resolve_effective_policy", side_effect=RuntimeError("boom")):
                    with self.assertRaises(RuntimeError):
                        _initialize_publish_runtime_after_private_upload(conn, job_id=job_id)

                after = conn.execute(
                    "SELECT publish_state, publish_last_transition_at FROM jobs WHERE id = ?",
                    (job_id,),
                ).fetchone()
                self.assertIsNone(after["publish_state"])
                self.assertIsNone(after["publish_last_transition_at"])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
