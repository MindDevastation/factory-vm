from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_inbox import build_batch_preview, execute_batch_ops_action
from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestE6AMf5Slice3BatchOpsIntegration(unittest.TestCase):
    def test_batch_preview_and_confirm_with_partial_results(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            insert_release_and_job(env, state="CANCELLED", stage="CANCELLED")
            conn = dbm.connect(env)
            try:
                ids = [int(r["id"]) for r in conn.execute("SELECT id FROM jobs ORDER BY id ASC").fetchall()]
                conn.execute("UPDATE jobs SET publish_state='retry_pending', publish_retry_at=? WHERE id=?", (dbm.now_ts()+60, ids[0]))
                conn.commit()
                preview = build_batch_preview(action="retry", selected_job_ids=ids)
                self.assertEqual(preview["target_count"], 2)

                out = execute_batch_ops_action(
                    conn,
                    action="retry",
                    selected_job_ids=ids,
                    actor="telegram:1",
                    confirm=True,
                    reason="batch retry",
                    request_id="mf5-s3",
                )
                self.assertEqual(out["summary"]["executed_count"], 2)
                self.assertGreaterEqual(out["summary"]["failed_count"], 1)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
