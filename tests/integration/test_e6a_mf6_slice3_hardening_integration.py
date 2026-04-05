from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_inbox import execute_batch_ops_action
from services.telegram_operator import TelegramOperatorRegistry
from services.telegram_publish import route_publish_action_via_gateway
from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestE6AMf6Slice3HardeningIntegration(unittest.TestCase):
    def _enroll(self, conn) -> None:
        reg = TelegramOperatorRegistry(conn)
        reg.start_enrollment(product_operator_id="op-mf6", telegram_user_id=9911, max_permission_class="STANDARD_OPERATOR_MUTATE")
        reg.create_binding(
            product_operator_id="op-mf6",
            telegram_user_id=9911,
            chat_id=-9911,
            thread_id=None,
            chat_binding_kind="PRIVATE_CHAT",
            binding_status="ACTIVE",
        )

    def test_mf3_mutating_flow_is_stale_safe_and_deterministic(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            conn = dbm.connect(env)
            try:
                self._enroll(conn)
                job_id = int(conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()["id"])
                conn.execute("UPDATE jobs SET publish_state='ready_to_publish' WHERE id=?", (job_id,))
                conn.commit()
                first = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=9911,
                    chat_id=-9911,
                    thread_id=None,
                    telegram_action="reject",
                    job_id=job_id,
                    expected_publish_state="ready_to_publish",
                    confirm=True,
                    reason="manual",
                    request_id="mf6-s3-r1",
                    correlation_id="mf6-s3-c1",
                )
                second = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=9911,
                    chat_id=-9911,
                    thread_id=None,
                    telegram_action="reject",
                    job_id=job_id,
                    expected_publish_state="ready_to_publish",
                    confirm=True,
                    reason="manual",
                    request_id="mf6-s3-r1",
                    correlation_id="mf6-s3-c2",
                )
                self.assertEqual(first["status"], "SUCCESS")
                self.assertIn(second["status"], {"STALE", "FAILED", "DENIED"})
            finally:
                conn.close()

    def test_mf5_batch_flow_reports_partial_results(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            insert_release_and_job(env, state="CANCELLED", stage="CANCELLED")
            conn = dbm.connect(env)
            try:
                ids = [int(r["id"]) for r in conn.execute("SELECT id FROM jobs ORDER BY id ASC").fetchall()]
                conn.execute("UPDATE jobs SET publish_state='retry_pending', publish_retry_at=? WHERE id=?", (dbm.now_ts()+60, ids[0]))
                conn.commit()
                out = execute_batch_ops_action(
                    conn,
                    action="retry",
                    selected_job_ids=ids,
                    actor="telegram:9911",
                    confirm=True,
                    reason="batch",
                    request_id="mf6-s3-batch",
                )
                self.assertEqual(out["summary"]["executed_count"], 2)
                self.assertGreaterEqual(out["summary"]["failed_count"], 1)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
