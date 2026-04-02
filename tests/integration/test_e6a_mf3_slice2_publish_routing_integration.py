from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_operator import TelegramOperatorRegistry
from services.telegram_publish import route_publish_action_via_gateway
from tests._helpers import seed_minimal_db, temp_env, insert_release_and_job


class TestE6AMf3Slice2PublishRoutingIntegration(unittest.TestCase):
    def _enroll(self, conn) -> None:
        reg = TelegramOperatorRegistry(conn)
        reg.start_enrollment(product_operator_id="op-m3", telegram_user_id=6601, max_permission_class="STANDARD_OPERATOR_MUTATE")
        reg.create_binding(
            product_operator_id="op-m3",
            telegram_user_id=6601,
            chat_id=-6601,
            thread_id=None,
            chat_binding_kind="PRIVATE_CHAT",
            binding_status="ACTIVE",
        )

    def test_reject_path_uses_canonical_publish_action(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            conn = dbm.connect(env)
            try:
                self._enroll(conn)
                job_id = int(conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()["id"])
                conn.execute("UPDATE jobs SET publish_state='ready_to_publish' WHERE id=?", (job_id,))
                conn.commit()
                out = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=6601,
                    chat_id=-6601,
                    thread_id=None,
                    telegram_action="reject",
                    job_id=job_id,
                    expected_publish_state="ready_to_publish",
                    confirm=True,
                    reason="manual path",
                    request_id="req-m3-reject-1",
                    correlation_id="corr-m3-reject-1",
                )
                self.assertEqual(out["status"], "SUCCESS")
                row = conn.execute("SELECT publish_state FROM jobs WHERE id=?", (job_id,)).fetchone()
                self.assertEqual(str(row["publish_state"]), "manual_handoff_pending")
            finally:
                conn.close()

    def test_permission_denied_and_stale_paths(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            conn = dbm.connect(env)
            try:
                reg = TelegramOperatorRegistry(conn)
                reg.start_enrollment(product_operator_id="op-ro", telegram_user_id=6602, max_permission_class="READ_ONLY")
                reg.create_binding(
                    product_operator_id="op-ro",
                    telegram_user_id=6602,
                    chat_id=-6602,
                    thread_id=None,
                    chat_binding_kind="PRIVATE_CHAT",
                    binding_status="ACTIVE",
                )
                job_id = int(conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()["id"])
                conn.execute("UPDATE jobs SET publish_state='ready_to_publish' WHERE id=?", (job_id,))
                conn.commit()

                denied = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=6602,
                    chat_id=-6602,
                    thread_id=None,
                    telegram_action="reject",
                    job_id=job_id,
                    expected_publish_state="ready_to_publish",
                    confirm=True,
                    reason="manual path",
                    request_id="req-m3-denied-1",
                    correlation_id="corr-m3-denied-1",
                )
                self.assertEqual(denied["status"], "DENIED")
                self.assertEqual(denied["gateway_result"], "DENIED")

                self._enroll(conn)
                stale = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=6601,
                    chat_id=-6601,
                    thread_id=None,
                    telegram_action="reject",
                    job_id=job_id,
                    expected_publish_state="retry_pending",
                    confirm=True,
                    reason="manual path",
                    request_id="req-m3-stale-1",
                    correlation_id="corr-m3-stale-1",
                )
                self.assertEqual(stale["status"], "STALE")
                self.assertEqual(stale["gateway_result"], "STALE")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
