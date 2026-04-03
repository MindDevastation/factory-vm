from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_operator import TelegramOperatorRegistry
from services.telegram_publish import route_publish_action_via_gateway
from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestE6AMf3Slice4Hardening(unittest.TestCase):
    def _enroll(self, conn, *, user_id: int = 8801) -> None:
        reg = TelegramOperatorRegistry(conn)
        reg.start_enrollment(product_operator_id="op-m3-hard", telegram_user_id=user_id, max_permission_class="STANDARD_OPERATOR_MUTATE")
        reg.create_binding(
            product_operator_id="op-m3-hard",
            telegram_user_id=user_id,
            chat_id=-8801,
            thread_id=None,
            chat_binding_kind="PRIVATE_CHAT",
            binding_status="ACTIVE",
        )

    def test_publish_action_attempts_are_audited_and_gateway_logged(self) -> None:
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
                    telegram_user_id=8801,
                    chat_id=-8801,
                    thread_id=None,
                    telegram_action="reject",
                    job_id=job_id,
                    expected_publish_state="ready_to_publish",
                    confirm=True,
                    reason="handoff",
                    request_id="req-m3-hard-1",
                    correlation_id="corr-m3-hard-1",
                )
                self.assertEqual(out["status"], "SUCCESS")
                gw = conn.execute("SELECT gateway_result FROM telegram_action_gateway_events WHERE correlation_id=?", ("corr-m3-hard-1",)).fetchone()
                self.assertEqual(str(gw["gateway_result"]), "ALLOWED")
                audit = conn.execute("SELECT event_type FROM telegram_operator_audit_events WHERE correlation_id=?", ("corr-m3-hard-1",)).fetchone()
                self.assertIsNotNone(audit)
            finally:
                conn.close()

    def test_stale_and_confirmation_denials_are_safe(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            conn = dbm.connect(env)
            try:
                self._enroll(conn)
                job_id = int(conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()["id"])
                conn.execute("UPDATE jobs SET publish_state='ready_to_publish' WHERE id=?", (job_id,))
                conn.commit()

                stale = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=8801,
                    chat_id=-8801,
                    thread_id=None,
                    telegram_action="reject",
                    job_id=job_id,
                    expected_publish_state="retry_pending",
                    confirm=True,
                    reason="handoff",
                    request_id="req-m3-hard-2",
                    correlation_id="corr-m3-hard-2",
                )
                self.assertEqual(stale["status"], "STALE")

                with self.assertRaises(ValueError):
                    route_publish_action_via_gateway(
                        conn,
                        telegram_user_id=8801,
                        chat_id=-8801,
                        thread_id=None,
                        telegram_action="reject",
                        job_id=job_id,
                        expected_publish_state="ready_to_publish",
                        confirm=False,
                        reason="handoff",
                        request_id="req-m3-hard-3",
                        correlation_id="corr-m3-hard-3",
                    )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
