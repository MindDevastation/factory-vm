from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from services.common import db as dbm
from services.telegram_operator import TelegramOperatorRegistry
from services.telegram_publish import route_publish_action_via_gateway
from tests._helpers import seed_minimal_db, temp_env, insert_release_and_job


class TestE6AMf3Slice3ManualHandoffIntegration(unittest.TestCase):
    def _enroll(self, conn) -> None:
        reg = TelegramOperatorRegistry(conn)
        reg.start_enrollment(product_operator_id="op-m3b", telegram_user_id=7701, max_permission_class="STANDARD_OPERATOR_MUTATE")
        reg.create_binding(
            product_operator_id="op-m3b",
            telegram_user_id=7701,
            chat_id=-7701,
            thread_id=None,
            chat_binding_kind="PRIVATE_CHAT",
            binding_status="ACTIVE",
        )

    def test_acknowledge_manual_handoff_and_result_continuity(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            conn = dbm.connect(env)
            try:
                self._enroll(conn)
                job_id = int(conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()["id"])
                conn.execute("UPDATE jobs SET publish_state='manual_handoff_pending' WHERE id=?", (job_id,))
                conn.commit()
                out = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=7701,
                    chat_id=-7701,
                    thread_id=None,
                    telegram_action="ack_manual_handoff",
                    job_id=job_id,
                    expected_publish_state="manual_handoff_pending",
                    confirm=True,
                    reason="ack",
                    request_id="req-m3-ack-1",
                    correlation_id="corr-m3-ack-1",
                )
                self.assertEqual(out["status"], "SUCCESS")
                self.assertEqual(out["continuity"]["what_changed"], "manual_handoff_acknowledged")
            finally:
                conn.close()

    def test_confirm_manual_completion_and_conflict_outcomes(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="PUBLISH")
            conn = dbm.connect(env)
            try:
                self._enroll(conn)
                job_id = int(conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()["id"])
                conn.execute("UPDATE jobs SET publish_state='manual_handoff_acknowledged' WHERE id=?", (job_id,))
                conn.commit()
                ts = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                bad = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=7701,
                    chat_id=-7701,
                    thread_id=None,
                    telegram_action="confirm_manual_completion",
                    job_id=job_id,
                    expected_publish_state="manual_handoff_acknowledged",
                    confirm=True,
                    reason="complete",
                    request_id="req-m3-cm-1",
                    correlation_id="corr-m3-cm-1",
                    actual_published_at=ts,
                )
                self.assertEqual(bad["status"], "FAILED")

                ok_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                ok = route_publish_action_via_gateway(
                    conn,
                    telegram_user_id=7701,
                    chat_id=-7701,
                    thread_id=None,
                    telegram_action="confirm_manual_completion",
                    job_id=job_id,
                    expected_publish_state="manual_handoff_acknowledged",
                    confirm=True,
                    reason="complete",
                    request_id="req-m3-cm-2",
                    correlation_id="corr-m3-cm-2",
                    actual_published_at=ok_ts,
                    video_id="yt-7701",
                )
                self.assertEqual(ok["status"], "SUCCESS")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
