from __future__ import annotations

import unittest
from unittest.mock import patch

from services.common import db as dbm
from services.telegram_inbox.ops_controls import execute_single_ops_action
from services.telegram_inbox.read_views import build_and_persist_read_view
from services.telegram_operator.core import TelegramOperatorRegistry
from services.telegram_publish.actions import route_publish_action_via_gateway
from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestE6ARuntimePersistenceUsageIntegration(unittest.TestCase):
    def test_publish_read_ops_paths_persist_required_records(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                conn.execute("UPDATE jobs SET publish_state = 'policy_blocked' WHERE id = ?", (int(job_id),))
                registry = TelegramOperatorRegistry(conn)
                registry.start_enrollment(product_operator_id="op-1", telegram_user_id=101, max_permission_class="STANDARD_OPERATOR_MUTATE")
                registry.create_binding(
                    product_operator_id="op-1",
                    telegram_user_id=101,
                    chat_id=-5001,
                    thread_id=None,
                    chat_binding_kind="PRIVATE_CHAT",
                    binding_status="ACTIVE",
                )

                with patch("services.telegram_publish.actions.execute_publish_job_action") as publish_action:
                    publish_action.return_value = {"ok": True, "result": {"publish_state_after": "ready_to_publish"}}
                    route_publish_action_via_gateway(
                        conn,
                        telegram_user_id=101,
                        chat_id=-5001,
                        thread_id=None,
                        telegram_action="approve",
                        job_id=job_id,
                        expected_publish_state="policy_blocked",
                        confirm=True,
                        reason="approve via test",
                        request_id="req-pub-1",
                        correlation_id="corr-pub-1",
                    )
                    route_publish_action_via_gateway(
                        conn,
                        telegram_user_id=101,
                        chat_id=-5001,
                        thread_id=None,
                        telegram_action="approve",
                        job_id=job_id,
                        expected_publish_state="retry_pending",
                        confirm=True,
                        reason="stale via test",
                        request_id="req-pub-2",
                        correlation_id="corr-pub-2",
                    )

                rows = [{"job_id": job_id, "publish_state": "policy_blocked"}]
                build_and_persist_read_view(
                    conn,
                    product_operator_id="op-1",
                    telegram_user_id=101,
                    view_name="factory_overview",
                    rows=rows,
                    generated_at="2026-04-03T00:00:00Z",
                )

                with patch("services.telegram_inbox.ops_controls.execute_publish_job_action") as ops_action:
                    ops_action.return_value = {"ok": True, "result": {"publish_state_after": "retry_pending"}}
                    execute_single_ops_action(
                        conn,
                        job_id=job_id,
                        action="retry",
                        actor="telegram:101",
                        confirm=True,
                        reason="retry via test",
                        request_id="req-ops-1",
                    )

                expected_non_empty = {
                    "telegram_publish_action_contexts",
                    "telegram_publish_action_results",
                    "telegram_read_view_snapshots",
                    "telegram_read_view_access_events",
                    "telegram_ops_action_contexts",
                    "telegram_ops_action_confirmations",
                    "telegram_ops_action_results",
                    "telegram_action_audit_records",
                    "telegram_action_idempotency_records",
                    "telegram_action_safety_events",
                }
                for table in expected_non_empty:
                    count = int(conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"])
                    self.assertGreater(count, 0, table)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
