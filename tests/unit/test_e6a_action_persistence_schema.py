from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import temp_env


class TestE6AActionPersistenceSchema(unittest.TestCase):
    def test_migrate_creates_required_action_persistence_tables_and_indexes(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                tables = {str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertTrue(
                    {
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
                    }.issubset(tables)
                )

                indexes = {str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
                self.assertTrue(
                    {
                        "idx_telegram_publish_action_contexts_target",
                        "idx_telegram_publish_action_results_request",
                        "idx_telegram_read_view_snapshots_operator_view",
                        "idx_telegram_read_view_access_events_operator_time",
                        "idx_telegram_ops_action_contexts_operator_type",
                        "idx_telegram_ops_action_confirmations_action",
                        "idx_telegram_ops_action_results_action",
                        "idx_telegram_action_audit_records_record_time",
                        "idx_telegram_action_idempotency_records_last_seen",
                        "idx_telegram_action_safety_events_type_time",
                    }.issubset(indexes)
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
