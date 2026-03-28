from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env

PUBLISH_RUNTIME_COLUMNS: tuple[str, ...] = (
    "publish_state",
    "publish_target_visibility",
    "publish_delivery_mode_effective",
    "publish_resolved_scope",
    "publish_reason_code",
    "publish_reason_detail",
    "publish_scheduled_at",
    "publish_attempt_count",
    "publish_retry_at",
    "publish_last_error_code",
    "publish_last_error_message",
    "publish_in_progress_at",
    "publish_last_transition_at",
    "publish_hold_active",
    "publish_hold_reason_code",
    "publish_manual_ack_at",
    "publish_manual_completed_at",
    "publish_manual_published_at",
    "publish_manual_video_id",
    "publish_manual_url",
    "publish_drift_detected_at",
    "publish_observed_visibility",
)

PUBLISH_RUNTIME_INDEXES: tuple[str, ...] = (
    "idx_jobs_publish_runtime_state_id",
    "idx_jobs_publish_runtime_retry_due",
)


class TestPublishRuntimeJobsSchemaMigration(unittest.TestCase):
    def test_publish_runtime_columns_and_indexes_exist(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                column_names = {str(row["name"]) for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
                for column in PUBLISH_RUNTIME_COLUMNS:
                    self.assertIn(column, column_names)

                index_names = {str(row["name"]) for row in conn.execute("PRAGMA index_list(jobs)").fetchall()}
                for index_name in PUBLISH_RUNTIME_INDEXES:
                    self.assertIn(index_name, index_names)
            finally:
                conn.close()

    def test_migration_is_idempotent_with_publish_runtime_schema(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                dbm.migrate(conn)
                index_names = {str(row["name"]) for row in conn.execute("PRAGMA index_list(jobs)").fetchall()}
                self.assertIn("idx_jobs_publish_runtime_state_id", index_names)
                self.assertIn("idx_jobs_publish_runtime_retry_due", index_names)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
