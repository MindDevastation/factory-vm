from __future__ import annotations

import sqlite3
import unittest

from tests._helpers import seed_minimal_db, temp_env
from services.prompt_registry.runtime_states import (
    is_allowed_runtime_transition,
    is_runtime_state,
    is_terminal_runtime_state,
)


class TestPromptRegistryRuntimeMf1(unittest.TestCase):
    def test_runtime_tables_exist_and_indexes_smoke(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row

            tables = {
                row["name"]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            self.assertIn("prompt_execution_groups", tables)
            self.assertIn("prompt_execution_attempts", tables)
            self.assertIn("prompt_execution_lifecycle_events", tables)
            self.assertIn("prompt_execution_usage", tables)


            indexes = {
                row["name"]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name LIKE 'prompt_execution_%'").fetchall()
            }
            self.assertIn("idx_prompt_execution_groups_dedup", indexes)
            self.assertIn("idx_prompt_execution_groups_active", indexes)
            self.assertIn("idx_prompt_execution_groups_lease_reclaim", indexes)
            self.assertIn("idx_prompt_execution_attempts_retryable_async", indexes)
            self.assertIn("idx_prompt_execution_lifecycle_events_timeline", indexes)

    def test_runtime_state_domain_and_terminal_validation(self) -> None:
        self.assertTrue(is_runtime_state("PREPARED"))
        self.assertFalse(is_runtime_state("UNKNOWN"))
        self.assertTrue(is_terminal_runtime_state("FAILED_TERMINAL"))
        self.assertFalse(is_terminal_runtime_state("RUNNING"))
        with self.assertRaises(ValueError):
            is_terminal_runtime_state("oops")

    def test_runtime_transition_validation(self) -> None:
        self.assertTrue(is_allowed_runtime_transition("PREPARED", "CONFIRMATION_REQUIRED"))
        self.assertTrue(is_allowed_runtime_transition("DISPATCHED", "RUNNING"))
        self.assertFalse(is_allowed_runtime_transition("PREPARED", "RUNNING"))
        self.assertFalse(is_allowed_runtime_transition("SUCCEEDED", "RUNNING"))
        with self.assertRaises(ValueError):
            is_allowed_runtime_transition("BAD", "RUNNING")
        with self.assertRaises(ValueError):
            is_allowed_runtime_transition("PREPARED", "BAD")


if __name__ == "__main__":
    unittest.main()
