from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import temp_env


class TestEpic5SchemaProvisioning(unittest.TestCase):
    def test_migrate_materializes_epic5_tables_from_empty_db(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                before = {
                    str(r["name"])
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                self.assertNotIn("release_visual_preview_snapshots", before)
                self.assertNotIn("release_visual_approved_previews_scoped", before)
                self.assertNotIn("release_visual_applied_packages", before)

                dbm.migrate(conn)

                after = {
                    str(r["name"])
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                self.assertIn("release_visual_preview_snapshots", after)
                self.assertIn("release_visual_approved_previews_scoped", after)
                self.assertIn("release_visual_applied_packages", after)
            finally:
                conn.close()

    def test_migrate_is_additive_and_idempotent_for_epic5_tables(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                first = {
                    str(r["name"])
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }

                dbm.migrate(conn)
                second = {
                    str(r["name"])
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }

                for table in (
                    "release_visual_configs",
                    "release_visual_preview_snapshots",
                    "release_visual_approved_previews_scoped",
                    "release_visual_applied_packages",
                    "release_visual_history_events",
                    "release_visual_batch_preview_sessions",
                ):
                    self.assertIn(table, first)
                    self.assertIn(table, second)
                self.assertTrue(first.issubset(second))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
