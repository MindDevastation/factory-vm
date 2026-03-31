from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import temp_env


class TestVisualFoundationSchema(unittest.TestCase):
    def test_migrate_creates_visual_foundation_tables_columns_and_indexes(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                tables = {
                    str(row["name"])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                self.assertIn("release_visual_configs", tables)
                self.assertIn("release_visual_preview_snapshots", tables)
                self.assertIn("release_visual_approved_previews", tables)
                self.assertIn("release_visual_approved_previews_scoped", tables)
                self.assertIn("release_visual_applied_packages", tables)
                self.assertIn("release_visual_cover_selection_inputs", tables)
                self.assertIn("release_visual_cover_candidates", tables)
                self.assertIn("release_visual_cover_selected_candidates", tables)
                self.assertNotIn("release_visual_thumbnails", tables)

                configs_cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(release_visual_configs)").fetchall()}
                self.assertEqual(
                    configs_cols,
                    {"release_id", "intent_config_json", "created_at", "updated_at"},
                )
                # release_visual_configs is retained only as a legacy compatibility table.
                # Active Epic-5 operational state is persisted in snapshot/approved/applied tables.
                for table_name in (
                    "release_visual_preview_snapshots",
                    "release_visual_approved_previews_scoped",
                    "release_visual_applied_packages",
                ):
                    create_sql_row = conn.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                        (table_name,),
                    ).fetchone()
                    assert create_sql_row is not None
                    self.assertNotIn("release_visual_configs", str(create_sql_row["sql"]))

                snapshot_cols = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(release_visual_preview_snapshots)").fetchall()
                }
                self.assertEqual(
                    snapshot_cols,
                    {
                        "id",
                        "release_id",
                        "preview_scope",
                        "intent_snapshot_json",
                        "preview_package_json",
                        "created_by",
                        "created_at",
                    },
                )
                snapshot_indexes = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA index_list(release_visual_preview_snapshots)").fetchall()
                }
                self.assertIn("idx_release_visual_preview_snapshots_release_created", snapshot_indexes)

                approved_cols = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(release_visual_approved_previews)").fetchall()
                }
                self.assertEqual(approved_cols, {"release_id", "preview_id", "approved_by", "approved_at"})

                scoped_approved_cols = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(release_visual_approved_previews_scoped)").fetchall()
                }
                self.assertEqual(
                    scoped_approved_cols,
                    {"release_id", "preview_scope", "preview_id", "approved_by", "approved_at"},
                )

                applied_cols = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(release_visual_applied_packages)").fetchall()
                }
                self.assertEqual(
                    applied_cols,
                    {
                        "release_id",
                        "background_asset_id",
                        "cover_asset_id",
                        "source_preview_id",
                        "applied_by",
                        "applied_at",
                    },
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
