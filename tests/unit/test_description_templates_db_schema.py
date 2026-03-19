from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestDescriptionTemplatesDbSchema(unittest.TestCase):
    def test_description_templates_table_and_indexes_exist(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                cols = {str(r["name"]) for r in conn.execute("PRAGMA table_info(description_templates)").fetchall()}
                self.assertTrue(
                    {
                        "id",
                        "channel_slug",
                        "template_name",
                        "template_body",
                        "status",
                        "is_default",
                        "validation_status",
                        "validation_errors_json",
                        "last_validated_at",
                        "created_at",
                        "updated_at",
                        "archived_at",
                    }.issubset(cols)
                )

                idx_rows = conn.execute("PRAGMA index_list(description_templates)").fetchall()
                idx_names = {str(r["name"]) for r in idx_rows}
                self.assertIn("idx_description_templates_channel_slug", idx_names)
                self.assertIn("idx_description_templates_channel_slug_status", idx_names)
                self.assertIn("idx_description_templates_channel_slug_updated_at", idx_names)
                self.assertIn("idx_description_templates_active_default_unique", idx_names)

                uniq_sql_row = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                    ("idx_description_templates_active_default_unique",),
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(uniq_sql_row)
            self.assertIn("WHERE status = 'ACTIVE' AND is_default = 1", str(uniq_sql_row["sql"]))


if __name__ == "__main__":
    unittest.main()
