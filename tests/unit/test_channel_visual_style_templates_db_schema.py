from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestChannelVisualStyleTemplatesDbSchema(unittest.TestCase):
    def test_table_and_indexes_exist(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                cols = {str(r["name"]) for r in conn.execute("PRAGMA table_info(channel_visual_style_templates)").fetchall()}
                self.assertTrue(
                    {
                        "id",
                        "channel_slug",
                        "template_name",
                        "template_payload_json",
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

                idx_rows = conn.execute("PRAGMA index_list(channel_visual_style_templates)").fetchall()
                idx_names = {str(r["name"]) for r in idx_rows}
                self.assertIn("idx_channel_visual_style_templates_channel_slug", idx_names)
                self.assertIn("idx_channel_visual_style_templates_channel_slug_status", idx_names)
                self.assertIn("idx_channel_visual_style_templates_channel_slug_updated_at", idx_names)
                self.assertIn("idx_channel_visual_style_templates_active_default_unique", idx_names)

                uniq_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                    ("idx_channel_visual_style_templates_active_default_unique",),
                ).fetchone()
            finally:
                conn.close()

            self.assertIsNotNone(uniq_sql)
            self.assertIn("WHERE status = 'ACTIVE' AND is_default = 1", str(uniq_sql["sql"]))

    def test_partial_unique_index_enforces_one_active_default_per_channel(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                now = "2026-03-30T00:00:00+00:00"
                payload_json = dbm.json_dumps(_valid_payload())
                conn.execute(
                    """
                    INSERT INTO channel_visual_style_templates(
                        channel_slug, template_name, template_payload_json, status, is_default,
                        validation_status, validation_errors_json, last_validated_at,
                        created_at, updated_at, archived_at
                    ) VALUES(?, ?, ?, 'ACTIVE', 1, 'VALID', NULL, ?, ?, ?, NULL)
                    """,
                    ("darkwood-reverie", "A", payload_json, now, now, now),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO channel_visual_style_templates(
                            channel_slug, template_name, template_payload_json, status, is_default,
                            validation_status, validation_errors_json, last_validated_at,
                            created_at, updated_at, archived_at
                        ) VALUES(?, ?, ?, 'ACTIVE', 1, 'VALID', NULL, ?, ?, ?, NULL)
                        """,
                        ("darkwood-reverie", "B", payload_json, now, now, now),
                    )
            finally:
                conn.close()


def _valid_payload() -> dict[str, object]:
    return {
        "palette_guidance": "Muted earth tones",
        "typography_rules": "Use clean sans serif titles",
        "text_layout_rules": "Center align title block",
        "composition_framing_rules": "Subject centered with margin",
        "allowed_motifs": ["forest", "fog"],
        "banned_motifs": ["neon"],
        "branding_rules": "Keep logo in lower right",
        "output_profile_guidance": "16:9 high contrast",
        "background_compatibility_guidance": "Works on dark backgrounds",
        "cover_composition_guidance": "Leave top third for text",
    }


if __name__ == "__main__":
    unittest.main()
