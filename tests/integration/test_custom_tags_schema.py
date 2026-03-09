from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from tests._helpers import temp_env


class CustomTagsSchemaIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._ctx = temp_env()
        _, self.env = self._ctx.__enter__()
        self.conn = dbm.connect(self.env)
        dbm.migrate(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        self._ctx.__exit__(None, None, None)

    def test_migration_creates_custom_tags_tables(self) -> None:
        tables = {
            row["name"]
            for row in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        self.assertTrue(
            {
                "custom_tags",
                "custom_tag_rules",
                "custom_tag_channel_bindings",
                "track_custom_tag_assignments",
            }.issubset(tables)
        )

    def test_unique_category_code_enforced(self) -> None:
        params = ("rain", "Rain", "MOOD", "desc", 1, "2025-01-01", "2025-01-01")
        self.conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            params,
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?)
                """,
                params,
            )

    def test_custom_tags_category_check_rejects_invalid_value(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?)
                """,
                ("mist", "Mist", "INVALID", None, 1, "2025-01-01", "2025-01-01"),
            )

    def test_custom_tag_rules_match_mode_check_rejects_invalid_value(self) -> None:
        cur = self.conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            ("mist", "Mist", "MOOD", None, 1, "2025-01-01", "2025-01-01"),
        )
        tag_id = int(cur.lastrowid)

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT INTO custom_tag_rules(
                    tag_id, source_path, operator, value_json, match_mode, priority, weight,
                    required, stop_after_match, is_active, created_at, updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    tag_id,
                    "features.mood",
                    "EQUALS",
                    '["calm"]',
                    "INVALID",
                    100,
                    None,
                    0,
                    0,
                    1,
                    "2025-01-01",
                    "2025-01-01",
                ),
            )

    def test_track_custom_tag_assignments_state_check_rejects_invalid_value(self) -> None:
        cur = self.conn.execute(
            """
            INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, discovered_at)
            VALUES(?,?,?,?)
            """,
            ("darkwood-reverie", "track-state", "file-state", 1234.0),
        )
        track_pk = int(cur.lastrowid)

        cur = self.conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            ("state", "State", "THEME", None, 1, "2025-01-01", "2025-01-01"),
        )
        tag_id = int(cur.lastrowid)

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?,?,?,?,?)
                """,
                (track_pk, tag_id, "INVALID", "2025-01-01", "2025-01-01"),
            )

    def test_custom_tag_channel_bindings_unique_enforced(self) -> None:
        cur = self.conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            ("channel", "Channel", "VISUAL", None, 1, "2025-01-01", "2025-01-01"),
        )
        tag_id = int(cur.lastrowid)

        params = (tag_id, "darkwood-reverie", "2025-01-01")
        self.conn.execute(
            """
            INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at)
            VALUES(?,?,?)
            """,
            params,
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at)
                VALUES(?,?,?)
                """,
                params,
            )

    def test_custom_tag_indexes_exist(self) -> None:
        indexes = {
            row["name"]
            for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_autoindex_%'"
            ).fetchall()
        }
        self.assertTrue(
            {
                "idx_custom_tags_category",
                "idx_custom_tags_is_active",
                "idx_ctr_tag_id",
                "idx_ctr_priority",
                "idx_ctr_active",
                "idx_ctcb_tag_id",
                "idx_ctcb_channel_slug",
                "idx_tcta_track_pk",
                "idx_tcta_tag_id",
                "idx_tcta_track_state",
            }.issubset(indexes)
        )

    def test_track_pk_fk_and_unique_track_tag_enforced(self) -> None:
        cur = self.conn.execute(
            """
            INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, discovered_at)
            VALUES(?,?,?,?)
            """,
            ("darkwood-reverie", "track-1", "file-1", 1234.0),
        )
        track_pk = int(cur.lastrowid)

        cur = self.conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            ("fog", "Fog", "VISUAL", None, 1, "2025-01-01", "2025-01-01"),
        )
        tag_id = int(cur.lastrowid)

        self.conn.execute(
            """
            INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
            VALUES(?,?,?,?,?)
            """,
            (track_pk, tag_id, "AUTO", "2025-01-01", "2025-01-01"),
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?,?,?,?,?)
                """,
                (track_pk, tag_id, "MANUAL", "2025-01-02", "2025-01-02"),
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?,?,?,?,?)
                """,
                (track_pk + 999, tag_id, "AUTO", "2025-01-03", "2025-01-03"),
            )


if __name__ == "__main__":
    unittest.main()
