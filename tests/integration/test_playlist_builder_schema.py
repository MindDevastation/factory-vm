from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import temp_env


class PlaylistBuilderSchemaIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._ctx = temp_env()
        _, self.env = self._ctx.__enter__()
        self.conn = dbm.connect(self.env)

    def tearDown(self) -> None:
        self.conn.close()
        self._ctx.__exit__(None, None, None)

    def test_migration_creates_playlist_builder_tables_indexes_and_columns(self) -> None:
        dbm.migrate(self.conn)

        tables = {
            row["name"]
            for row in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        self.assertTrue(
            {
                "playlist_builder_channel_settings",
                "playlist_history",
                "playlist_history_items",
                "playlist_build_previews",
            }.issubset(tables)
        )

        indexes = {
            row["name"]
            for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_autoindex_%'"
            ).fetchall()
        }
        self.assertTrue(
            {
                "idx_playlist_history_channel_stage_created",
                "idx_playlist_history_items_track_pos",
                "idx_playlist_history_items_history_pos",
            }.issubset(indexes)
        )

        ui_job_drafts_columns = dbm._table_columns(self.conn, "ui_job_drafts")
        self.assertIn("playlist_builder_override_json", ui_job_drafts_columns)

        tracks_columns = dbm._table_columns(self.conn, "tracks")
        self.assertIn("month_batch", tracks_columns)

    def test_migration_is_idempotent_for_playlist_builder_schema(self) -> None:
        dbm.migrate(self.conn)
        dbm.migrate(self.conn)

        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='playlist_build_previews'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertIn("expires_at TEXT NOT NULL", row["sql"])

    def test_migration_adds_new_nullable_columns_when_absent(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE ui_job_drafts (
                job_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                tags_csv TEXT NOT NULL,
                cover_name TEXT,
                cover_ext TEXT,
                background_name TEXT NOT NULL,
                background_ext TEXT NOT NULL,
                audio_ids_text TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_slug TEXT NOT NULL,
                track_id TEXT NOT NULL,
                gdrive_file_id TEXT NOT NULL UNIQUE,
                source TEXT,
                filename TEXT,
                title TEXT,
                artist TEXT,
                duration_sec REAL,
                discovered_at REAL NOT NULL,
                analyzed_at REAL
            );
            """
        )

        dbm.migrate(self.conn)

        ui_job_drafts_columns = dbm._table_columns(self.conn, "ui_job_drafts")
        self.assertIn("playlist_builder_override_json", ui_job_drafts_columns)

        tracks_columns = dbm._table_columns(self.conn, "tracks")
        self.assertIn("month_batch", tracks_columns)


if __name__ == "__main__":
    unittest.main()
