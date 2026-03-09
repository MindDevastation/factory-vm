from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, temp_env


class TestCustomTagsReportIntegration(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _seed_channel_and_track(self, env) -> int:
        conn = dbm.connect(env)
        try:
            dbm.migrate(conn)
            conn.execute(
                """
                INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled)
                VALUES(?, ?, 'LONG', 1.0, 'long_1080p24', 0)
                """,
                ("darkwood-reverie", "Darkwood Reverie"),
            )
            conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))
            conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
            return int(
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?, ?, ?, 'gdrive', ?, ?, ?, 180.0, 1000.0, 1005.0)
                    """,
                    ("darkwood-reverie", "001", "file-001", "001.wav", "Title 001", "Artist X"),
                ).lastrowid
            )
        finally:
            conn.close()

    def _seed_custom_tags(self, env, track_pk: int) -> None:
        conn = dbm.connect(env)
        try:
            visual_a = int(
                conn.execute(
                    """
                    INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                    VALUES(?, ?, 'VISUAL', ?, 1, ?, ?)
                    """,
                    ("forest-a", "A Forest", "", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
                ).lastrowid
            )
            visual_z = int(
                conn.execute(
                    """
                    INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                    VALUES(?, ?, 'VISUAL', ?, 1, ?, ?)
                    """,
                    ("forest-z", "Z Forest", "", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
                ).lastrowid
            )
            mood = int(
                conn.execute(
                    """
                    INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                    VALUES(?, ?, 'MOOD', ?, 1, ?, ?)
                    """,
                    ("calm", "Calm", "", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
                ).lastrowid
            )
            suppressed_visual = int(
                conn.execute(
                    """
                    INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                    VALUES(?, ?, 'VISUAL', ?, 1, ?, ?)
                    """,
                    ("hidden-fog", "Hidden Fog", "", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
                ).lastrowid
            )
            theme = int(
                conn.execute(
                    """
                    INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                    VALUES(?, ?, 'THEME', ?, 1, ?, ?)
                    """,
                    ("night", "Night", "", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
                ).lastrowid
            )

            conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?, ?, 'AUTO', ?, ?)
                """,
                (track_pk, visual_z, "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?, ?, 'MANUAL', ?, ?)
                """,
                (track_pk, visual_a, "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?, ?, 'AUTO', ?, ?)
                """,
                (track_pk, mood, "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?, ?, 'AUTO', ?, ?)
                """,
                (track_pk, theme, "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?, ?, 'SUPPRESSED', ?, ?)
                """,
                (track_pk, suppressed_visual, "2024-01-02T00:00:00Z", "2024-01-02T00:00:00Z"),
            )
        finally:
            conn.close()

    def test_analysis_report_includes_effective_custom_tag_columns(self) -> None:
        with temp_env() as (_, env):
            track_pk = self._seed_channel_and_track(env)
            self._seed_custom_tags(env, track_pk)
            _, client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(
                "/v1/track-catalog/analysis-report",
                params={"channel_slug": "darkwood-reverie"},
                headers=auth,
            )

            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            keys = [col["key"] for col in payload["columns"]]
            self.assertIn("custom_tags_visual", keys)
            self.assertIn("custom_tags_mood", keys)
            self.assertIn("custom_tags_theme", keys)

            row = payload["rows"][0]
            self.assertEqual(row["custom_tags_visual"], "A Forest, Z Forest")
            self.assertEqual(row["custom_tags_mood"], "Calm")
            self.assertEqual(row["custom_tags_theme"], "Night")


if __name__ == "__main__":
    unittest.main()
