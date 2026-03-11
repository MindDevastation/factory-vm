from __future__ import annotations

import json
import unittest

from services.common import db as dbm
from services.custom_tags import reassign_service
from tests._helpers import seed_minimal_db, temp_env


class TestCustomTagsRulePreview(unittest.TestCase):
    def _insert_track_with_analysis(self, conn, *, track_id: str, voice_flag: bool) -> int:
        cur = conn.execute(
            """
            INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            ("darkwood-reverie", track_id, f"gid-{track_id}", "gdrive", "f.wav", "title", "artist", 11.0, 1000.0, 1001.0),
        )
        track_pk = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?,?,?)",
            (track_pk, json.dumps({"voice_flag": voice_flag}), 1001.0),
        )
        conn.execute(
            "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?,?,?)",
            (track_pk, json.dumps({"yamnet_tags": ["Music"]}), 1001.0),
        )
        conn.execute(
            "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?,?,?)",
            (track_pk, json.dumps({"energy": 0.8}), 1001.0),
        )
        return track_pk

    def test_preview_rule_matches_returns_count_sample_and_summary(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    ("cyber_arena", "Cyber Arena", "MOOD", None, 1, "2025-01-01", "2025-01-01"),
                )
                self._insert_track_with_analysis(conn, track_id="t1", voice_flag=False)
                self._insert_track_with_analysis(conn, track_id="t2", voice_flag=True)

                result = reassign_service.preview_rule_matches(
                    conn,
                    tag_code="cyber_arena",
                    rule={
                        "source_path": "track_features.payload_json.voice_flag",
                        "operator": "equals",
                        "value_json": "false",
                        "match_mode": "ALL",
                        "priority": 100,
                        "required": False,
                        "stop_after_match": False,
                        "is_active": True,
                    },
                    channel_slug=None,
                )

                self.assertEqual(result["matched_tracks_count"], 1)
                self.assertEqual(len(result["sample_track_ids"]), 1)
                self.assertEqual(result["summary"], "1 analyzed tracks would match")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
