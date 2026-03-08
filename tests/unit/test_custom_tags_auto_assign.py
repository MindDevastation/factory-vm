from __future__ import annotations

import json
import unittest

from services.common import db as dbm
from services.custom_tags.auto_assign import apply_auto_custom_tags
from tests._helpers import seed_minimal_db, temp_env


class TestCustomTagsAutoAssign(unittest.TestCase):
    def _insert_track(self, conn, channel_slug: str = "darkwood-reverie") -> int:
        cur = conn.execute(
            """
            INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (channel_slug, "trk-1", "gid-1", "gdrive", "f.wav", "title", "artist", 11.0, 1000.0, None),
        )
        return int(cur.lastrowid)

    def _insert_tag(self, conn, *, code: str, category: str) -> int:
        cur = conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (code, code.title(), category, None, 1, "2025-01-01", "2025-01-01"),
        )
        return int(cur.lastrowid)

    def _insert_rule(self, conn, *, tag_id: int, source_path: str, operator: str, value: object) -> int:
        cur = conn.execute(
            """
            INSERT INTO custom_tag_rules(
                tag_id, source_path, operator, value_json, match_mode,
                priority, weight, required, stop_after_match, is_active, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (tag_id, source_path, operator, json.dumps(value), "ALL", 100, None, 0, 0, 1, "2025-01-01", "2025-01-01"),
        )
        return int(cur.lastrowid)

    def test_empty_catalog_or_rules_is_safe_no_op(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                track_pk = self._insert_track(conn)

                result_no_catalog = apply_auto_custom_tags(conn, track_pk, analyzer_payload={})
                self.assertEqual(result_no_catalog["auto_added"], [])
                self.assertEqual(result_no_catalog["auto_removed"], [])
                self.assertEqual(result_no_catalog["preserved_manual"], [])
                self.assertEqual(result_no_catalog["suppressed_skipped"], [])

                tag_id = self._insert_tag(conn, code="calm", category="MOOD")
                result_no_rules = apply_auto_custom_tags(conn, track_pk, analyzer_payload={})
                self.assertEqual(result_no_rules["auto_added"], [])

                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM track_custom_tag_assignments WHERE track_pk = ? AND tag_id = ?",
                    (track_pk, tag_id),
                ).fetchone()
                self.assertEqual(int(row["c"]), 0)
            finally:
                conn.close()

    def test_no_active_rules_keeps_existing_auto_untouched(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                track_pk = self._insert_track(conn)
                tag_id = self._insert_tag(conn, code="calm", category="MOOD")
                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, tag_id, "AUTO", "2025-01-01", "2025-01-01"),
                )

                before = conn.execute(
                    "SELECT state, assigned_at, updated_at FROM track_custom_tag_assignments WHERE track_pk = ? AND tag_id = ?",
                    (track_pk, tag_id),
                ).fetchone()

                result = apply_auto_custom_tags(
                    conn,
                    track_pk,
                    analyzer_payload={"track_features": {"payload_json": {"voice_flag": True}}},
                )

                self.assertEqual(result["auto_added"], [])
                self.assertEqual(result["auto_removed"], [])
                self.assertEqual(result["preserved_manual"], [])
                self.assertEqual(result["suppressed_skipped"], [])

                after = conn.execute(
                    "SELECT state, assigned_at, updated_at FROM track_custom_tag_assignments WHERE track_pk = ? AND tag_id = ?",
                    (track_pk, tag_id),
                ).fetchone()
                self.assertIsNotNone(after)
                self.assertEqual(str(after["state"]), "AUTO")
                self.assertEqual(str(after["assigned_at"]), str(before["assigned_at"]))
                self.assertEqual(str(after["updated_at"]), str(before["updated_at"]))
            finally:
                conn.close()

    def test_candidate_creates_auto_and_stale_auto_removed(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                track_pk = self._insert_track(conn)
                add_tag = self._insert_tag(conn, code="night", category="MOOD")
                stale_tag = self._insert_tag(conn, code="day", category="THEME")

                self._insert_rule(
                    conn,
                    tag_id=add_tag,
                    source_path="track_scores.payload_json.energy",
                    operator="gte",
                    value=0.8,
                )
                self._insert_rule(
                    conn,
                    tag_id=stale_tag,
                    source_path="track_features.payload_json.voice_flag",
                    operator="equals",
                    value=True,
                )

                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, stale_tag, "AUTO", "2025-01-01", "2025-01-01"),
                )

                result = apply_auto_custom_tags(
                    conn,
                    track_pk,
                    analyzer_payload={
                        "track_features": {"payload_json": {"voice_flag": False}},
                        "track_scores": {"payload_json": {"energy": 0.9}},
                    },
                )

                self.assertEqual(result["auto_added"], [add_tag])
                self.assertEqual(result["auto_removed"], [stale_tag])

                states = {
                    int(r["tag_id"]): str(r["state"])
                    for r in conn.execute(
                        "SELECT tag_id, state FROM track_custom_tag_assignments WHERE track_pk = ?",
                        (track_pk,),
                    ).fetchall()
                }
                self.assertEqual(states, {add_tag: "AUTO"})
            finally:
                conn.close()

    def test_manual_and_suppressed_are_preserved(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                track_pk = self._insert_track(conn)
                manual_tag = self._insert_tag(conn, code="echo", category="MOOD")
                suppressed_tag = self._insert_tag(conn, code="grain", category="THEME")

                self._insert_rule(
                    conn,
                    tag_id=manual_tag,
                    source_path="track_features.payload_json.voice_flag",
                    operator="equals",
                    value=True,
                )
                self._insert_rule(
                    conn,
                    tag_id=suppressed_tag,
                    source_path="track_tags.payload_json.palette",
                    operator="contains",
                    value="dark",
                )

                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, manual_tag, "MANUAL", "2025-01-01", "2025-01-01"),
                )
                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, suppressed_tag, "SUPPRESSED", "2025-01-01", "2025-01-01"),
                )

                result = apply_auto_custom_tags(
                    conn,
                    track_pk,
                    analyzer_payload={
                        "track_features": {"payload_json": {"voice_flag": True}},
                        "track_tags": {"payload_json": {"palette": "dark ambient"}},
                    },
                )

                self.assertEqual(result["auto_added"], [])
                self.assertEqual(result["auto_removed"], [])
                self.assertEqual(result["preserved_manual"], [manual_tag])
                self.assertEqual(result["suppressed_skipped"], [suppressed_tag])

                states = {
                    int(r["tag_id"]): str(r["state"])
                    for r in conn.execute(
                        "SELECT tag_id, state FROM track_custom_tag_assignments WHERE track_pk = ? ORDER BY tag_id",
                        (track_pk,),
                    ).fetchall()
                }
                self.assertEqual(states[manual_tag], "MANUAL")
                self.assertEqual(states[suppressed_tag], "SUPPRESSED")
            finally:
                conn.close()

    def test_visual_channel_binding_gates_candidates(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                track_pk = self._insert_track(conn, channel_slug="darkwood-reverie")
                visual_tag = self._insert_tag(conn, code="nebula", category="VISUAL")
                self._insert_rule(
                    conn,
                    tag_id=visual_tag,
                    source_path="track_features.payload_json.scene",
                    operator="equals",
                    value="space",
                )

                result_without_binding = apply_auto_custom_tags(
                    conn,
                    track_pk,
                    analyzer_payload={"track_features": {"payload_json": {"scene": "space"}}},
                )
                self.assertEqual(result_without_binding["auto_added"], [])

                conn.execute(
                    "INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at) VALUES(?,?,?)",
                    (visual_tag, "darkwood-reverie", "2025-01-01"),
                )
                result_with_binding = apply_auto_custom_tags(
                    conn,
                    track_pk,
                    analyzer_payload={"track_features": {"payload_json": {"scene": "space"}}},
                )
                self.assertEqual(result_with_binding["auto_added"], [visual_tag])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
