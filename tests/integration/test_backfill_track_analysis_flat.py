from __future__ import annotations

import unittest

from scripts.backfill_track_analysis_flat import backfill_track_analysis_flat
from services.common import db as dbm


class TestBackfillTrackAnalysisFlat(unittest.TestCase):
    def test_backfill_is_idempotent_and_updates_existing_rows(self) -> None:
        conn = dbm.connect(type("E", (), {"db_path": ":memory:"})())
        try:
            dbm.migrate(conn)
            conn.execute(
                """
                INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                ("darkwood-reverie", "001", "fid-1", "GDRIVE", "001.wav", "A", None, None, dbm.now_ts(), None),
            )
            track_pk = int(conn.execute("SELECT id FROM tracks WHERE track_id = ?", ("001",)).fetchone()["id"])
            features_json = dbm.json_dumps(
                {
                    "analysis_status": "COMPLETE",
                    "duration_sec": 12.5,
                    "yamnet_top_classes": [{"label": "Music", "score": 0.95}],
                    "advanced_v1": {"meta": {"analyzer_version": "adv", "schema_version": "v1"}},
                }
            )
            tags_json = dbm.json_dumps(
                {
                    "yamnet_tags": ["Music"],
                    "prohibited_cues": {"flags": {"clipping_detected": False}},
                    "prohibited_cues_notes": "No prohibited cues.",
                }
            )
            scores_json = dbm.json_dumps({"dsp_score": 0.7, "dsp_score_version": "v1", "dsp_notes": "weighted"})
            conn.execute("INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?,?,?)", (track_pk, features_json, 1000.0))
            conn.execute("INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?,?,?)", (track_pk, tags_json, 1001.0))
            conn.execute("INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?,?,?)", (track_pk, scores_json, 1002.0))

            first = backfill_track_analysis_flat(conn)
            self.assertEqual((first.scanned, first.inserted, first.updated, first.skipped, first.errors), (1, 1, 0, 0, 0))

            flat = conn.execute("SELECT * FROM track_analysis_flat WHERE track_pk = ?", (track_pk,)).fetchone()
            self.assertIsNotNone(flat)
            assert flat is not None
            self.assertEqual(flat["analysis_computed_at"], 1002.0)
            self.assertEqual(flat["analysis_status"], "COMPLETE")

            conn.execute(
                "UPDATE track_scores SET payload_json = ?, computed_at = ? WHERE track_pk = ?",
                (dbm.json_dumps({"dsp_score": 0.9, "dsp_score_version": "v2", "dsp_notes": "retuned"}), 1003.0, track_pk),
            )

            second = backfill_track_analysis_flat(conn)
            self.assertEqual((second.scanned, second.inserted, second.updated, second.skipped, second.errors), (1, 0, 1, 0, 0))

            flat2 = conn.execute("SELECT * FROM track_analysis_flat WHERE track_pk = ?", (track_pk,)).fetchone()
            self.assertIsNotNone(flat2)
            assert flat2 is not None
            self.assertEqual(flat2["analysis_computed_at"], 1003.0)
            self.assertEqual(flat2["dsp_score"], 0.9)
            self.assertEqual(flat2["dsp_score_version"], "v2")

            # Ensure source payload rows were not mutated by the backfill utility.
            persisted = conn.execute("SELECT payload_json FROM track_scores WHERE track_pk = ?", (track_pk,)).fetchone()
            self.assertEqual(persisted["payload_json"], dbm.json_dumps({"dsp_score": 0.9, "dsp_score_version": "v2", "dsp_notes": "retuned"}))
        finally:
            conn.close()

    def test_backfill_skips_tracks_with_missing_or_invalid_payload_rows(self) -> None:
        conn = dbm.connect(type("E", (), {"db_path": ":memory:"})())
        try:
            dbm.migrate(conn)
            conn.execute(
                """
                INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                ("darkwood-reverie", "001", "fid-1", "GDRIVE", "001.wav", "A", None, None, dbm.now_ts(), None),
            )
            conn.execute(
                """
                INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                ("darkwood-reverie", "002", "fid-2", "GDRIVE", "002.wav", "B", None, None, dbm.now_ts(), None),
            )
            first_pk = int(conn.execute("SELECT id FROM tracks WHERE track_id = ?", ("001",)).fetchone()["id"])
            second_pk = int(conn.execute("SELECT id FROM tracks WHERE track_id = ?", ("002",)).fetchone()["id"])

            conn.execute("INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?,?,?)", (first_pk, "{", 1010.0))
            conn.execute(
                "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (first_pk, dbm.json_dumps({"yamnet_tags": ["Music"]}), 1010.0),
            )
            conn.execute(
                "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (first_pk, dbm.json_dumps({"dsp_score": 0.5}), 1010.0),
            )

            conn.execute(
                "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?,?,?)",
                (second_pk, dbm.json_dumps({"analysis_status": "COMPLETE"}), 1020.0),
            )

            summary = backfill_track_analysis_flat(conn)
            self.assertEqual((summary.scanned, summary.inserted, summary.updated, summary.skipped, summary.errors), (2, 0, 0, 2, 0))
            self.assertIsNone(conn.execute("SELECT 1 FROM track_analysis_flat WHERE track_pk = ?", (first_pk,)).fetchone())
            self.assertIsNone(conn.execute("SELECT 1 FROM track_analysis_flat WHERE track_pk = ?", (second_pk,)).fetchone())
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
