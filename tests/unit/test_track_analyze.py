from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.common import db as dbm
from services.track_analyzer.analyze import AnalyzeError, analyze_tracks


class FakeDrive:
    def download_to_path(self, file_id: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"fake wav bytes " + file_id.encode("utf-8"))


class TestTrackAnalyze(unittest.TestCase):
    def test_analyze_writes_required_rows_and_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("darkwood-reverie", "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
                )
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    ("darkwood-reverie", "001", "fid-1", "GDRIVE", "001_A.wav", "A", None, None, dbm.now_ts(), None),
                )

                with mock.patch("services.track_analyzer.analyze.ffmpeg.ffprobe_json", return_value={"format": {"duration": "12.5"}}), mock.patch(
                    "services.track_analyzer.analyze.ffmpeg.run",
                    return_value=(0, "", "[Parsed_ebur128_0] Peak: -2.1 dB"),
                ):
                    stats = analyze_tracks(
                        conn,
                        FakeDrive(),
                        channel_slug="darkwood-reverie",
                        storage_root=td,
                        job_id=99,
                        scope="pending",
                        force=False,
                        max_tracks=10,
                    )

                self.assertEqual(stats.selected, 1)
                self.assertEqual(stats.processed, 1)
                self.assertEqual(stats.failed, 0)

                feature_row = conn.execute("SELECT payload_json FROM track_features LIMIT 1").fetchone()
                tag_row = conn.execute("SELECT payload_json FROM track_tags LIMIT 1").fetchone()
                score_row = conn.execute("SELECT payload_json FROM track_scores LIMIT 1").fetchone()

                self.assertIsNotNone(feature_row)
                self.assertIsNotNone(tag_row)
                self.assertIsNotNone(score_row)

                features = dbm.json_loads(feature_row["payload_json"])
                tags = dbm.json_loads(tag_row["payload_json"])
                scores = dbm.json_loads(score_row["payload_json"])

                self.assertTrue(str(features.get("dominant_texture") or "").strip())
                self.assertTrue(str(tags.get("prohibited_cues_notes") or "").strip())
                self.assertIsNotNone(scores.get("dsp_score"))

                tmp_track_dir = Path(td) / "tmp" / "track_analyzer" / "99" / "1"
                self.assertFalse(tmp_track_dir.exists())
            finally:
                conn.close()

    def test_analyze_requires_thresholds(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("darkwood-reverie", "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
                )

                with self.assertRaises(AnalyzeError) as ctx:
                    analyze_tracks(
                        conn,
                        FakeDrive(),
                        channel_slug="darkwood-reverie",
                        storage_root=td,
                        job_id=100,
                    )
                self.assertEqual(str(ctx.exception), "CHANNEL_NOT_IN_CANON")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
