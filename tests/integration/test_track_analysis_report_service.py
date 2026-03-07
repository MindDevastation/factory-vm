from __future__ import annotations

import unittest

from services.common import db as dbm
from services.track_analysis_report.registry import COLUMN_REGISTRY
from services.track_analysis_report.report_service import (
    ChannelNotFoundError,
    InvalidChannelSlugError,
    build_channel_report,
)
from tests._helpers import temp_env


class TestTrackAnalysisReportService(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_env_ctx = temp_env()
        self._td, self.env = self._temp_env_ctx.__enter__()
        self.conn = dbm.connect(self.env)
        dbm.migrate(self.conn)
        self.conn.execute(
            """
            INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled)
            VALUES(?, ?, 'LONG', 1.0, 'long_1080p24', 0)
            """,
            ("darkwood-reverie", "Darkwood Reverie"),
        )

    def tearDown(self) -> None:
        self.conn.close()
        self._temp_env_ctx.__exit__(None, None, None)

    def _insert_track(self, track_id: str, gdrive_file_id: str) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
            VALUES(?, ?, ?, 'gdrive', ?, ?, ?, 180.0, 1000.0, 1005.0)
            """,
            ("darkwood-reverie", track_id, gdrive_file_id, f"{track_id}.wav", f"Title {track_id}", "Artist X"),
        )
        return int(cur.lastrowid)

    def test_valid_channel_returns_columns_rows_and_summary(self) -> None:
        track_pk = self._insert_track("001", "file-001")
        self.conn.execute(
            "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, '{"analysis_status":"ok","voice_flag":false}', 1010.0),
        )
        self.conn.execute(
            "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, '{"yamnet_tags":["rain","wind"]}', 1020.0),
        )
        self.conn.execute(
            "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, '{"dsp_score":0.93}', 1030.0),
        )

        report = build_channel_report(self.conn, "darkwood-reverie")

        self.assertEqual(report["channel_slug"], "darkwood-reverie")
        self.assertEqual(report["summary"]["tracks_count"], 1)
        self.assertEqual(len(report["columns"]), len(COLUMN_REGISTRY))
        self.assertEqual(len(report["rows"]), 1)

    def test_partial_or_missing_analysis_rows_do_not_fail(self) -> None:
        first_pk = self._insert_track("001", "file-001")
        second_pk = self._insert_track("002", "file-002")

        self.conn.execute(
            "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (first_pk, '{"analysis_status":"ok"}', 1010.0),
        )
        self.conn.execute(
            "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (second_pk, '{invalid-json', 1020.0),
        )

        report = build_channel_report(self.conn, "darkwood-reverie")

        self.assertEqual(report["summary"]["tracks_count"], 2)
        self.assertEqual(len(report["rows"]), 2)
        self.assertIsNone(report["rows"][0]["dsp_score"])
        self.assertIsNone(report["rows"][1]["yamnet_tags"])

    def test_invalid_channel_slug_raises_typed_error(self) -> None:
        with self.assertRaises(InvalidChannelSlugError):
            build_channel_report(self.conn, "   ")

    def test_unknown_channel_raises_typed_not_found_error(self) -> None:
        with self.assertRaises(ChannelNotFoundError):
            build_channel_report(self.conn, "unknown-channel")

    def test_row_keys_exactly_match_registry_keys(self) -> None:
        self._insert_track("001", "file-001")
        report = build_channel_report(self.conn, "darkwood-reverie")

        expected_keys = {entry["key"] for entry in COLUMN_REGISTRY}
        self.assertEqual(len(report["rows"]), 1)
        self.assertEqual(set(report["rows"][0].keys()), expected_keys)


if __name__ == "__main__":
    unittest.main()
