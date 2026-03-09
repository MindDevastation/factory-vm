from __future__ import annotations

import unittest
import json

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

    def test_advanced_v1_fields_are_exposed_with_expected_flattening(self) -> None:
        track_pk = self._insert_track("001", "file-001")
        features_payload = {
            "analysis_status": "ok",
            "voice_flag": False,
            "advanced_v1": {
                "meta": {"analyzer_version": "advanced_track_analyzer_v1.1", "schema_version": "advanced_v1"},
                "quality": {"integrated_lufs": -18.2, "channels_count": 2},
                "dynamics": {"energy_mean": 0.44, "intensity_curve_summary": {"start_mean": 0.1, "middle_mean": 0.2}},
                "timbre": {"brightness": 0.31},
                "structure": {"intro_energy": 0.22, "section_summary": {"parts": 3}},
                "voice": {"speech_probability": 0.12},
                "similarity": {"normalized_feature_vector": [0.1, 0.2, 0.3], "diversity_penalty_base": 0.27},
            },
        }
        tags_payload = {
            "yamnet_tags": ["rain"],
            "advanced_v1": {
                "semantic": {"mood_tags": ["calm", "ambient"], "theme_tags": ["minimal"]},
                "voice_tags": ["spoken_word"],
                "classifier_evidence": {"yamnet_top_classes": [{"label": "rain", "score": 0.9}]},
            },
        }
        scores_payload = {
            "dsp_score": 0.93,
            "advanced_v1": {
                "semantic": {"functional_scores": {"focus": 0.8, "energy": 0.3, "narrative": 0.2, "background_compatibility": 0.7}},
                "playlist_fit": {"continuity_score": 0.6, "mixability_score": 0.7, "variety_support_score": 0.8},
                "transition": {"intro_profile": "soft", "outro_profile": "tail", "transition_risk_score": 0.1},
                "suitability": {
                    "content_type_fit_score": 0.9,
                    "channel_fit_score": 0.85,
                    "selected_content_context": "LONG_INSTRUMENTAL_AMBIENT",
                    "content_type_fit_by_context": {"LONG_INSTRUMENTAL_AMBIENT": 0.9, "LONG_LYRICAL": 0.2},
                },
                "rule_trace": [{"rule_id": "semantic.focus.v1", "matched": True}],
                "final_decisions": {"hard_veto": False, "soft_penalty_total": 0.15, "warning_codes": ["PENALTY_TRANSITION_RISK"]},
            },
        }
        self.conn.execute(
            "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, json.dumps(features_payload), 1010.0),
        )
        self.conn.execute(
            "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, json.dumps(tags_payload), 1020.0),
        )
        self.conn.execute(
            "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, json.dumps(scores_payload), 1030.0),
        )

        report = build_channel_report(self.conn, "darkwood-reverie")
        row = report["rows"][0]
        self.assertEqual(row["analysis_status"], "ok")
        self.assertEqual(row["voice_flag"], False)
        self.assertEqual(row["yamnet_tags"], "rain")
        self.assertEqual(row["dsp_score"], 0.93)
        self.assertEqual(row["analyzer_version"], "advanced_track_analyzer_v1.1")
        self.assertEqual(row["schema_version"], "advanced_v1")
        self.assertEqual(row["hard_veto"], False)
        self.assertEqual(row["soft_penalty_total"], 0.15)
        self.assertEqual(row["mood_tags_csv"], "calm, ambient")
        self.assertEqual(row["theme_tags_csv"], "minimal")
        self.assertEqual(row["warning_codes_json"], '["PENALTY_TRANSITION_RISK"]')
        self.assertEqual(row["intensity_curve_summary_json"], '{"middle_mean": 0.2, "start_mean": 0.1}')
        self.assertEqual(row["section_summary_json"], '{"parts": 3}')
        self.assertEqual(row["normalized_feature_vector_json"], "[0.1, 0.2, 0.3]")
        self.assertEqual(row["similarity_diversity_penalty_base"], 0.27)
        self.assertEqual(row["rule_trace_json"], '[{"matched": true, "rule_id": "semantic.focus.v1"}]')

        keys = [col["key"] for col in report["columns"]]
        self.assertIn("similarity_diversity_penalty_base", keys)
        self.assertLess(keys.index("quality_integrated_lufs"), keys.index("dynamics_energy_mean"))
        self.assertLess(keys.index("dynamics_energy_mean"), keys.index("timbre_brightness"))
        self.assertLess(keys.index("timbre_brightness"), keys.index("structure_intro_energy"))
        self.assertLess(keys.index("structure_intro_energy"), keys.index("voice_speech_probability"))
        self.assertLess(keys.index("voice_speech_probability"), keys.index("semantic_focus"))
        self.assertLess(keys.index("semantic_focus"), keys.index("playlist_continuity_score"))
        self.assertLess(keys.index("playlist_continuity_score"), keys.index("transition_intro_profile"))
        self.assertLess(keys.index("transition_intro_profile"), keys.index("suitability_content_type_fit_score"))
        self.assertLess(keys.index("suitability_content_type_fit_score"), keys.index("analyzer_version"))
        self.assertLess(keys.index("analyzer_version"), keys.index("intensity_curve_summary_json"))

    def test_legacy_rows_without_advanced_v1_remain_readable(self) -> None:
        track_pk = self._insert_track("001", "file-001")
        self.conn.execute(
            "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, '{"analysis_status":"ok","voice_flag":true,"yamnet_agg":{"source":"top_classes"}}', 1010.0),
        )
        self.conn.execute(
            "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, '{"yamnet_tags":["legacy-rain"]}', 1020.0),
        )
        self.conn.execute(
            "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
            (track_pk, '{"dsp_score":0.42}', 1030.0),
        )

        report = build_channel_report(self.conn, "darkwood-reverie")
        self.assertEqual(report["summary"]["tracks_count"], 1)
        row = report["rows"][0]
        self.assertEqual(row["analysis_status"], "ok")
        self.assertEqual(row["voice_flag"], True)
        self.assertEqual(row["yamnet_tags"], "legacy-rain")
        self.assertEqual(row["dsp_score"], 0.42)
        self.assertIsNone(row["analyzer_version"])
        self.assertIsNone(row["warning_codes_json"])


if __name__ == "__main__":
    unittest.main()
