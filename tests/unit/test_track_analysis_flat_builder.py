from __future__ import annotations

import json
import unittest

from services.track_analyzer.track_analysis_flat import build_track_analysis_flat_row


class TestTrackAnalysisFlatBuilder(unittest.TestCase):
    def test_build_row_derives_human_friendly_fields_stably(self) -> None:
        row = build_track_analysis_flat_row(
            track_row={"id": 7, "channel_slug": "darkwood-reverie", "track_id": "trk-007", "gdrive_file_id": "g-7"},
            features_payload={
                "analysis_status": "COMPLETE",
                "duration_sec": 181.2,
                "true_peak_dbfs": -2.1,
                "spikes_found": True,
                "yamnet_top_classes": [{"label": "Speech", "score": 0.2}, {"label": "Music", "score": 0.8}],
                "voice_flag": True,
                "voice_flag_reason": "voice_prob=0.340 (min=0.20)",
                "speech_flag": False,
                "speech_flag_reason": "speech_prob=0.020 (min=0.10)",
                "dominant_texture": "smooth pad",
                "texture_confidence": 0.7,
                "texture_reason": "stable harmonics",
                "advanced_v1": {"meta": {"analyzer_version": "adv-v1", "schema_version": "schema-1"}},
            },
            tags_payload={
                "yamnet_tags": ["Music", "Rain"],
                "prohibited_cues_notes": "Fallback analyzer flags: clipping_detected",
                "prohibited_cues": {"flags": {"clipping_detected": True, "silence_gaps": False}},
                "mood": "calm",
            },
            scores_payload={
                "dsp_score": 0.86,
                "dsp_score_version": "v1",
                "dsp_notes": "weighted components",
                "safety": 0.9,
                "scene_match": 0.8,
            },
            analysis_computed_at=1700000000.5,
        )

        self.assertEqual(row["track_pk"], 7)
        self.assertEqual(row["yamnet_top_tags_text"], "Music, Rain, Speech")
        self.assertEqual(
            row["prohibited_cues_summary"],
            "Fallback analyzer flags: clipping_detected | active_flags=clipping_detected",
        )
        self.assertEqual(
            row["human_readable_notes"],
            "Fallback analyzer flags: clipping_detected | weighted components | stable harmonics | voice_prob=0.340 (min=0.20) | speech_prob=0.020 (min=0.10)",
        )
        self.assertEqual(json.loads(row["yamnet_top_classes_json"]), [{"label": "Speech", "score": 0.2}, {"label": "Music", "score": 0.8}])
        self.assertEqual(json.loads(row["prohibited_cues_flags_json"]), {"clipping_detected": True, "silence_gaps": False})

    def test_build_row_defaults_status_and_booleans(self) -> None:
        row = build_track_analysis_flat_row(
            track_row={"id": 1, "channel_slug": "c", "track_id": "t", "gdrive_file_id": None},
            features_payload={},
            tags_payload={},
            scores_payload={},
            analysis_computed_at=1.0,
        )
        self.assertEqual(row["analysis_status"], "UNKNOWN")
        self.assertEqual(row["voice_flag"], 0)
        self.assertEqual(row["speech_flag"], 0)
        self.assertEqual(row["spikes_found"], 0)


if __name__ == "__main__":
    unittest.main()
