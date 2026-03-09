from __future__ import annotations

import tempfile
import unittest
import wave

import numpy as np
from pathlib import Path
from unittest import mock

from services.common import db as dbm
from services.track_analyzer.analyze import (
    AnalyzeError,
    SIMILARITY_VECTOR_ORDER,
    _compute_advanced_derived_outputs,
    analyze_tracks,
)
from services.track_analyzer.yamnet import YAMNetUnavailableError


class FakeDrive:
    def download_to_path(self, file_id: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        sample_rate = 16000
        seconds = 2.0
        t = np.linspace(0.0, seconds, int(sample_rate * seconds), endpoint=False, dtype=np.float32)
        waveform = 0.3 * np.sin(2.0 * np.pi * 440.0 * t)
        pcm = np.clip(waveform * 32767.0, -32768.0, 32767.0).astype(np.int16)
        with wave.open(str(dest), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(pcm.tobytes())


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

                yamnet_payload = {
                    "top_classes": [
                        {"label": "Music", "score": 0.95},
                        {"label": "Speech", "score": 0.03},
                    ],
                    "probabilities": {"speech": 0.03, "voice": 0.12, "music": 0.95},
                }
                with mock.patch("services.track_analyzer.analyze.ffmpeg.ffprobe_json", return_value={"format": {"duration": "12.5"}}), mock.patch(
                    "services.track_analyzer.analyze.ffmpeg.run",
                    return_value=(0, "", "[Parsed_ebur128_0] Peak: -2.1 dB"),
                ), mock.patch("services.track_analyzer.analyze.yamnet.analyze_with_yamnet", return_value=yamnet_payload):
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

                feature_meta = features.get("advanced_v1", {}).get("meta", {})
                self.assertEqual(
                    {
                        "analyzer_version": feature_meta.get("analyzer_version"),
                        "schema_version": feature_meta.get("schema_version"),
                        "analyzed_at": feature_meta.get("analyzed_at"),
                        "rollout_tier": feature_meta.get("rollout_tier"),
                        "segment_policy": feature_meta.get("segment_policy"),
                    },
                    {
                        "analyzer_version": "advanced_track_analyzer_v1.1",
                        "schema_version": "advanced_v1",
                        "analyzed_at": feature_meta.get("analyzed_at"),
                        "rollout_tier": "s1",
                        "segment_policy": "track_full",
                    },
                )
                self.assertTrue(str(feature_meta.get("analyzed_at") or "").strip())

                self.assertTrue(str(features.get("dominant_texture") or "").strip())
                self.assertEqual(features.get("texture_backend"), "heuristic")
                self.assertGreaterEqual(float(features.get("texture_confidence")), 0.0)
                self.assertLessEqual(float(features.get("texture_confidence")), 1.0)
                self.assertIn(features.get("texture_reason"), {"ok", "low_confidence"})
                self.assertEqual(features.get("missing_fields"), [])
                self.assertTrue(str(tags.get("prohibited_cues_notes") or "").strip())
                self.assertIsNotNone(scores.get("dsp_score"))
                self.assertEqual(features.get("yamnet_probabilities", {}).get("music"), 0.95)
                self.assertEqual(tags.get("yamnet_tags"), ["Music", "Speech"])
                self.assertIn("yamnet_agg", features)
                self.assertIn("voice_flag", features)
                self.assertIn("voice_flag_reason", features)
                self.assertIn("speech_flag", features)
                self.assertIn("speech_flag_reason", features)
                self.assertFalse(features.get("speech_flag"))
                self.assertIn("prohibited_cues", tags)
                self.assertIn("dsp_score_version", scores)
                self.assertIn("dsp_components", scores)

                legacy_feature_keys = {
                    "duration_sec",
                    "true_peak_dbfs",
                    "spikes_found",
                    "yamnet_top_classes",
                    "yamnet_probabilities",
                    "yamnet_agg",
                    "voice_flag",
                    "voice_flag_reason",
                    "speech_flag",
                    "speech_flag_reason",
                    "dominant_texture",
                    "texture_backend",
                    "texture_confidence",
                    "texture_reason",
                    "analysis_status",
                    "missing_fields",
                }
                legacy_tag_keys = {
                    "yamnet_tags",
                    "prohibited_cues_notes",
                    "prohibited_cues",
                    "analysis_status",
                    "missing_fields",
                }
                legacy_score_keys = {
                    "dsp_score",
                    "dsp_score_version",
                    "dsp_components",
                    "dsp_notes",
                    "analysis_status",
                    "missing_fields",
                }

                self.assertTrue(legacy_feature_keys.issubset(features.keys()))
                self.assertTrue(legacy_tag_keys.issubset(tags.keys()))
                self.assertTrue(legacy_score_keys.issubset(scores.keys()))

                for payload in (features, tags, scores):
                    advanced_v1 = payload.get("advanced_v1")
                    self.assertIsInstance(advanced_v1, dict)
                    meta = advanced_v1.get("meta")
                    self.assertIsInstance(meta, dict)
                    self.assertEqual(meta.get("analyzer_version"), "advanced_track_analyzer_v1.1")
                    self.assertEqual(meta.get("schema_version"), "advanced_v1")
                    self.assertTrue(str(meta.get("analyzed_at") or "").strip())
                    self.assertEqual(meta.get("rollout_tier"), "s1")
                    self.assertEqual(meta.get("segment_policy"), "track_full")
                    self.assertEqual(advanced_v1.get("profiles"), {})

                self.assertIn("quality", features["advanced_v1"])
                self.assertIn("dynamics", features["advanced_v1"])
                self.assertIn("timbre", features["advanced_v1"])
                self.assertIn("structure", features["advanced_v1"])
                self.assertIn("voice", features["advanced_v1"])
                self.assertIn("similarity", features["advanced_v1"])
                self.assertNotIn("quality", tags["advanced_v1"])
                self.assertNotIn("dynamics", tags["advanced_v1"])
                self.assertNotIn("quality", scores["advanced_v1"])
                self.assertNotIn("dynamics", scores["advanced_v1"])

                self.assertIn("semantic", tags["advanced_v1"])
                self.assertIn("voice_tags", tags["advanced_v1"])
                self.assertIn("classifier_evidence", tags["advanced_v1"])
                self.assertIn("semantic", scores["advanced_v1"])
                self.assertIn("playlist_fit", scores["advanced_v1"])
                self.assertIn("transition", scores["advanced_v1"])
                self.assertIn("suitability", scores["advanced_v1"])
                self.assertIn("rule_trace", scores["advanced_v1"])
                self.assertIn("final_decisions", scores["advanced_v1"])

                quality = features["advanced_v1"]["quality"]
                quality_keys = {
                    "duration_sec",
                    "integrated_lufs",
                    "loudness_range_lra",
                    "true_peak_dbfs",
                    "clipping_ratio",
                    "noise_floor_estimate",
                    "silence_ratio",
                    "intro_silence_ratio",
                    "outro_silence_ratio",
                    "stereo_width",
                    "mono_compatibility",
                    "sample_rate",
                    "channels_count",
                }
                self.assertTrue(quality_keys.issubset(quality.keys()))
                self.assertGreaterEqual(float(quality["clipping_ratio"]), 0.0)
                self.assertLessEqual(float(quality["clipping_ratio"]), 1.0)
                self.assertGreaterEqual(float(quality["silence_ratio"]), 0.0)
                self.assertLessEqual(float(quality["silence_ratio"]), 1.0)
                self.assertGreaterEqual(float(quality["intro_silence_ratio"]), 0.0)
                self.assertLessEqual(float(quality["intro_silence_ratio"]), 1.0)
                self.assertGreaterEqual(float(quality["outro_silence_ratio"]), 0.0)
                self.assertLessEqual(float(quality["outro_silence_ratio"]), 1.0)
                self.assertGreaterEqual(float(quality["mono_compatibility"]), -1.0)
                self.assertLessEqual(float(quality["mono_compatibility"]), 1.0)

                dynamics = features["advanced_v1"]["dynamics"]
                dynamics_keys = {
                    "energy_mean",
                    "energy_variance",
                    "dynamic_stability",
                    "transient_density",
                    "pulse_strength",
                    "tempo_estimate",
                    "tempo_confidence",
                    "event_density",
                    "intensity_curve_summary",
                }
                self.assertTrue(dynamics_keys.issubset(dynamics.keys()))
                self.assertGreaterEqual(float(dynamics["energy_mean"]), 0.0)
                self.assertGreaterEqual(float(dynamics["energy_variance"]), 0.0)
                self.assertGreaterEqual(float(dynamics["dynamic_stability"]), 0.0)
                self.assertLessEqual(float(dynamics["dynamic_stability"]), 1.0)
                self.assertGreaterEqual(float(dynamics["pulse_strength"]), 0.0)
                self.assertLessEqual(float(dynamics["pulse_strength"]), 1.0)
                self.assertGreaterEqual(float(dynamics["tempo_confidence"]), 0.0)
                self.assertLessEqual(float(dynamics["tempo_confidence"]), 1.0)

                curve = dynamics["intensity_curve_summary"]
                self.assertIsInstance(curve, dict)
                self.assertEqual(
                    set(curve.keys()),
                    {
                        "start_mean",
                        "middle_mean",
                        "end_mean",
                        "linear_slope",
                        "peak_position_ratio",
                        "convexity_hint",
                    },
                )
                self.assertGreaterEqual(float(curve["peak_position_ratio"]), 0.0)
                self.assertLessEqual(float(curve["peak_position_ratio"]), 1.0)
                self.assertIn(float(curve["convexity_hint"]), {-1.0, 0.0, 1.0})

                timbre = features["advanced_v1"]["timbre"]
                timbre_keys = {
                    "brightness",
                    "warmth",
                    "darkness",
                    "spectral_centroid_mean",
                    "spectral_rolloff_mean",
                    "low_end_weight",
                    "high_end_sharpness",
                    "harmonic_density",
                    "tonal_stability",
                    "drone_presence",
                    "pad_presence",
                    "percussion_presence",
                    "melodic_prominence",
                    "texture_smoothness",
                }
                self.assertEqual(set(timbre.keys()), timbre_keys)

                structure = features["advanced_v1"]["structure"]
                structure_keys = {
                    "intro_energy",
                    "early_section_energy",
                    "middle_section_energy",
                    "late_section_energy",
                    "outro_energy",
                    "intro_smoothness",
                    "outro_smoothness",
                    "structural_stability",
                    "climax_presence",
                    "abruptness_score",
                    "loop_friendliness",
                    "fade_friendliness",
                    "section_summary",
                }
                self.assertEqual(set(structure.keys()), structure_keys)
                self.assertEqual(set(structure["section_summary"].keys()), {"parts", "peak_section", "mean_energy"})
                self.assertEqual(len(structure["section_summary"]["parts"]), 5)

                voice = features["advanced_v1"]["voice"]
                self.assertEqual(
                    set(voice.keys()),
                    {"speech_probability", "vocal_probability", "spoken_word_density", "human_presence_score"},
                )

                similarity = features["advanced_v1"]["similarity"]
                vector = similarity["normalized_feature_vector"]
                self.assertEqual(
                    SIMILARITY_VECTOR_ORDER,
                    [
                        "timbre.brightness",
                        "timbre.warmth",
                        "timbre.darkness",
                        "timbre.spectral_centroid_mean",
                        "timbre.spectral_rolloff_mean",
                        "timbre.low_end_weight",
                        "timbre.high_end_sharpness",
                        "timbre.harmonic_density",
                        "timbre.tonal_stability",
                        "timbre.drone_presence",
                        "timbre.pad_presence",
                        "timbre.percussion_presence",
                        "timbre.melodic_prominence",
                        "timbre.texture_smoothness",
                        "structure.intro_energy",
                        "structure.early_section_energy",
                        "structure.middle_section_energy",
                        "structure.late_section_energy",
                        "structure.outro_energy",
                        "structure.intro_smoothness",
                        "structure.outro_smoothness",
                        "structure.structural_stability",
                        "structure.climax_presence",
                        "structure.abruptness_score",
                        "structure.loop_friendliness",
                        "structure.fade_friendliness",
                        "voice.speech_probability",
                        "voice.vocal_probability",
                        "voice.spoken_word_density",
                        "voice.human_presence_score",
                    ],
                )
                self.assertEqual(len(vector), len(SIMILARITY_VECTOR_ORDER))
                self.assertTrue(all(isinstance(v, float) for v in vector))
                self.assertIn("diversity_penalty_base", similarity)
                self.assertGreaterEqual(float(similarity["diversity_penalty_base"]), 0.0)
                self.assertLessEqual(float(similarity["diversity_penalty_base"]), 1.0)

                flat = {
                    **{f"timbre.{k}": float(v) for k, v in timbre.items()},
                    **{f"structure.{k}": float(v) for k, v in structure.items() if k != "section_summary"},
                    **{f"voice.{k}": float(v) for k, v in voice.items()},
                }
                expected_vector = [flat[key] for key in SIMILARITY_VECTOR_ORDER]
                self.assertEqual(vector, expected_vector)

                self.assertEqual(quality["duration_sec"], features["duration_sec"])
                self.assertEqual(quality["true_peak_dbfs"], features["true_peak_dbfs"])

                self.assertEqual(features["advanced_v1"]["meta"]["analyzed_at"], tags["advanced_v1"]["meta"]["analyzed_at"])
                self.assertEqual(features["advanced_v1"]["meta"]["analyzed_at"], scores["advanced_v1"]["meta"]["analyzed_at"])

                tmp_track_dir = Path(td) / "tmp" / "track_analyzer" / "99" / "1"
                self.assertFalse(tmp_track_dir.exists())
            finally:
                conn.close()



    def test_compute_advanced_outputs_tags_and_scores(self) -> None:
        outputs = _compute_advanced_derived_outputs(
            quality={"clipping_ratio": 0.0},
            dynamics={"energy_mean": 0.6, "pulse_strength": 0.4, "tempo_confidence": 0.5},
            timbre={"pad_presence": 0.8, "drone_presence": 0.6, "percussion_presence": 0.1, "texture_smoothness": 0.9},
            structure={
                "intro_smoothness": 0.9,
                "outro_smoothness": 0.9,
                "climax_presence": 0.3,
                "fade_friendliness": 0.7,
                "loop_friendliness": 0.8,
                "abruptness_score": 0.1,
            },
            voice={"speech_probability": 0.1, "vocal_probability": 0.1, "spoken_word_density": 0.1, "human_presence_score": 0.1},
            similarity={"diversity_penalty_base": 0.35},
            channel_slug="darkwood-reverie",
        )
        self.assertIn("ambient", outputs["semantic"]["mood_tags"])
        self.assertIn("calm", outputs["semantic"]["mood_tags"])
        self.assertIn("fade-friendly", outputs["semantic"]["theme_tags"])
        self.assertIn("loopable", outputs["semantic"]["theme_tags"])
        self.assertGreaterEqual(outputs["suitability"]["channel_fit_score"], 0.0)
        self.assertLessEqual(outputs["suitability"]["channel_fit_score"], 1.0)

    def test_compute_advanced_outputs_veto_and_penalties(self) -> None:
        outputs = _compute_advanced_derived_outputs(
            quality={"clipping_ratio": 0.05},
            dynamics={"energy_mean": 0.4, "pulse_strength": 0.3, "tempo_confidence": 0.2},
            timbre={"pad_presence": 0.7, "drone_presence": 0.7, "percussion_presence": 0.1, "texture_smoothness": 0.8},
            structure={
                "intro_smoothness": 0.8,
                "outro_smoothness": 0.8,
                "climax_presence": 0.2,
                "fade_friendliness": 0.3,
                "loop_friendliness": 0.2,
                "abruptness_score": 0.9,
            },
            voice={"speech_probability": 0.95, "vocal_probability": 0.6, "spoken_word_density": 0.9, "human_presence_score": 0.95},
            similarity={"diversity_penalty_base": 0.6},
            channel_slug="darkwood-reverie",
        )
        self.assertTrue(outputs["final_decisions"]["hard_veto"])
        self.assertGreater(outputs["final_decisions"]["soft_penalty_total"], 0.0)
        self.assertIn("VETO_SPEECH_IN_INSTRUMENTAL", outputs["final_decisions"]["warning_codes"])
        self.assertTrue(any(row.get("rule_id") == "decision.veto.instrumental_speech.v1" for row in outputs["rule_trace"]))




    def test_analyze_texture_exception_sets_reason(self) -> None:
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

                yamnet_payload = {
                    "top_classes": [{"label": "Music", "score": 0.95}],
                    "probabilities": {"music": 0.95},
                }
                with mock.patch("services.track_analyzer.analyze.ffmpeg.ffprobe_json", return_value={"format": {"duration": "12.5"}}), mock.patch(
                    "services.track_analyzer.analyze.ffmpeg.run",
                    return_value=(0, "", "[Parsed_ebur128_0] Peak: -2.1 dB"),
                ), mock.patch("services.track_analyzer.analyze.yamnet.analyze_with_yamnet", return_value=yamnet_payload), mock.patch(
                    "services.track_analyzer.analyze._analyze_texture",
                    side_effect=RuntimeError("texture backend crash"),
                ):
                    stats = analyze_tracks(
                        conn,
                        FakeDrive(),
                        channel_slug="darkwood-reverie",
                        storage_root=td,
                        job_id=102,
                        scope="pending",
                        force=False,
                        max_tracks=10,
                    )

                self.assertEqual(stats.processed, 1)
                feature_row = conn.execute("SELECT payload_json FROM track_features LIMIT 1").fetchone()
                self.assertIsNotNone(feature_row)

                features = dbm.json_loads(feature_row["payload_json"])
                self.assertEqual(features.get("dominant_texture"), "unknown texture")
                self.assertEqual(features.get("texture_backend"), "heuristic")
                self.assertIsNone(features.get("texture_confidence"))
                self.assertEqual(features.get("texture_reason"), "exception")
                self.assertEqual(features.get("missing_fields"), [])
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

    def test_analyze_raises_deterministic_error_when_yamnet_missing(self) -> None:
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
                ), mock.patch(
                    "services.track_analyzer.analyze.yamnet.analyze_with_yamnet",
                    side_effect=YAMNetUnavailableError("YAMNET_NOT_INSTALLED"),
                ):
                    with self.assertRaises(AnalyzeError) as ctx:
                        analyze_tracks(
                            conn,
                            FakeDrive(),
                            channel_slug="darkwood-reverie",
                            storage_root=td,
                            job_id=101,
                            scope="pending",
                            force=False,
                            max_tracks=10,
                        )

                self.assertEqual(str(ctx.exception), "YAMNET_NOT_INSTALLED: install via UI button and retry")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
