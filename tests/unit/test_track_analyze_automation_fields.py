from __future__ import annotations

import unittest

import numpy as np

from services.track_analyzer.analyze import (
    SINGING_MIN_PROB,
    SILENCE_GAP_MIN_MS,
    SILENCE_IGNORE_EDGE_MS,
    VOICE_MIN_PROB,
    _aggregate_yamnet_probabilities,
    _analyze_prohibited_cues,
    _derive_dsp_score,
    _derive_voice_flag,
)


class TestTrackAnalyzeAutomationFields(unittest.TestCase):
    def test_yamnet_aggregation_includes_singing_and_labels_used(self) -> None:
        payload = {
            "top_classes": [
                {"label": "Singing", "score": 0.12},
                {"label": "Speech", "score": 0.21},
                {"label": "Music", "score": 0.9},
            ]
        }

        agg = _aggregate_yamnet_probabilities(payload)
        self.assertAlmostEqual(agg["singing_prob"], 0.12)
        self.assertAlmostEqual(agg["voice_prob"], 0.12)
        self.assertAlmostEqual(agg["speech_prob"], 0.21)
        self.assertIn("Singing", agg["voice_labels_used"])
        self.assertIn("Speech", agg["speech_labels_used"])
        self.assertEqual(agg["source"], "top_classes")
        self.assertEqual(agg["top_classes_count"], 3)

        flag, reason = _derive_voice_flag(agg)
        self.assertTrue(flag)
        self.assertIn(f"min={VOICE_MIN_PROB:.2f}", reason)
        self.assertIn(f"min={SINGING_MIN_PROB:.2f}", reason)


    def test_yamnet_aggregation_uses_top_classes_singing_score(self) -> None:
        payload = {
            "top_classes": [
                {"label": "Singing", "score": 0.02},
                {"label": "Music", "score": 0.99},
            ]
        }

        agg = _aggregate_yamnet_probabilities(payload)

        self.assertAlmostEqual(agg["singing_prob"], 0.02)
        self.assertIn("Singing", agg["voice_labels_used"])

    def test_prohibited_cues_detects_clipping_and_middle_silence_gap(self) -> None:
        sr = 16000
        head = 0.6 * np.ones(int(sr * 2.5), dtype=np.float32)
        silence = np.zeros(int(sr * 1.2), dtype=np.float32)
        tail = 0.2 * np.ones(int(sr * 2.5), dtype=np.float32)
        waveform = np.concatenate([head, silence, tail])

        cues = _analyze_prohibited_cues(waveform, sr, true_peak_dbfs=-0.05, spikes_found=True)

        self.assertEqual(cues["backend"], "fallback")
        self.assertIn("clipping", cues["checks_run"])
        self.assertTrue(cues["flags"]["clipping_detected"])
        self.assertTrue(cues["flags"]["silence_gaps"])
        self.assertIn("true_peak_dbfs", cues["metrics"])
        self.assertEqual(cues["thresholds"]["silence_gap_min_ms"], float(SILENCE_GAP_MIN_MS))

    def test_prohibited_cues_ignores_edge_silence_when_configured(self) -> None:
        sr = 16000
        start = np.zeros(int(sr * (SILENCE_IGNORE_EDGE_MS / 1000.0)), dtype=np.float32)
        middle = 0.2 * np.ones(int(sr * 0.6), dtype=np.float32)
        end = np.zeros(int(sr * (SILENCE_IGNORE_EDGE_MS / 1000.0)), dtype=np.float32)
        waveform = np.concatenate([start, middle, end])

        cues = _analyze_prohibited_cues(waveform, sr, true_peak_dbfs=-6.0, spikes_found=False)

        self.assertFalse(cues["flags"]["silence_gaps"])
        self.assertLess(cues["metrics"]["silence_max_gap_ms"], float(SILENCE_GAP_MIN_MS))

    def test_dsp_score_monotonic_and_bounded(self) -> None:
        base = {
            "flags": {"clipping_detected": False},
            "metrics": {"frame_rms_std": 0.01},
        }
        small_gap_cues = {**base, "metrics": {**base["metrics"], "silence_max_gap_ms": 500.0}}
        medium_gap_cues = {**base, "metrics": {**base["metrics"], "silence_max_gap_ms": 1500.0}}
        large_gap_cues = {**base, "metrics": {**base["metrics"], "silence_max_gap_ms": 4500.0}}

        small_score, small_components, _ = _derive_dsp_score(true_peak_dbfs=-6.0, spikes_found=False, prohibited_cues=small_gap_cues)
        medium_score, medium_components, _ = _derive_dsp_score(true_peak_dbfs=-6.0, spikes_found=False, prohibited_cues=medium_gap_cues)
        large_score, large_components, _ = _derive_dsp_score(true_peak_dbfs=-6.0, spikes_found=False, prohibited_cues=large_gap_cues)

        self.assertGreater(small_components["silence_component"], medium_components["silence_component"])
        self.assertGreater(medium_components["silence_component"], large_components["silence_component"])
        self.assertGreater(small_score, medium_score)
        self.assertGreater(medium_score, large_score)
        for score in (small_score, medium_score, large_score):
            self.assertGreaterEqual(score, 0.0)
            self.assertLessEqual(score, 1.0)


if __name__ == "__main__":
    unittest.main()
