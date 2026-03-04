from __future__ import annotations

import unittest

import numpy as np

from services.track_analyzer.analyze import (
    SINGING_MIN_PROB,
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

        flag, reason = _derive_voice_flag(agg)
        self.assertTrue(flag)
        self.assertIn(f"min={VOICE_MIN_PROB:.2f}", reason)
        self.assertIn(f"min={SINGING_MIN_PROB:.2f}", reason)

    def test_prohibited_cues_detects_clipping_and_silence_gap(self) -> None:
        sr = 16000
        clipped = np.ones(sr // 2, dtype=np.float32)
        silence = np.zeros(sr, dtype=np.float32)
        waveform = np.concatenate([clipped, silence, 0.2 * np.ones(sr // 2, dtype=np.float32)])

        cues = _analyze_prohibited_cues(waveform, sr, true_peak_dbfs=-0.05, spikes_found=True)

        self.assertEqual(cues["backend"], "fallback")
        self.assertIn("clipping", cues["checks_run"])
        self.assertTrue(cues["flags"]["clipping_detected"])
        self.assertTrue(cues["flags"]["silence_gaps"])
        self.assertIn("true_peak_dbfs", cues["metrics"])

    def test_dsp_score_monotonic_and_bounded(self) -> None:
        sr = 16000
        good_wave = 0.1 * np.sin(2 * np.pi * 220 * np.linspace(0, 1, sr, endpoint=False)).astype(np.float32)
        bad_wave = np.concatenate([np.ones(sr // 2, dtype=np.float32), np.zeros(sr // 2, dtype=np.float32)])

        good_cues = _analyze_prohibited_cues(good_wave, sr, true_peak_dbfs=-6.0, spikes_found=False)
        bad_cues = _analyze_prohibited_cues(bad_wave, sr, true_peak_dbfs=-0.05, spikes_found=True)

        good_score, good_components, _ = _derive_dsp_score(true_peak_dbfs=-6.0, spikes_found=False, prohibited_cues=good_cues)
        bad_score, bad_components, _ = _derive_dsp_score(true_peak_dbfs=-0.05, spikes_found=True, prohibited_cues=bad_cues)

        self.assertGreater(good_score, bad_score)
        self.assertGreaterEqual(good_score, 0.0)
        self.assertLessEqual(good_score, 1.0)
        self.assertGreaterEqual(bad_score, 0.0)
        self.assertLessEqual(bad_score, 1.0)
        self.assertGreater(good_components["headroom_component"], bad_components["headroom_component"])
        self.assertGreater(good_components["clipping_component"], bad_components["clipping_component"])


if __name__ == "__main__":
    unittest.main()
