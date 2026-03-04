from __future__ import annotations

import unittest

import numpy as np

from services.track_analyzer.texture_heuristics import classify_texture


class TestTextureHeuristics(unittest.TestCase):
    def test_sine_classifies_tonal_sustained(self) -> None:
        sr = 16000
        t = np.linspace(0.0, 4.0, int(sr * 4.0), endpoint=False, dtype=np.float32)
        waveform = 0.4 * np.sin(2.0 * np.pi * 220.0 * t)

        label, confidence, _debug = classify_texture(waveform, sr)

        self.assertEqual(label, "tonal_sustained")
        self.assertGreaterEqual(confidence, 0.0)
        self.assertLessEqual(confidence, 1.0)

    def test_white_noise_classifies_noisy_distorted(self) -> None:
        sr = 16000
        rng = np.random.default_rng(seed=42)
        waveform = rng.normal(0.0, 0.3, size=int(sr * 4.0)).astype(np.float32)

        label, confidence, _debug = classify_texture(waveform, sr)

        self.assertEqual(label, "noisy_distorted")
        self.assertGreaterEqual(confidence, 0.0)
        self.assertLessEqual(confidence, 1.0)

    def test_click_train_is_percussive_or_mixed(self) -> None:
        sr = 16000
        seconds = 4.0
        waveform = np.zeros(int(sr * seconds), dtype=np.float32)
        click_every = int(0.2 * sr)
        click_len = int(0.01 * sr)
        for i in range(0, waveform.size - click_len, click_every):
            waveform[i : i + click_len] = 0.7

        label, confidence, _debug = classify_texture(waveform, sr)

        self.assertIn(label, {"percussive_rhythmic", "mixed"})
        self.assertGreaterEqual(confidence, 0.0)
        self.assertLessEqual(confidence, 1.0)


if __name__ == "__main__":
    unittest.main()
