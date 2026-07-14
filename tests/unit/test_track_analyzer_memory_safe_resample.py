from __future__ import annotations

import sys
import types
import unittest
from unittest import mock

import numpy as np

from services.track_analyzer import yamnet_resample


class _DummyTF(types.SimpleNamespace):
    __version__ = "test"
    float32 = np.float32

    @staticmethod
    def convert_to_tensor(value, dtype=None):
        return np.asarray(value, dtype=dtype)

    @staticmethod
    def cast(value, dtype):
        return np.asarray(value, dtype=dtype)


class TestTrackAnalyzerMemorySafeResample(unittest.TestCase):
    def test_stereo_float64_like_input_is_downmixed_to_mono_float32_before_resample(self) -> None:
        stereo = np.vstack([np.arange(8, dtype=np.float64), np.arange(8, dtype=np.float64) + 2.0])

        mono = yamnet_resample.normalize_audio_for_yamnet(stereo)

        self.assertEqual(mono.shape, (8,))
        self.assertEqual(mono.dtype, np.float32)
        np.testing.assert_allclose(mono, np.arange(8, dtype=np.float32) + 1.0)

    def test_numpy_fallback_returns_mono_float32_not_stereo_shape(self) -> None:
        stereo = np.column_stack([np.linspace(-1, 1, 12), np.linspace(1, -1, 12)]).astype(np.float64)
        dummy_tf = _DummyTF(signal=types.SimpleNamespace(resample=None))

        with mock.patch.dict(sys.modules, {"tensorflow": dummy_tf}), mock.patch(
            "services.track_analyzer.yamnet_resample._tensorflow_io_audio_module",
            return_value=None,
        ), mock.patch("services.track_analyzer.yamnet_resample._resample_poly_np", return_value=None):
            out = yamnet_resample.resample_1d_tf(stereo, 48000, 16000)

        self.assertEqual(out.ndim, 1)
        self.assertNotEqual(out.shape, stereo.shape)
        self.assertEqual(out.dtype, np.float32)

    def test_numpy_allocation_value_error_becomes_memory_limit_error(self) -> None:
        with mock.patch("numpy.asarray", side_effect=ValueError("Unable to allocate 157. MiB for an array")):
            with self.assertRaises(yamnet_resample.TrackAnalyzeMemoryLimitError) as cm:
                yamnet_resample.normalize_audio_for_yamnet([0.0, 1.0])

        self.assertIn("TRACK_ANALYZE_MEMORY_LIMIT", str(cm.exception))
        self.assertIn("Track analysis ran out of RAM while resampling audio", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
