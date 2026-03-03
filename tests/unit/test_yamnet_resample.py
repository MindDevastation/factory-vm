from __future__ import annotations

import importlib
import sys
import types
import unittest
from unittest import mock


class _FakeMath:
    @staticmethod
    def round(value):
        return round(value)


class _FakeTF(types.SimpleNamespace):
    float32 = "float32"
    int32 = "int32"

    def __init__(self, *, signal_resample=None, version="2.test"):
        signal = types.SimpleNamespace() if signal_resample is None else types.SimpleNamespace(resample=signal_resample)
        super().__init__(__version__=version, signal=signal, math=_FakeMath())

    @staticmethod
    def shape(x):
        return [len(x)]

    @staticmethod
    def cast(value, dtype):
        if dtype == "int32":
            return int(value)
        return value

    @staticmethod
    def convert_to_tensor(value):
        return value


class YamnetResampleTests(unittest.TestCase):
    def _load_module(self):
        import services.track_analyzer.yamnet_resample as mod

        return importlib.reload(mod)

    def test_uses_tf_signal_resample_when_available(self) -> None:
        seen = {}

        def _signal_resample(x, target_len):
            seen["args"] = (x, target_len)
            return [0.1] * int(target_len)

        fake_tf = _FakeTF(signal_resample=_signal_resample, version="2.15.test")
        with mock.patch.dict(sys.modules, {"tensorflow": fake_tf}, clear=False):
            mod = self._load_module()
            out = mod.resample_1d_tf([1.0, 2.0, 3.0, 4.0], 32000, 16000)

        self.assertEqual(seen["args"][1], 2)
        self.assertEqual(len(out), 2)

    def test_uses_tensorflow_io_when_tf_signal_resample_missing(self) -> None:
        fake_tf = _FakeTF(signal_resample=None, version="2.16.1")
        called = {}

        class _FakeAudio:
            @staticmethod
            def resample(x, rate_in, rate_out):
                called["args"] = (x, rate_in, rate_out)
                return [9.0]

        fake_tfio = types.SimpleNamespace(audio=_FakeAudio())
        with mock.patch.dict(sys.modules, {"tensorflow": fake_tf, "tensorflow_io": fake_tfio}, clear=False):
            mod = self._load_module()
            out = mod.resample_1d_tf([1.0, 2.0, 3.0, 4.0], 48000, 16000)

        self.assertEqual(called["args"][1:], (48000, 16000))
        self.assertEqual(out, [9.0])

    def test_uses_numpy_fallback_when_tf_signal_and_tfio_missing(self) -> None:
        fake_tf = _FakeTF(signal_resample=None, version="2.16.1")
        with mock.patch.dict(sys.modules, {"tensorflow": fake_tf}, clear=False):
            mod = self._load_module()
            with self.assertLogs("services.track_analyzer.yamnet_resample", level="INFO") as logs:
                with mock.patch.object(mod, "_tensorflow_io_audio_module", return_value=None):
                    out = mod.resample_1d_tf([1.0, 2.0, 3.0, 4.0], 44100, 16000)

        expected_len = round(4 * 16000 / 44100)
        self.assertEqual(len(out), expected_len)
        self.assertIn("using numpy resample fallback", "\n".join(logs.output))

    def test_raises_clear_error_when_numpy_fallback_fails(self) -> None:
        fake_tf = _FakeTF(signal_resample=None, version="2.16.1")
        with mock.patch.dict(sys.modules, {"tensorflow": fake_tf}, clear=False):
            mod = self._load_module()
            with mock.patch.object(mod, "_tensorflow_io_audio_module", return_value=None):
                with mock.patch.object(mod.np, "interp", side_effect=RuntimeError("no numpy")):
                    with self.assertRaises(RuntimeError) as ctx:
                        mod.resample_1d_tf([1.0, 2.0, 3.0, 4.0], 44100, 16000)

        msg = str(ctx.exception)
        self.assertIn("RESAMPLE_UNSUPPORTED_TF", msg)
        self.assertIn("tensorflow==2.16.1", msg)
        self.assertIn("numpy fallback failed", msg)


if __name__ == "__main__":
    unittest.main()
