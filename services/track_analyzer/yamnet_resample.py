from __future__ import annotations

import importlib
import inspect
import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


def _tensorflow_io_audio_module() -> Any | None:
    try:
        tfio = importlib.import_module("tensorflow_io")
    except Exception:
        return None
    return getattr(tfio, "audio", None)


def resample_1d_tf(x: Any, src_rate: int, dst_rate: int) -> Any:
    """Resample a 1D waveform tensor across TensorFlow variants.

    Uses tf.signal.resample when available, otherwise falls back to
    tensorflow_io.audio.resample when installed.
    """
    import tensorflow as tf  # type: ignore

    src = int(src_rate)
    dst = int(dst_rate)
    if src == dst:
        return tf.cast(x, tf.float32)

    wav_len = tf.shape(x)[0]
    target_len = tf.cast(
        tf.math.round(tf.cast(wav_len, tf.float32) * (float(dst) / float(src))),
        tf.int32,
    )

    signal_mod = getattr(tf, "signal", None)
    tf_signal_resample = getattr(signal_mod, "resample", None)
    if callable(tf_signal_resample):
        try:
            params = inspect.signature(tf_signal_resample).parameters
        except (TypeError, ValueError):
            params = None
        if params is None or len(params) >= 2:
            return tf.cast(tf_signal_resample(x, target_len), tf.float32)

    tfio_audio = _tensorflow_io_audio_module()
    if tfio_audio is not None and hasattr(tfio_audio, "resample"):
        return tf.cast(tfio_audio.resample(x, rate_in=src, rate_out=dst), tf.float32)

    logger.info("using numpy resample fallback")
    try:
        x_np = np.asarray(x, dtype=np.float32).reshape(-1)
        new_len = int(round(len(x_np) * float(dst) / float(src)))
        if new_len <= 0:
            new_len = 1
        xp = np.arange(len(x_np), dtype=np.float32)
        x_new = np.interp(
            np.linspace(0, len(x_np) - 1, new_len, dtype=np.float32),
            xp,
            x_np,
        ).astype(np.float32)
    except Exception as exc:
        raise RuntimeError(
            "RESAMPLE_UNSUPPORTED_TF: tensorflow.signal.resample and tensorflow_io.audio.resample "
            f"are unavailable in tensorflow=={getattr(tf, '__version__', 'unknown')}, and numpy fallback failed: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc

    return tf.convert_to_tensor(x_new)
