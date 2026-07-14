from __future__ import annotations

import importlib
import inspect
import logging
import math
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

TRACK_ANALYZE_MEMORY_LIMIT = "TRACK_ANALYZE_MEMORY_LIMIT"
TRACK_ANALYZE_MEMORY_GUIDANCE = (
    "Track analysis ran out of RAM while resampling audio; try shorter input, "
    "mono/16k preconversion, or lower concurrent workers."
)


class TrackAnalyzeMemoryLimitError(RuntimeError):
    """Raised when audio normalization/resampling cannot allocate safely."""

    def __init__(self, detail: str | None = None) -> None:
        suffix = f": {detail}" if detail else ""
        super().__init__(f"{TRACK_ANALYZE_MEMORY_LIMIT}: {TRACK_ANALYZE_MEMORY_GUIDANCE}{suffix}")


def _tf_convert_float32(tf: Any, value: Any) -> Any:
    try:
        return tf.convert_to_tensor(value, dtype=tf.float32)
    except TypeError:
        return tf.cast(tf.convert_to_tensor(value), tf.float32)


def _tf_convert_accepts_dtype(tf: Any) -> bool:
    try:
        return "dtype" in inspect.signature(tf.convert_to_tensor).parameters
    except (TypeError, ValueError):
        return True


def _tensorflow_io_audio_module() -> Any | None:
    try:
        tfio = importlib.import_module("tensorflow_io")
    except Exception:
        return None
    return getattr(tfio, "audio", None)


def _is_numpy_allocation_error(exc: BaseException) -> bool:
    if isinstance(exc, MemoryError):
        return True
    if isinstance(exc, ValueError):
        message = str(exc).lower()
        return "unable to allocate" in message or "array is too big" in message
    return False


def _raise_memory_limit(exc: BaseException) -> None:
    raise TrackAnalyzeMemoryLimitError(f"{exc.__class__.__name__}: {exc}") from exc


def normalize_audio_for_yamnet(waveform: Any) -> np.ndarray:
    """Return a mono float32 1-D buffer suitable for YAMNet/resampling.

    Downmixing happens before any expensive resampling so stereo inputs do not
    carry a two-channel allocation through the analysis path.
    """

    try:
        arr = np.asarray(waveform)
        if arr.ndim == 0:
            arr = arr.reshape(1)
        if arr.ndim > 1:
            if arr.shape[-1] in (1, 2):
                arr = arr.astype(np.float32, copy=False)
                arr = arr.mean(axis=-1, dtype=np.float32)
            elif arr.shape[0] in (1, 2):
                arr = arr.astype(np.float32, copy=False)
                arr = arr.mean(axis=0, dtype=np.float32)
            else:
                arr = arr.reshape(-1)
        arr = np.asarray(arr, dtype=np.float32).reshape(-1)
    except Exception as exc:
        if _is_numpy_allocation_error(exc):
            _raise_memory_limit(exc)
        raise
    return arr


def _resample_poly_np(x_np: np.ndarray, src: int, dst: int) -> np.ndarray | None:
    try:
        scipy_signal = importlib.import_module("scipy.signal")
        resample_poly = getattr(scipy_signal, "resample_poly", None)
    except Exception:
        return None
    if not callable(resample_poly):
        return None

    gcd = math.gcd(src, dst)
    up = dst // gcd
    down = src // gcd
    try:
        return np.asarray(resample_poly(x_np, up, down), dtype=np.float32).reshape(-1)
    except Exception as exc:
        if _is_numpy_allocation_error(exc):
            _raise_memory_limit(exc)
        raise


def _bounded_linear_resample_np(x_np: np.ndarray, src: int, dst: int) -> np.ndarray:
    new_len = int(round(len(x_np) * float(dst) / float(src)))
    if new_len <= 0:
        new_len = 1
    try:
        # Keep the fallback bounded to mono float32 coordinates and output. It is
        # less fancy than FFT resampling but avoids the large float64/stereo
        # intermediates that caused local worker failures.
        xp = np.arange(len(x_np), dtype=np.float32)
        x_new = np.interp(
            np.linspace(0, len(x_np) - 1, new_len, dtype=np.float32),
            xp,
            x_np,
        ).astype(np.float32, copy=False)
    except Exception as exc:
        if _is_numpy_allocation_error(exc):
            _raise_memory_limit(exc)
        raise
    return x_new


def resample_1d_tf(x: Any, src_rate: int, dst_rate: int) -> Any:
    """Resample a waveform tensor across TensorFlow variants.

    The YAMNet path normalizes to mono float32 before resampling. TensorFlow or
    tensorflow-io resamplers are used when available; otherwise scipy's
    polyphase resampler is preferred before a bounded numpy interpolation
    fallback.
    """
    import tensorflow as tf  # type: ignore

    src = int(src_rate)
    dst = int(dst_rate)
    if src <= 0 or dst <= 0:
        raise ValueError("sample rates must be positive")

    try:
        x_np = normalize_audio_for_yamnet(x)
    except Exception as exc:
        if _is_numpy_allocation_error(exc):
            _raise_memory_limit(exc)
        raise

    if src == dst:
        return _tf_convert_float32(tf, x_np)

    target_len = int(round(len(x_np) * float(dst) / float(src)))
    if target_len <= 0:
        target_len = 1

    x_tf = _tf_convert_float32(tf, x_np)
    signal_mod = getattr(tf, "signal", None)
    tf_signal_resample = getattr(signal_mod, "resample", None)
    if callable(tf_signal_resample):
        try:
            params = inspect.signature(tf_signal_resample).parameters
        except (TypeError, ValueError):
            params = None
        if params is None or len(params) >= 2:
            try:
                return tf.cast(tf_signal_resample(x_tf, target_len), tf.float32)
            except Exception as exc:
                if _is_numpy_allocation_error(exc):
                    _raise_memory_limit(exc)
                raise

    tfio_audio = _tensorflow_io_audio_module()
    if tfio_audio is not None and hasattr(tfio_audio, "resample"):
        try:
            return tf.cast(tfio_audio.resample(x_tf, rate_in=src, rate_out=dst), tf.float32)
        except Exception as exc:
            if _is_numpy_allocation_error(exc):
                _raise_memory_limit(exc)
            raise

    poly = _resample_poly_np(x_np, src, dst) if _tf_convert_accepts_dtype(tf) else None
    if poly is not None:
        return _tf_convert_float32(tf, poly)

    logger.info("using numpy resample fallback")
    try:
        x_new = _bounded_linear_resample_np(x_np, src, dst)
    except Exception as exc:
        if isinstance(exc, TrackAnalyzeMemoryLimitError):
            raise
        raise RuntimeError(
            "RESAMPLE_UNSUPPORTED_TF: tensorflow.signal.resample and tensorflow_io.audio.resample "
            f"are unavailable in tensorflow=={getattr(tf, '__version__', 'unknown')}, and numpy fallback failed: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc

    return _tf_convert_float32(tf, x_new)
