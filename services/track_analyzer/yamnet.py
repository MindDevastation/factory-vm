from __future__ import annotations

from pathlib import Path
from typing import Any

_IMPORT_ERROR: Exception | None = None
try:
    import tensorflow as tf  # type: ignore
    import tensorflow_hub as hub  # type: ignore
except Exception as exc:  # pragma: no cover - exercised via is_available contract
    tf = None  # type: ignore[assignment]
    hub = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc


class YAMNetUnavailableError(RuntimeError):
    pass


_YAMNET_HANDLE = "https://tfhub.dev/google/yamnet/1"
_YAMNET_MODEL: Any | None = None
_YAMNET_CLASS_NAMES: list[str] | None = None


def is_available() -> bool:
    return tf is not None and hub is not None


def _require_available() -> None:
    if not is_available():
        raise YAMNetUnavailableError("YAMNET_NOT_INSTALLED") from _IMPORT_ERROR


def _load_model() -> Any:
    global _YAMNET_MODEL
    _require_available()
    if _YAMNET_MODEL is None:
        _YAMNET_MODEL = hub.load(_YAMNET_HANDLE)
    return _YAMNET_MODEL


def _load_class_names() -> list[str]:
    global _YAMNET_CLASS_NAMES
    _require_available()
    if _YAMNET_CLASS_NAMES is not None:
        return _YAMNET_CLASS_NAMES

    model = _load_model()
    class_map_path = model.class_map_path().numpy().decode("utf-8")
    names: list[str] = []
    with tf.io.gfile.GFile(class_map_path) as f:
        _ = f.readline()
        for line in f:
            cols = line.strip().split(",")
            if len(cols) >= 3:
                names.append(cols[2])

    _YAMNET_CLASS_NAMES = names
    return names


def _resample_to_16k_mono(waveform: Any, sample_rate: Any) -> Any:
    if int(sample_rate) == 16000:
        return waveform

    wav_len = tf.shape(waveform)[0]
    target_len = tf.cast(tf.math.round(tf.cast(wav_len, tf.float32) * (16000.0 / tf.cast(sample_rate, tf.float32))), tf.int32)
    resampled = tf.signal.resample(waveform, target_len)
    return tf.cast(resampled, tf.float32)


def analyze_with_yamnet(wav_path: str | Path, *, top_k: int = 5) -> dict[str, Any]:
    _require_available()
    model = _load_model()
    class_names = _load_class_names()

    audio_bytes = tf.io.read_file(str(wav_path))
    waveform, sample_rate = tf.audio.decode_wav(audio_bytes, desired_channels=1)
    waveform = tf.squeeze(waveform, axis=-1)
    sample_rate = int(sample_rate.numpy())
    waveform_16k = _resample_to_16k_mono(waveform, sample_rate)

    scores, _embeddings, _spectrogram = model(waveform_16k)
    mean_scores = tf.reduce_mean(scores, axis=0).numpy()

    top_n = max(1, int(top_k))
    top_indices = mean_scores.argsort()[-top_n:][::-1]
    top_classes = [
        {
            "label": class_names[int(idx)] if int(idx) < len(class_names) else f"class_{int(idx)}",
            "score": float(mean_scores[int(idx)]),
        }
        for idx in top_indices
    ]

    probs = {
        "speech": 0.0,
        "voice": 0.0,
        "music": 0.0,
    }
    for idx, label in enumerate(class_names):
        lowered = label.lower()
        score = float(mean_scores[idx])
        if "speech" in lowered:
            probs["speech"] = max(probs["speech"], score)
        if "voice" in lowered or "vocal" in lowered:
            probs["voice"] = max(probs["voice"], score)
        if "music" in lowered:
            probs["music"] = max(probs["music"], score)

    return {
        "top_classes": top_classes,
        "probabilities": probs,
    }
