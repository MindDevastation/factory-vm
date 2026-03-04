from __future__ import annotations

from typing import Any

import numpy as np


_LABELS = {
    "tonal_sustained",
    "percussive_rhythmic",
    "noisy_distorted",
    "mixed",
}


def _safe_float(value: float | np.floating[Any]) -> float:
    return float(np.clip(float(value), 0.0, 1.0))


def classify_texture(waveform: np.ndarray, sample_rate: int) -> tuple[str, float, dict[str, float]]:
    """Classify coarse texture labels using lightweight numpy-only heuristics."""

    if sample_rate <= 0:
        raise ValueError("sample_rate must be positive")

    x = np.asarray(waveform, dtype=np.float32).reshape(-1)
    if x.size == 0:
        raise ValueError("waveform is empty")

    max_seconds = 60
    max_samples = int(sample_rate * max_seconds)
    if x.size > max_samples:
        x = x[:max_samples]

    peak = float(np.max(np.abs(x)))
    if peak > 1.0:
        x = x / peak

    frame_size = 2048
    hop = 1024
    if x.size < frame_size:
        x = np.pad(x, (0, frame_size - x.size))

    n_frames = 1 + (x.size - frame_size) // hop
    frames = np.lib.stride_tricks.sliding_window_view(x, frame_size)[::hop]
    if frames.shape[0] > n_frames:
        frames = frames[:n_frames]

    eps = 1e-10
    rms = np.sqrt(np.mean(frames * frames, axis=1) + eps)
    rms_mean = float(np.mean(rms))
    rms_std = float(np.std(rms))

    signs = frames >= 0.0
    zcr = np.mean(signs[:, 1:] != signs[:, :-1], axis=1)
    zcr_mean = float(np.mean(zcr))

    window = np.hanning(frame_size).astype(np.float32)
    win_frames = frames * window[None, :]
    mags = np.abs(np.fft.rfft(win_frames, axis=1)) + eps
    freqs = np.fft.rfftfreq(frame_size, d=1.0 / sample_rate).astype(np.float32)

    centroid_hz = np.sum(mags * freqs[None, :], axis=1) / np.sum(mags, axis=1)
    centroid_norm = centroid_hz / (sample_rate / 2.0)

    flatness = np.exp(np.mean(np.log(mags), axis=1)) / np.mean(mags, axis=1)

    rms_diff = np.diff(rms)
    if rms_diff.size == 0:
        onset_proxy = 0.0
    else:
        spike_threshold = float(np.mean(rms_diff) + np.std(rms_diff))
        spikes = np.sum(rms_diff > spike_threshold)
        onset_proxy = float(spikes / rms_diff.size)

    flatness_mean = float(np.mean(flatness))
    centroid_mean_norm = float(np.mean(centroid_norm))

    noisy_strength = _safe_float(((flatness_mean - 0.28) / 0.25 + (zcr_mean - 0.12) / 0.20) / 2.0)
    percussive_strength = _safe_float(((onset_proxy - 0.08) / 0.30 + (centroid_mean_norm - 0.18) / 0.30) / 2.0)
    tonal_strength = _safe_float(((0.22 - flatness_mean) / 0.22 + (0.06 - rms_std) / 0.06) / 2.0)

    scores = {
        "tonal_sustained": tonal_strength,
        "percussive_rhythmic": percussive_strength,
        "noisy_distorted": noisy_strength,
        "mixed": 0.0,
    }

    sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    best_label, best_score = sorted_scores[0]
    second_score = sorted_scores[1][1]
    margin = max(best_score - second_score, 0.0)
    confidence = _safe_float(0.7 * best_score + 0.3 * margin)

    label = best_label if best_score >= 0.25 else "mixed"

    debug = {
        "rms_mean": rms_mean,
        "rms_std": rms_std,
        "zcr_mean": zcr_mean,
        "flatness_mean": flatness_mean,
        "centroid_mean_norm": centroid_mean_norm,
        "onset_proxy": onset_proxy,
        "score_tonal_sustained": tonal_strength,
        "score_percussive_rhythmic": percussive_strength,
        "score_noisy_distorted": noisy_strength,
    }

    if label not in _LABELS:
        label = "mixed"

    return label, confidence, debug
