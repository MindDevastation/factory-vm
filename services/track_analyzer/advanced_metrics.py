from __future__ import annotations

from typing import Any

import numpy as np


EPS = 1e-9
SILENCE_RMS_THRESHOLD = 0.01


def _safe_db(value: float) -> float:
    return float(20.0 * np.log10(max(value, EPS)))


def _window_rms(signal: np.ndarray, frame_size: int, hop_size: int) -> np.ndarray:
    if signal.size == 0 or frame_size <= 0 or hop_size <= 0:
        return np.zeros(0, dtype=np.float32)
    if signal.size < frame_size:
        frame = np.pad(signal, (0, frame_size - signal.size))
        return np.array([float(np.sqrt(np.mean(np.square(frame)) + EPS))], dtype=np.float32)

    values: list[float] = []
    for start in range(0, signal.size - frame_size + 1, hop_size):
        frame = signal[start : start + frame_size]
        values.append(float(np.sqrt(np.mean(np.square(frame)) + EPS)))
    return np.array(values, dtype=np.float32)


def compute_quality_metrics(
    *,
    mono_waveform: np.ndarray,
    stereo_waveform: np.ndarray | None,
    sample_rate: int,
    channels_count: int,
    duration_sec: float | None,
    true_peak_dbfs: float | None,
) -> dict[str, Any]:
    mono = mono_waveform.astype(np.float32, copy=False)
    abs_mono = np.abs(mono)
    clip_ratio = float(np.mean(abs_mono >= 0.999)) if mono.size else 0.0

    rms = float(np.sqrt(np.mean(np.square(mono)) + EPS)) if mono.size else 0.0
    integrated_lufs = _safe_db(rms)

    frame_size = max(1, int(sample_rate * 0.4))
    hop_size = max(1, int(sample_rate * 0.1))
    frame_rms = _window_rms(mono, frame_size, hop_size)
    frame_db = np.array([_safe_db(float(v)) for v in frame_rms], dtype=np.float32) if frame_rms.size else np.zeros(0, dtype=np.float32)

    if frame_db.size:
        loudness_range_lra = float(np.percentile(frame_db, 95) - np.percentile(frame_db, 10))
        noise_floor_estimate = float(np.percentile(frame_db, 10))
        silent_frames = frame_rms < SILENCE_RMS_THRESHOLD
        silence_ratio = float(np.mean(silent_frames))
        edge_frames = max(1, int(np.ceil(frame_rms.size * 0.1)))
        intro_silence_ratio = float(np.mean(silent_frames[:edge_frames]))
        outro_silence_ratio = float(np.mean(silent_frames[-edge_frames:]))
    else:
        loudness_range_lra = 0.0
        noise_floor_estimate = -120.0
        silence_ratio = 1.0
        intro_silence_ratio = 1.0
        outro_silence_ratio = 1.0

    stereo_width = 0.0
    mono_compatibility = 1.0
    if stereo_waveform is not None and stereo_waveform.size:
        left = stereo_waveform[:, 0]
        right = stereo_waveform[:, 1]
        side = left - right
        mid = left + right
        stereo_width = float(np.clip(np.mean(np.abs(side)) / (np.mean(np.abs(mid)) + EPS), 0.0, 1.0))
        cancellation_ratio = float(np.mean(np.abs(side)) / (2.0 * np.mean(np.abs(mid * 0.5)) + EPS))
        mono_compatibility = float(np.clip(1.0 - cancellation_ratio, 0.0, 1.0))

    return {
        "duration_sec": float(duration_sec) if duration_sec is not None else None,
        "integrated_lufs": float(integrated_lufs),
        "loudness_range_lra": float(max(loudness_range_lra, 0.0)),
        "true_peak_dbfs": float(true_peak_dbfs) if true_peak_dbfs is not None else None,
        "clipping_ratio": float(np.clip(clip_ratio, 0.0, 1.0)),
        "noise_floor_estimate": float(noise_floor_estimate),
        "silence_ratio": float(np.clip(silence_ratio, 0.0, 1.0)),
        "intro_silence_ratio": float(np.clip(intro_silence_ratio, 0.0, 1.0)),
        "outro_silence_ratio": float(np.clip(outro_silence_ratio, 0.0, 1.0)),
        "stereo_width": float(np.clip(stereo_width, 0.0, 1.0)),
        "mono_compatibility": float(np.clip(mono_compatibility, 0.0, 1.0)),
        "sample_rate": int(sample_rate),
        "channels_count": int(channels_count),
    }


def _one_second_energy(signal: np.ndarray, sample_rate: int) -> np.ndarray:
    win = max(1, sample_rate)
    values: list[float] = []
    for start in range(0, signal.size, win):
        chunk = signal[start : start + win]
        if chunk.size == 0:
            continue
        values.append(float(np.mean(np.square(chunk))))
    return np.array(values, dtype=np.float32)


def _detect_onsets(signal: np.ndarray, sample_rate: int) -> np.ndarray:
    hop = max(1, int(0.02 * sample_rate))
    frame = max(hop, int(0.04 * sample_rate))
    env = _window_rms(np.abs(signal), frame, hop)
    if env.size <= 2:
        return np.zeros(0, dtype=np.float32)

    novelty = np.diff(env, prepend=env[0])
    threshold = float(np.median(novelty) + np.std(novelty))
    peaks: list[float] = []
    for idx in range(1, novelty.size - 1):
        value = float(novelty[idx])
        if value > threshold and value >= float(novelty[idx - 1]) and value >= float(novelty[idx + 1]):
            peaks.append(float(idx * hop / sample_rate))
    return np.array(peaks, dtype=np.float32)


def _intensity_curve_summary(energy: np.ndarray) -> dict[str, float | str]:
    if energy.size == 0:
        return {
            "start_mean": 0.0,
            "middle_mean": 0.0,
            "end_mean": 0.0,
            "linear_slope": 0.0,
            "peak_position_ratio": 0.0,
            "convexity_hint": "flat",
        }

    chunks = np.array_split(energy, 3)
    start_mean = float(np.mean(chunks[0])) if chunks[0].size else 0.0
    middle_mean = float(np.mean(chunks[1])) if chunks[1].size else start_mean
    end_mean = float(np.mean(chunks[2])) if chunks[2].size else middle_mean

    x = np.arange(energy.size, dtype=np.float32)
    slope = float(np.polyfit(x, energy, 1)[0]) if energy.size >= 2 else 0.0
    peak_ratio = float(np.argmax(energy) / max(energy.size - 1, 1))

    convexity = 0.0
    if energy.size >= 3:
        convexity = float(np.polyfit(x, energy, 2)[0])
    if convexity > 1e-4:
        hint = "convex"
    elif convexity < -1e-4:
        hint = "concave"
    else:
        hint = "flat"

    return {
        "start_mean": start_mean,
        "middle_mean": middle_mean,
        "end_mean": end_mean,
        "linear_slope": slope,
        "peak_position_ratio": float(np.clip(peak_ratio, 0.0, 1.0)),
        "convexity_hint": hint,
    }


def compute_dynamics_metrics(*, mono_waveform: np.ndarray, sample_rate: int, duration_sec: float | None) -> dict[str, Any]:
    energy = _one_second_energy(mono_waveform.astype(np.float32, copy=False), sample_rate)
    energy_mean = float(np.mean(energy)) if energy.size else 0.0
    energy_var = float(np.var(energy)) if energy.size else 0.0
    dynamic_stability = float(np.clip(1.0 - (np.sqrt(energy_var) / (energy_mean + EPS)), 0.0, 1.0))

    onsets = _detect_onsets(mono_waveform, sample_rate)
    total_duration = float(duration_sec) if duration_sec is not None else float(mono_waveform.size / max(sample_rate, 1))
    total_duration = max(total_duration, EPS)

    transient_density = float(onsets.size / total_duration)
    event_density = transient_density

    pulse_strength = 0.0
    if energy.size >= 2:
        centered = energy - np.mean(energy)
        acf = np.correlate(centered, centered, mode="full")
        right = acf[acf.size // 2 :]
        denom = float(right[0]) if right.size else 0.0
        if denom > EPS and right.size > 1:
            pulse_strength = float(np.clip(np.max(right[1: min(9, right.size)]) / denom, 0.0, 1.0))

    tempo_estimate = 0.0
    tempo_confidence = 0.0
    if onsets.size >= 2:
        intervals = np.diff(onsets)
        valid = intervals[(intervals > 0.25) & (intervals < 1.5)]
        if valid.size:
            median = float(np.median(valid))
            tempo_estimate = float(np.clip(60.0 / median, 40.0, 220.0))
            close = np.abs(valid - median) <= 0.08
            tempo_confidence = float(np.clip(np.mean(close), 0.0, 1.0))

    return {
        "energy_mean": energy_mean,
        "energy_variance": energy_var,
        "dynamic_stability": dynamic_stability,
        "transient_density": transient_density,
        "pulse_strength": pulse_strength,
        "tempo_estimate": tempo_estimate,
        "tempo_confidence": tempo_confidence,
        "event_density": event_density,
        "intensity_curve_summary": _intensity_curve_summary(energy),
    }
