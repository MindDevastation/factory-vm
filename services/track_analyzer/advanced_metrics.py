from __future__ import annotations

from typing import Any

import numpy as np


def compute_quality_metrics(
    *,
    mono_waveform: np.ndarray,
    stereo_waveform: np.ndarray | None,
    sample_rate: int,
    channels_count: int,
    duration_sec: float | None,
    true_peak_dbfs: float | None,
) -> dict[str, Any]:
    duration = float(duration_sec) if duration_sec is not None else float(mono_waveform.size / max(sample_rate, 1))
    rms = float(np.sqrt(np.mean(np.square(mono_waveform)) + 1e-12)) if mono_waveform.size else 0.0
    integrated_lufs = float(-0.691 + (20.0 * np.log10(max(rms, 1e-9))))

    frame_rms = _window_rms(mono_waveform, sample_rate, window_sec=0.4, hop_sec=0.1)
    frame_loudness = -0.691 + (20.0 * np.log10(np.maximum(frame_rms, 1e-9))) if frame_rms.size else np.zeros(0, dtype=np.float32)
    loudness_range_lra = float(np.percentile(frame_loudness, 95) - np.percentile(frame_loudness, 10)) if frame_loudness.size else 0.0

    clipping_ratio = float(np.mean(np.abs(mono_waveform) >= 0.999)) if mono_waveform.size else 0.0

    abs_wave = np.abs(mono_waveform)
    non_silent = abs_wave[abs_wave > 1e-4]
    if non_silent.size:
        noise_floor_estimate = float(20.0 * np.log10(max(float(np.percentile(non_silent, 10)), 1e-9)))
    else:
        noise_floor_estimate = -120.0

    silence_ratio = _silence_ratio(mono_waveform, sample_rate)
    intro_silence_ratio, outro_silence_ratio = _edge_silence_ratios(mono_waveform, sample_rate)

    stereo_width, mono_compatibility = _stereo_metrics(stereo_waveform)

    return {
        "duration_sec": duration,
        "integrated_lufs": integrated_lufs,
        "loudness_range_lra": loudness_range_lra,
        "true_peak_dbfs": float(true_peak_dbfs) if true_peak_dbfs is not None else None,
        "clipping_ratio": clipping_ratio,
        "noise_floor_estimate": noise_floor_estimate,
        "silence_ratio": silence_ratio,
        "intro_silence_ratio": intro_silence_ratio,
        "outro_silence_ratio": outro_silence_ratio,
        "stereo_width": stereo_width,
        "mono_compatibility": mono_compatibility,
        "sample_rate": int(sample_rate),
        "channels_count": int(channels_count),
    }


def compute_dynamics_metrics(*, mono_waveform: np.ndarray, sample_rate: int) -> dict[str, Any]:
    one_sec_energy = _window_energy(mono_waveform, sample_rate, window_sec=1.0, hop_sec=1.0)
    if one_sec_energy.size == 0:
        one_sec_energy = np.zeros(1, dtype=np.float32)

    energy_mean = float(np.mean(one_sec_energy))
    energy_variance = float(np.var(one_sec_energy))
    dynamic_stability = float(np.clip(1.0 - (np.std(one_sec_energy) / (energy_mean + 1e-9)), 0.0, 1.0))

    onset_strength = _onset_strength(mono_waveform, sample_rate)
    duration_sec = max(float(mono_waveform.size / max(sample_rate, 1)), 1e-9)

    onset_flags = onset_strength > (float(np.mean(onset_strength)) + float(np.std(onset_strength))) if onset_strength.size else np.zeros(0, dtype=bool)
    event_count = int(np.count_nonzero(onset_flags))
    event_density = float(event_count / duration_sec)
    transient_density = event_density

    tempo_estimate, tempo_confidence, pulse_strength = _tempo_from_onsets(onset_strength, sample_rate)

    intensity_curve_summary = _intensity_curve_summary(one_sec_energy)

    return {
        "energy_mean": energy_mean,
        "energy_variance": energy_variance,
        "dynamic_stability": dynamic_stability,
        "transient_density": transient_density,
        "pulse_strength": pulse_strength,
        "tempo_estimate": tempo_estimate,
        "tempo_confidence": tempo_confidence,
        "event_density": event_density,
        "intensity_curve_summary": intensity_curve_summary,
    }


def _window_rms(waveform: np.ndarray, sample_rate: int, *, window_sec: float, hop_sec: float) -> np.ndarray:
    window = max(1, int(sample_rate * window_sec))
    hop = max(1, int(sample_rate * hop_sec))
    if waveform.size < window:
        if waveform.size == 0:
            return np.zeros(0, dtype=np.float32)
        padded = np.pad(waveform, (0, window - waveform.size))
        return np.array([float(np.sqrt(np.mean(np.square(padded)) + 1e-12))], dtype=np.float32)

    out: list[float] = []
    for start in range(0, waveform.size - window + 1, hop):
        frame = waveform[start : start + window]
        out.append(float(np.sqrt(np.mean(np.square(frame)) + 1e-12)))
    return np.array(out, dtype=np.float32)


def _window_energy(waveform: np.ndarray, sample_rate: int, *, window_sec: float, hop_sec: float) -> np.ndarray:
    rms = _window_rms(waveform, sample_rate, window_sec=window_sec, hop_sec=hop_sec)
    return np.square(rms).astype(np.float32)


def _silence_ratio(waveform: np.ndarray, sample_rate: int) -> float:
    frame_rms = _window_rms(waveform, sample_rate, window_sec=0.05, hop_sec=0.05)
    if frame_rms.size == 0:
        return 1.0
    return float(np.mean(frame_rms < 0.01))


def _edge_silence_ratios(waveform: np.ndarray, sample_rate: int) -> tuple[float, float]:
    if waveform.size == 0:
        return 1.0, 1.0
    edge_samples = min(max(int(sample_rate * 5), 1), max(waveform.size // 10, 1))
    intro = waveform[:edge_samples]
    outro = waveform[-edge_samples:]
    return _silence_ratio(intro, sample_rate), _silence_ratio(outro, sample_rate)


def _stereo_metrics(stereo_waveform: np.ndarray | None) -> tuple[float, float]:
    if stereo_waveform is None or stereo_waveform.ndim != 2 or stereo_waveform.shape[1] < 2:
        return 0.0, 1.0

    left = stereo_waveform[:, 0]
    right = stereo_waveform[:, 1]
    mid = (left + right) * 0.5
    side = (left - right) * 0.5

    mid_energy = float(np.mean(np.square(mid)) + 1e-12)
    side_energy = float(np.mean(np.square(side)))
    stereo_width = float(np.clip(np.sqrt(side_energy / mid_energy), 0.0, 2.0))

    corr = np.corrcoef(left, right)[0, 1] if left.size > 1 else 1.0
    if not np.isfinite(corr):
        corr = 1.0
    mono_compatibility = float(np.clip(corr, -1.0, 1.0))
    return stereo_width, mono_compatibility


def _onset_strength(waveform: np.ndarray, sample_rate: int) -> np.ndarray:
    frame = max(1, int(0.05 * sample_rate))
    hop = frame
    rms = _window_rms(waveform, sample_rate, window_sec=frame / sample_rate, hop_sec=hop / sample_rate)
    if rms.size <= 1:
        return np.zeros(0, dtype=np.float32)
    return np.maximum(np.diff(rms), 0.0).astype(np.float32)


def _tempo_from_onsets(onset_strength: np.ndarray, sample_rate: int) -> tuple[float, float, float]:
    if onset_strength.size < 4:
        return 0.0, 0.0, 0.0

    hop_sec = 0.05
    min_lag = int((60.0 / 200.0) / hop_sec)
    max_lag = int((60.0 / 40.0) / hop_sec)
    if max_lag <= min_lag:
        return 0.0, 0.0, 0.0

    corr = np.correlate(onset_strength, onset_strength, mode="full")
    corr = corr[corr.size // 2 :]
    lag_slice = corr[min_lag : min(max_lag + 1, corr.size)]
    if lag_slice.size == 0:
        return 0.0, 0.0, 0.0

    best_idx = int(np.argmax(lag_slice))
    best_lag = min_lag + best_idx
    tempo = float(60.0 / (best_lag * hop_sec))

    denom = float(np.max(corr[1:]) + 1e-9) if corr.size > 1 else 1e-9
    pulse_strength = float(np.clip(lag_slice[best_idx] / denom, 0.0, 1.0))
    tempo_confidence = pulse_strength
    return tempo, tempo_confidence, pulse_strength


def _intensity_curve_summary(one_sec_energy: np.ndarray) -> dict[str, float]:
    values = one_sec_energy.astype(np.float64)
    n = values.size
    third = max(1, n // 3)
    start = values[:third]
    middle = values[third : min(2 * third, n)]
    end = values[min(2 * third, n) :]
    if middle.size == 0:
        middle = values
    if end.size == 0:
        end = values[-third:]

    x = np.arange(n, dtype=np.float64)
    slope = float(np.polyfit(x, values, 1)[0]) if n >= 2 else 0.0
    peak_pos = float(np.argmax(values) / max(n - 1, 1))
    convexity = float(np.mean(middle) - ((float(np.mean(start)) + float(np.mean(end))) * 0.5))

    if convexity > 1e-6:
        hint = 1.0
    elif convexity < -1e-6:
        hint = -1.0
    else:
        hint = 0.0

    return {
        "start_mean": float(np.mean(start)),
        "middle_mean": float(np.mean(middle)),
        "end_mean": float(np.mean(end)),
        "linear_slope": slope,
        "peak_position_ratio": peak_pos,
        "convexity_hint": hint,
    }
