from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from typing import Any

CORE_ANALYZER_MODE = "ONE_ANALYZER_MANY_PROFILES"

CHANNEL_STRATEGY_PROFILES: tuple[str, ...] = (
    "LONG_FORM_BACKGROUND_MUSIC",
    "DAILY_SINGLE_TRACK_RELEASES",
)

FORMAT_PROFILES: tuple[str, ...] = (
    "LONG_FORM",
    "SINGLE_TRACK",
)


@dataclass(frozen=True)
class AnalyzerProfileBundle:
    channel_strategy_profile: str
    format_profile: str
    weighting_hooks: dict[str, float]
    baseline_hooks: dict[str, Any]
    prediction_hooks: dict[str, Any]
    recommendation_hooks: dict[str, Any]
    planning_hooks: dict[str, Any]


_CHANNEL_HOOKS: dict[str, dict[str, Any]] = {
    "LONG_FORM_BACKGROUND_MUSIC": {
        "weighting": {"retention": 1.3, "watch_time": 1.25, "ctr": 0.9, "subscribers": 0.95},
        "baseline": {"window_bias": "LONG_HORIZON", "lookback_days": 56, "cohort_focus": "D28"},
        "prediction": {"primary_target": "WATCH_TIME_GROWTH", "confidence_bias": "STABILITY_WEIGHTED"},
        "recommendation": {"priority_theme": "SESSION_DEPTH", "action_bias": "PACKAGING_ITERATION"},
        "planning": {"cadence_mode": "LOWER_FREQUENCY", "risk_bias": "DECAY_RISK"},
    },
    "DAILY_SINGLE_TRACK_RELEASES": {
        "weighting": {"retention": 0.95, "watch_time": 0.9, "ctr": 1.2, "subscribers": 1.15},
        "baseline": {"window_bias": "SHORT_HORIZON", "lookback_days": 28, "cohort_focus": "D7"},
        "prediction": {"primary_target": "CTR_AND_EARLY_TRACTION", "confidence_bias": "VELOCITY_WEIGHTED"},
        "recommendation": {"priority_theme": "RELEASE_MOMENTUM", "action_bias": "TIMING_AND_TITLE"},
        "planning": {"cadence_mode": "HIGHER_FREQUENCY", "risk_bias": "FATIGUE_RISK"},
    },
}

_FORMAT_HOOKS: dict[str, dict[str, Any]] = {
    "LONG_FORM": {
        "weighting": {"average_view_duration": 1.25, "retention": 1.15},
        "baseline": {"benchmark_family": "LONG_FORM_RETENTION"},
        "prediction": {"variance_tolerance": "MEDIUM"},
        "recommendation": {"metadata_focus": "DISCOVERY_PLUS_SESSION"},
        "planning": {"slot_length_pref": "EXTENDED"},
    },
    "SINGLE_TRACK": {
        "weighting": {"average_view_duration": 0.9, "retention": 0.95, "ctr": 1.15},
        "baseline": {"benchmark_family": "SINGLE_TRACK_LAUNCH"},
        "prediction": {"variance_tolerance": "HIGH"},
        "recommendation": {"metadata_focus": "LAUNCH_DISCOVERY"},
        "planning": {"slot_length_pref": "COMPACT"},
    },
}


def _require_known(value: str, *, allowed: tuple[str, ...], field: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise ValueError(f"unknown {field}: {value}")
    return normalized


def _combine_weighting(channel_weights: dict[str, float], format_weights: dict[str, float]) -> dict[str, float]:
    keys = sorted(set(channel_weights) | set(format_weights))
    out: dict[str, float] = {}
    for key in keys:
        out[key] = float(channel_weights.get(key, 1.0)) * float(format_weights.get(key, 1.0))
    return out


def resolve_profile_bundle(*, channel_strategy_profile: str, format_profile: str) -> AnalyzerProfileBundle:
    strategy = _require_known(channel_strategy_profile, allowed=CHANNEL_STRATEGY_PROFILES, field="channel_strategy_profile")
    fmt = _require_known(format_profile, allowed=FORMAT_PROFILES, field="format_profile")

    ch = _CHANNEL_HOOKS[strategy]
    fm = _FORMAT_HOOKS[fmt]

    return AnalyzerProfileBundle(
        channel_strategy_profile=strategy,
        format_profile=fmt,
        weighting_hooks=_combine_weighting(ch["weighting"], fm["weighting"]),
        baseline_hooks={**ch["baseline"], **fm["baseline"]},
        prediction_hooks={**ch["prediction"], **fm["prediction"]},
        recommendation_hooks={**ch["recommendation"], **fm["recommendation"]},
        planning_hooks={**ch["planning"], **fm["planning"]},
    )


def profile_hook_fingerprint(bundle: AnalyzerProfileBundle) -> str:
    payload = {
        "channel_strategy_profile": bundle.channel_strategy_profile,
        "format_profile": bundle.format_profile,
        "weighting_hooks": bundle.weighting_hooks,
        "baseline_hooks": bundle.baseline_hooks,
        "prediction_hooks": bundle.prediction_hooks,
        "recommendation_hooks": bundle.recommendation_hooks,
        "planning_hooks": bundle.planning_hooks,
    }
    return sha1(repr(payload).encode("utf-8")).hexdigest()


def build_profile_registry_contract() -> dict[str, Any]:
    samples = [
        resolve_profile_bundle(channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC", format_profile="LONG_FORM"),
        resolve_profile_bundle(channel_strategy_profile="DAILY_SINGLE_TRACK_RELEASES", format_profile="SINGLE_TRACK"),
    ]
    return {
        "core_analyzer_mode": CORE_ANALYZER_MODE,
        "channel_strategy_profiles": list(CHANNEL_STRATEGY_PROFILES),
        "format_profiles": list(FORMAT_PROFILES),
        "foundations_affected": ["weighting", "baseline", "prediction", "recommendation", "planning"],
        "sample_hook_fingerprints": [profile_hook_fingerprint(bundle) for bundle in samples],
    }
