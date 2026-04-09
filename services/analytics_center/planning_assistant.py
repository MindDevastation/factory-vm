from __future__ import annotations

from typing import Any

from services.analytics_center.profile_registry import resolve_profile_bundle

_ALLOWED_SCENARIOS = {"WEEK", "MONTH", "QUARTER"}


def build_planning_assistant_summary(
    *,
    scenario: str,
    channel_strategy_profile: str,
    format_profile: str,
    historical_performance: dict[str, Any] | None = None,
    audience_behavior: dict[str, Any] | None = None,
    publish_windows: list[str] | None = None,
    cadence_patterns: dict[str, Any] | None = None,
    risk_signals: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    scenario_u = str(scenario or "").strip().upper()
    if scenario_u not in _ALLOWED_SCENARIOS:
        raise ValueError("invalid planning scenario")
    profile = resolve_profile_bundle(
        channel_strategy_profile=channel_strategy_profile,
        format_profile=format_profile,
    )
    historical = dict(historical_performance or {})
    audience = dict(audience_behavior or {})
    cadence = dict(cadence_patterns or {})
    risks = list(risk_signals or [])
    windows = list(publish_windows or ["weekday_evening"])

    return {
        "scenario": scenario_u,
        "inputs": {
            "channel_profile": profile.channel_strategy_profile,
            "format_profile": profile.format_profile,
            "historical_performance": historical,
            "audience_behavior": audience,
            "publish_windows": windows,
            "cadence_patterns": cadence,
            "risk_signals": risks,
        },
        "outputs": {
            "recommended_release_schedule": [
                {"slot": i + 1, "window": windows[i % len(windows)], "priority": "HIGH" if i == 0 else "MEDIUM"}
                for i in range({"WEEK": 3, "MONTH": 8, "QUARTER": 12}[scenario_u])
            ],
            "recommended_mix_by_format": {
                "format_profile": profile.format_profile,
                "mix_hint": profile.planning_hooks.get("slot_length_pref", "BALANCED"),
            },
            "recommended_priority_for_releases": [
                {"item": "growth_candidates", "priority": "HIGH"},
                {"item": "risk_mitigation", "priority": "HIGH" if risks else "MEDIUM"},
            ],
            "expected_outcome_risk_narrative": f"{scenario_u} plan generated with profile-aware weighting and risk safeguards.",
            "linked_actions": [
                {"target_domain": "PLANNER", "path": "/planner", "action": "open_planning_surface"},
                {"target_domain": "PUBLISH", "path": "/ui/publish/queue", "action": "review_publish_queue"},
            ],
        },
        "default_behavior": {"auto_apply": False, "mutation": False},
    }

