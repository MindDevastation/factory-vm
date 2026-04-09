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

    slot_count = {"WEEK": 3, "MONTH": 8, "QUARTER": 12}[scenario_u]
    schedule = [
        {
            "slot": i + 1,
            "window": windows[i % len(windows)],
            "priority": "HIGH" if i < 2 else "MEDIUM",
            "focus": "growth" if i % 2 == 0 else "retention",
        }
        for i in range(slot_count)
    ]
    plan_horizon_days = {"WEEK": 7, "MONTH": 30, "QUARTER": 90}[scenario_u]
    expected_mix = {
        "long_form": 0.7 if profile.format_profile == "LONG_FORM" else 0.4,
        "short_form": 0.3 if profile.format_profile == "LONG_FORM" else 0.6,
    }

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
            "recommended_release_schedule": schedule,
            "planning_horizon_days": plan_horizon_days,
            "scenario_targets": {
                "target_slots": slot_count,
                "risk_budget": "LOW" if risks else "MEDIUM",
                "primary_goal": "growth_and_retention_balance",
            },
            "recommended_mix_by_format": {
                "format_profile": profile.format_profile,
                "mix_hint": profile.planning_hooks.get("slot_length_pref", "BALANCED"),
                "mix_ratio": expected_mix,
            },
            "recommended_priority_for_releases": [
                {"item": "growth_candidates", "priority": "HIGH"},
                {"item": "risk_mitigation", "priority": "HIGH" if risks else "MEDIUM"},
            ],
            "expected_outcome_risk_narrative": f"{scenario_u} plan generated with profile-aware weighting and risk safeguards.",
            "execution_checklist": [
                {"step": "review_schedule_slots", "required": True},
                {"step": "review_risk_signals", "required": True},
                {"step": "apply_to_planner_manually", "required": True},
            ],
            "linked_actions": [
                {
                    "target_domain": "PLANNER",
                    "path": "/planner",
                    "action": "open_planning_surface",
                    "mutation": False,
                    "auto_apply": False,
                },
                {
                    "target_domain": "PUBLISH",
                    "path": "/ui/publish/queue",
                    "action": "review_publish_queue",
                    "mutation": False,
                    "auto_apply": False,
                },
            ],
        },
        "default_behavior": {"auto_apply": False, "mutation": False},
    }
