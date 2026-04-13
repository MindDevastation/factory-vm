from __future__ import annotations

import json
from typing import Any

from services.analytics_center.helpers import canonicalize_scope_ref
from services.analytics_center.profile_registry import resolve_profile_bundle

_ALLOWED_SCENARIOS = {"WEEK", "MONTH", "QUARTER"}
_ALLOWED_SCOPE_TYPES = {"CHANNEL", "RELEASE", "BATCH_MONTH"}


def _normalize_scope(*, conn: Any, scope_type: str | None, scope_ref: str | None) -> tuple[str | None, str | None, str | None]:
    normalized_scope_type = str(scope_type or "").strip().upper()
    if normalized_scope_type not in _ALLOWED_SCOPE_TYPES:
        return None, None, None
    ref = canonicalize_scope_ref(conn, scope_type=normalized_scope_type, scope_ref=str(scope_ref or ""))
    entity_type = "BATCH" if normalized_scope_type == "BATCH_MONTH" else normalized_scope_type
    return normalized_scope_type, str(ref), entity_type


def _safe_json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def _safe_json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return list(parsed)
    return []


def _derive_grounded_inputs(
    conn: Any,
    *,
    scope_type: str | None,
    scope_ref: str | None,
) -> dict[str, Any]:
    normalized_scope_type, normalized_scope_ref, entity_type = _normalize_scope(conn=conn, scope_type=scope_type, scope_ref=scope_ref)
    if normalized_scope_type is None or normalized_scope_ref is None or entity_type is None:
        return {}

    external_rows = conn.execute(
        """
        SELECT payload_json, captured_at
        FROM analytics_snapshots
        WHERE entity_type = ? AND entity_ref = ? AND source_family = 'EXTERNAL_YOUTUBE'
        ORDER BY captured_at DESC, id DESC
        LIMIT 5
        """,
        (entity_type, normalized_scope_ref),
    ).fetchall()
    latest_external_payload = _safe_json_dict(external_rows[0]["payload_json"]) if external_rows else {}
    historical_performance = {
        "source": "ANALYZER_PERSISTED_HISTORY",
        "scope_type": normalized_scope_type,
        "scope_ref": normalized_scope_ref,
        "history_points": len(external_rows),
        "latest_external_metrics": latest_external_payload,
    }

    prediction_rows = conn.execute(
        """
        SELECT prediction_family, predicted_label, predicted_value_json, confidence_class, variance_class, created_at
        FROM analytics_prediction_snapshots
        WHERE scope_type = ? AND scope_ref = ? AND is_current = 1
        ORDER BY created_at DESC, id DESC
        """,
        (normalized_scope_type, normalized_scope_ref),
    ).fetchall()
    publish_windows: list[str] = []
    risk_signals: list[dict[str, Any]] = []
    for row in prediction_rows:
        family = str(row["prediction_family"] or "")
        if family == "BEST_PUBLISH_WINDOW_PREDICTION":
            predicted_value = _safe_json_dict(row["predicted_value_json"])
            for key in ("publish_windows", "windows"):
                values = predicted_value.get(key)
                if isinstance(values, list):
                    publish_windows.extend(str(item).strip() for item in values if str(item).strip())
            label = str(row["predicted_label"] or "").strip()
            if label:
                publish_windows.append(label)
        if str(row["variance_class"] or "").upper() in {"RISK", "ANOMALY"}:
            risk_signals.append(
                {
                    "source": "PREDICTION",
                    "family": family,
                    "predicted_label": str(row["predicted_label"] or ""),
                    "variance_class": str(row["variance_class"] or ""),
                    "confidence_class": str(row["confidence_class"] or ""),
                }
            )

    recommendation_rows = conn.execute(
        """
        SELECT recommendation_family, severity_class, confidence_class, summary_text, created_at
        FROM analytics_recommendation_snapshots
        WHERE recommendation_scope_type = ? AND recommendation_scope_ref = ? AND is_current = 1 AND lifecycle_status = 'OPEN'
        ORDER BY created_at DESC, id DESC
        LIMIT 10
        """,
        (normalized_scope_type, normalized_scope_ref),
    ).fetchall()
    for row in recommendation_rows:
        if str(row["severity_class"] or "").upper() in {"WARNING", "CRITICAL"}:
            risk_signals.append(
                {
                    "source": "RECOMMENDATION",
                    "family": str(row["recommendation_family"] or ""),
                    "severity_class": str(row["severity_class"] or ""),
                    "confidence_class": str(row["confidence_class"] or ""),
                    "summary": str(row["summary_text"] or ""),
                }
            )

    kpi_rows = conn.execute(
        """
        SELECT kpi_family, kpi_code, status_class, value_payload_json, created_at
        FROM analytics_operational_kpi_snapshots
        WHERE scope_type = ? AND scope_ref = ? AND is_current = 1
        ORDER BY created_at DESC, id DESC
        """,
        (normalized_scope_type, normalized_scope_ref),
    ).fetchall()
    cadence_patterns: dict[str, Any] = {
        "source": "ANALYZER_PERSISTED_STATE",
        "current_kpis": [
            {
                "kpi_family": str(row["kpi_family"] or ""),
                "kpi_code": str(row["kpi_code"] or ""),
                "status_class": str(row["status_class"] or ""),
                "value_payload": _safe_json_dict(row["value_payload_json"]),
            }
            for row in kpi_rows
        ],
    }
    audience_behavior = {
        "source": "ANALYZER_PERSISTED_STATE",
        "best_publish_window_prediction_count": sum(1 for row in prediction_rows if str(row["prediction_family"] or "") == "BEST_PUBLISH_WINDOW_PREDICTION"),
        "prediction_count": len(prediction_rows),
        "recommendation_count": len(recommendation_rows),
    }
    deduped_windows = list(dict.fromkeys(w for w in publish_windows if w))
    return {
        "historical_performance": historical_performance if external_rows else {},
        "audience_behavior": audience_behavior if prediction_rows or recommendation_rows else {},
        "publish_windows": deduped_windows,
        "cadence_patterns": cadence_patterns if kpi_rows else {},
        "risk_signals": risk_signals,
    }


def build_planning_assistant_summary(
    *,
    scenario: str,
    channel_strategy_profile: str,
    format_profile: str,
    conn: Any | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
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
    grounded_inputs: dict[str, Any] = {}
    if conn is not None:
        grounded_inputs = _derive_grounded_inputs(conn, scope_type=scope_type, scope_ref=scope_ref)

    historical = dict(grounded_inputs.get("historical_performance") or historical_performance or {})
    audience = dict(grounded_inputs.get("audience_behavior") or audience_behavior or {})
    cadence = dict(grounded_inputs.get("cadence_patterns") or cadence_patterns or {})
    risks = list(grounded_inputs.get("risk_signals") or risk_signals or [])
    windows = list(grounded_inputs.get("publish_windows") or publish_windows or ["weekday_evening"])

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
