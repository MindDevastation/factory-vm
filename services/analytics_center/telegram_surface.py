from __future__ import annotations

import json
from typing import Any

from services.analytics_center.helpers import canonicalize_scope_ref
from services.analytics_center.planning_assistant import build_planning_assistant_summary

_ALLOWED_SCOPE_TYPES = {"CHANNEL", "RELEASE", "BATCH_MONTH"}


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


def _normalize_scope(
    *,
    scope_type: str | None,
    scope_ref: str | None,
    channel_slug: str | None,
    release_id: str | None,
) -> tuple[str | None, str | None]:
    candidate_type = str(scope_type or "").strip().upper()
    candidate_ref = str(scope_ref or "").strip()
    if candidate_type in _ALLOWED_SCOPE_TYPES and candidate_ref:
        return candidate_type, candidate_ref
    if str(release_id or "").strip():
        return "RELEASE", str(release_id).strip()
    if str(channel_slug or "").strip():
        return "CHANNEL", str(channel_slug).strip()
    return None, None


def _derive_grounded_surface_inputs(
    conn: Any,
    *,
    scope_type: str | None,
    scope_ref: str | None,
    channel_slug: str | None,
    release_id: str | None,
) -> dict[str, Any]:
    normalized_scope_type, normalized_scope_ref = _normalize_scope(
        scope_type=scope_type,
        scope_ref=scope_ref,
        channel_slug=channel_slug,
        release_id=release_id,
    )
    if normalized_scope_type is None or normalized_scope_ref is None:
        return {}

    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=normalized_scope_type, scope_ref=normalized_scope_ref)
    recommendations = [
        {
            "recommendation_family": str(row["recommendation_family"] or ""),
            "summary_text": str(row["summary_text"] or ""),
            "title_text": str(row["title_text"] or ""),
            "severity_class": str(row["severity_class"] or ""),
            "confidence_class": str(row["confidence_class"] or ""),
            "target_domain": str(row["target_domain"] or ""),
        }
        for row in conn.execute(
            """
            SELECT recommendation_family, summary_text, title_text, severity_class, confidence_class, target_domain
            FROM analytics_recommendation_snapshots
            WHERE recommendation_scope_type = ?
              AND recommendation_scope_ref = ?
              AND is_current = 1
              AND lifecycle_status = 'OPEN'
            ORDER BY created_at DESC, id DESC
            LIMIT 12
            """,
            (normalized_scope_type, canonical_scope_ref),
        ).fetchall()
    ]
    entity_type = "BATCH" if normalized_scope_type == "BATCH_MONTH" else normalized_scope_type
    snapshot_rows = conn.execute(
        """
        SELECT source_family, payload_json, captured_at
        FROM analytics_snapshots
        WHERE entity_type = ? AND entity_ref = ?
        ORDER BY captured_at DESC, id DESC
        LIMIT 20
        """,
        (entity_type, canonical_scope_ref),
    ).fetchall()
    latest_external_payload = {}
    for row in snapshot_rows:
        if str(row["source_family"] or "") == "EXTERNAL_YOUTUBE":
            latest_external_payload = _safe_json_dict(row["payload_json"])
            break
    external_history_points = sum(1 for row in snapshot_rows if str(row["source_family"] or "") == "EXTERNAL_YOUTUBE")
    latest_kpi = conn.execute(
        """
        SELECT kpi_code, status_class, value_payload_json
        FROM analytics_operational_kpi_snapshots
        WHERE scope_type = ? AND scope_ref = ? AND is_current = 1
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (normalized_scope_type, canonical_scope_ref),
    ).fetchone()
    planning = build_planning_assistant_summary(
        scenario="WEEK",
        channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
        format_profile="LONG_FORM",
        conn=conn,
        scope_type=normalized_scope_type,
        scope_ref=canonical_scope_ref,
    )
    return {
        "scope_type": normalized_scope_type,
        "scope_ref": str(canonical_scope_ref),
        "recommendations": recommendations,
        "planning_summary": {
            "source": "ANALYZER_PERSISTED_STATE",
            "scenario": planning["scenario"],
            "target_slots": planning["outputs"]["scenario_targets"]["target_slots"],
            "risk_budget": planning["outputs"]["scenario_targets"]["risk_budget"],
            "planning_horizon_days": planning["outputs"]["planning_horizon_days"],
        },
        "history_summary": {
            "source": "ANALYZER_PERSISTED_HISTORY",
            "external_history_points": external_history_points,
            "latest_external_metrics": latest_external_payload,
            "latest_kpi": (
                {
                    "kpi_code": str(latest_kpi["kpi_code"] or ""),
                    "status_class": str(latest_kpi["status_class"] or ""),
                    "value_payload": _safe_json_dict(latest_kpi["value_payload_json"]),
                }
                if latest_kpi
                else {}
            ),
        },
    }


def build_telegram_analyzer_surface(
    *,
    conn: Any | None = None,
    scope_type: str | None = None,
    scope_ref: str | None = None,
    channel_slug: str | None = None,
    release_id: str | None = None,
    recommendation_items: list[dict[str, Any]] | None = None,
    planning_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    grounded = (
        _derive_grounded_surface_inputs(
            conn,
            scope_type=scope_type,
            scope_ref=scope_ref,
            channel_slug=channel_slug,
            release_id=release_id,
        )
        if conn is not None
        else {}
    )
    active_scope_type = str(grounded.get("scope_type") or "").strip().upper()
    active_scope_ref = str(grounded.get("scope_ref") or "").strip()
    channel_ref = str(channel_slug or "").strip() or ("all" if active_scope_type != "CHANNEL" else active_scope_ref)
    release_ref = str(release_id or "").strip() or ("latest" if active_scope_type != "RELEASE" else active_scope_ref)
    recommendations = list(grounded.get("recommendations") or recommendation_items or [])
    planning = dict(grounded.get("planning_summary") or planning_summary or {})
    history_summary = dict(grounded.get("history_summary") or {})
    alerts = [r for r in recommendations if str(r.get("severity_class") or "").upper() in {"WARNING", "CRITICAL"}]
    deep_links = {
        "overview": "/v1/analytics/overview",
        "recommendations": "/v1/analytics/recommendations",
        "planner": "/planner",
        "channel_snapshot": f"/v1/analytics/channels/{channel_ref}",
        "release_snapshot": f"/v1/analytics/releases/{release_ref}",
    }
    return {
        "summaries": {
            "overview": f"Analyzer summary for {active_scope_type.lower() if active_scope_type else 'channel'}={active_scope_ref or channel_ref}",
            "active_recommendations": len(recommendations),
            "active_alerts": len(alerts),
            "planning_attached": bool(planning),
            "history_summary": history_summary,
        },
        "alerts": [
            {
                "kind": "ANOMALY_RISK",
                "title": str(item.get("title_text") or item.get("recommendation_family") or "alert"),
                "severity": str(item.get("severity_class") or "WARNING"),
                "deep_link": deep_links["recommendations"],
            }
            for item in alerts
        ],
        "channel_snapshots": [{"channel_slug": channel_ref, "deep_link": deep_links["channel_snapshot"]}],
        "release_video_snapshots": [{"release_id": release_ref, "deep_link": deep_links["release_snapshot"]}],
        "recommendation_summaries": [
            {
                "recommendation_family": str(item.get("recommendation_family") or ""),
                "summary": str(item.get("summary_text") or ""),
                "deep_link": deep_links["recommendations"],
            }
            for item in recommendations
        ],
        "planning_summaries": [planning] if planning else [],
        "linked_actions": [
            {"label": "Open Overview", "path": deep_links["overview"], "deep_link": deep_links["overview"], "auto_apply": False, "mutation": False},
            {"label": "Open Recommendations", "path": deep_links["recommendations"], "deep_link": deep_links["recommendations"], "auto_apply": False, "mutation": False},
            {"label": "Open Planner", "path": deep_links["planner"], "deep_link": deep_links["planner"], "auto_apply": False, "mutation": False},
            {"label": "Open Channel Snapshot", "path": deep_links["channel_snapshot"], "deep_link": deep_links["channel_snapshot"], "auto_apply": False, "mutation": False},
            {"label": "Open Release Snapshot", "path": deep_links["release_snapshot"], "deep_link": deep_links["release_snapshot"], "auto_apply": False, "mutation": False},
        ],
        "deep_links": deep_links,
        "operator_runtime": {
            "surface_kind": "TELEGRAM_ANALYZER_OPERATOR",
            "delivery_mode": "operator_preview",
            "supports_alerts": True,
            "supports_planning_summary": True,
            "supports_recommendation_summary": True,
        },
        "interface_role": {"replaces_web_ui": False, "operator_surface": True},
        "default_behavior": {"auto_apply": False, "mutation": False},
    }
