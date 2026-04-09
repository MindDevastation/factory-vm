from __future__ import annotations

from typing import Any


def build_telegram_analyzer_surface(
    *,
    channel_slug: str | None = None,
    release_id: str | None = None,
    recommendation_items: list[dict[str, Any]] | None = None,
    planning_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    channel_ref = str(channel_slug or "").strip() or "all"
    release_ref = str(release_id or "").strip() or "latest"
    recommendations = list(recommendation_items or [])
    planning = dict(planning_summary or {})
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
            "overview": f"Analyzer summary for channel={channel_ref}",
            "active_recommendations": len(recommendations),
            "active_alerts": len(alerts),
            "planning_attached": bool(planning),
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
