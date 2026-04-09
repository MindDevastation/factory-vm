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
    return {
        "summaries": {
            "overview": f"Analyzer summary for channel={channel_ref}",
            "active_recommendations": len(recommendations),
        },
        "alerts": [
            {
                "kind": "ANOMALY_RISK",
                "title": str(item.get("title_text") or item.get("recommendation_family") or "alert"),
                "severity": str(item.get("severity_class") or "WARNING"),
            }
            for item in alerts
        ],
        "channel_snapshots": [{"channel_slug": channel_ref, "deep_link": f"/v1/analytics/channels/{channel_ref}"}],
        "release_video_snapshots": [{"release_id": release_ref, "deep_link": f"/v1/analytics/releases/{release_ref}"}],
        "recommendation_summaries": [
            {
                "recommendation_family": str(item.get("recommendation_family") or ""),
                "summary": str(item.get("summary_text") or ""),
                "deep_link": "/v1/analytics/recommendations",
            }
            for item in recommendations
        ],
        "planning_summaries": [planning] if planning else [],
        "linked_actions": [
            {"label": "Open Overview", "path": "/v1/analytics/overview"},
            {"label": "Open Recommendations", "path": "/v1/analytics/recommendations"},
            {"label": "Open Planner", "path": "/planner"},
        ],
        "interface_role": {"replaces_web_ui": False, "operator_surface": True},
        "default_behavior": {"auto_apply": False, "mutation": False},
    }

