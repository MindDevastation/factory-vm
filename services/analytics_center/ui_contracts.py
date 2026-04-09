from __future__ import annotations

import json
from typing import Any

from services.analytics_center.errors import AnalyticsDomainError, E5A_INVALID_ANALYTICS_FILTERS, E5A_INVALID_ANALYTICS_PAGE_SCOPE
from services.analytics_center.freshness_state_model import summarize_coverage_states

ALLOWED_PAGE_SCOPES = {"OVERVIEW", "CHANNEL", "RELEASE", "BATCH_MONTH", "PORTFOLIO", "ANOMALIES", "RECOMMENDATIONS", "REPORTS_EXPORTS"}


def normalize_analytics_filters(raw: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "channel",
        "release_video",
        "batch_month",
        "time_window",
        "anomaly_risk_status",
        "recommendation_family",
        "severity",
        "confidence",
        "freshness",
        "source_family",
        "target_domain",
        "report_export_type",
        "scope_type",
        "lifecycle_status",
        "kpi_family",
        "prediction_family",
        "portfolio_project",
        "date_from",
        "date_to",
        "period_compare",
    }
    unknown = set(raw) - allowed
    if unknown:
        raise AnalyticsDomainError(code=E5A_INVALID_ANALYTICS_FILTERS, message=f"unsupported filters: {sorted(unknown)}")
    return {k: v for k, v in raw.items() if v not in (None, "", [])}


def build_freshness_coverage_summary(*, source_states: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    return summarize_coverage_states(source_states=source_states)

def build_analytics_page_contract(*, page_scope: str, applied_filters: dict[str, Any], freshness_summary: dict[str, Any], source_coverage_summary: dict[str, Any], summary_cards: list[dict[str, Any]], detail_blocks: list[dict[str, Any]], anomaly_risk_markers: list[dict[str, Any]], recommendation_summary: list[dict[str, Any]], available_actions: list[dict[str, Any]], export_report_actions: list[dict[str, Any],], period_semantics: dict[str, Any] | None = None, chart_blocks: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if page_scope not in ALLOWED_PAGE_SCOPES:
        raise AnalyticsDomainError(code=E5A_INVALID_ANALYTICS_PAGE_SCOPE, message="invalid page scope")
    semantics = dict(period_semantics or {})
    required_semantic_keys = (
        "current_period",
        "previous_period",
        "baseline_comparison",
        "trend_direction",
        "anomalies",
        "growth_opportunities",
    )
    for key in required_semantic_keys:
        semantics.setdefault(key, {})
    semantic_filter_contract = {
        "period_comparison_supported": True,
        "date_filters": {
            "date_from": applied_filters.get("date_from"),
            "date_to": applied_filters.get("date_to"),
            "period_compare": applied_filters.get("period_compare", "PREVIOUS_PERIOD"),
        },
    }
    charts = list(chart_blocks or [])
    if not charts:
        charts = [
            {"chart_id": "trend_primary", "animated": True, "kind": "LINE"},
            {"chart_id": "comparison_secondary", "animated": True, "kind": "BAR"},
        ]
    completeness_state = str(source_coverage_summary.get("status") or "").upper()
    return {
        "page_scope": page_scope,
        "applied_filters": applied_filters,
        "freshness_summary": freshness_summary,
        "source_coverage_summary": source_coverage_summary,
        "summary_cards": summary_cards,
        "detail_blocks": detail_blocks,
        "anomaly_risk_markers": anomaly_risk_markers,
        "recommendation_summary": recommendation_summary,
        "available_actions": available_actions,
        "export_report_actions": export_report_actions,
        "period_semantics": semantics,
        "semantic_filter_contract": semantic_filter_contract,
        "chart_blocks": charts,
        "filter_state_token": json.dumps(applied_filters, sort_keys=True),
        "data_completeness": "FULL" if completeness_state in {"REFRESHED", "FULL"} else "PARTIAL",
    }
