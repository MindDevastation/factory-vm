from __future__ import annotations

import json
from typing import Any

from services.analytics_center.errors import AnalyticsDomainError, E5A_INVALID_ANALYTICS_FILTERS, E5A_INVALID_ANALYTICS_PAGE_SCOPE
from services.analytics_center.freshness_state_model import summarize_coverage_states

ALLOWED_PAGE_SCOPES = {"OVERVIEW", "CHANNEL", "RELEASE", "BATCH_MONTH", "ANOMALIES", "RECOMMENDATIONS", "REPORTS_EXPORTS"}


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
    }
    unknown = set(raw) - allowed
    if unknown:
        raise AnalyticsDomainError(code=E5A_INVALID_ANALYTICS_FILTERS, message=f"unsupported filters: {sorted(unknown)}")
    return {k: v for k, v in raw.items() if v not in (None, "", [])}


def build_freshness_coverage_summary(*, source_states: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    return summarize_coverage_states(source_states=source_states)

def build_analytics_page_contract(*, page_scope: str, applied_filters: dict[str, Any], freshness_summary: dict[str, Any], source_coverage_summary: dict[str, Any], summary_cards: list[dict[str, Any]], detail_blocks: list[dict[str, Any]], anomaly_risk_markers: list[dict[str, Any]], recommendation_summary: list[dict[str, Any]], available_actions: list[dict[str, Any]], export_report_actions: list[dict[str, Any]]) -> dict[str, Any]:
    if page_scope not in ALLOWED_PAGE_SCOPES:
        raise AnalyticsDomainError(code=E5A_INVALID_ANALYTICS_PAGE_SCOPE, message="invalid page scope")
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
        "filter_state_token": json.dumps(applied_filters, sort_keys=True),
        "data_completeness": "FULL" if source_coverage_summary.get("status") == "REFRESHED" else "PARTIAL",
    }
