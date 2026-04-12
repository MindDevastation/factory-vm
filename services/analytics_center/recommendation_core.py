from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_INVALID_RECOMMENDATION_CONFIDENCE,
    E5A_INVALID_RECOMMENDATION_EXPLAINABILITY_PAYLOAD,
    E5A_INVALID_RECOMMENDATION_FAMILY,
    E5A_INVALID_RECOMMENDATION_SCOPE,
    E5A_INVALID_RECOMMENDATION_SEVERITY,
    E5A_INVALID_RECOMMENDATION_STATUS,
    E5A_INVALID_RECOMMENDATION_TARGET_DOMAIN,
    E5A_RECOMMENDATION_SOURCE_MISSING,
)
from services.analytics_center.literals import (
    ANALYTICS_MF5_CONFIDENCE_CLASSES,
    ANALYTICS_MF5_LIFECYCLE_STATUSES,
    ANALYTICS_MF5_RECOMMENDATION_FAMILIES,
    ANALYTICS_MF5_SCOPE_TYPES,
    ANALYTICS_MF5_SEVERITY_CLASSES,
    ANALYTICS_MF5_TARGET_DOMAINS,
)
from services.analytics_center.helpers import canonicalize_scope_ref
from services.common.db import now_ts


@dataclass(frozen=True)
class RecommendationOutput:
    scope_type: str
    scope_ref: str
    recommendation_family: str
    issue_key: str
    title_text: str
    summary_text: str
    severity_class: str
    confidence_class: str
    target_domain: str
    target_pointer_payload: dict[str, Any]
    explainability_payload: dict[str, Any]
    source_snapshot_refs: list[str]
    lifecycle_status: str = "OPEN"


def _require_enum(name: str, value: str, allowed: tuple[str, ...], code: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise AnalyticsDomainError(code=code, message=f"invalid {name}")
    return normalized


def build_explainability_payload(*, primary_reason_code: str, primary_reason_text: str, supporting_signals_json: list[dict[str, Any]], comparison_context_json: dict[str, Any], confidence_class: str, severity_class: str, next_action_hint: str, target_domain: str, target_pointer_payload_json: dict[str, Any], source_snapshot_refs_json: list[str]) -> dict[str, Any]:
    if not str(primary_reason_code or "").strip() or not str(primary_reason_text or "").strip():
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_EXPLAINABILITY_PAYLOAD, message="primary reason is required")
    if not isinstance(supporting_signals_json, list):
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_EXPLAINABILITY_PAYLOAD, message="supporting_signals_json must be list")
    if not isinstance(comparison_context_json, dict):
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_EXPLAINABILITY_PAYLOAD, message="comparison_context_json must be object")
    if not str(next_action_hint or "").strip():
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_EXPLAINABILITY_PAYLOAD, message="next_action_hint is required")
    if not isinstance(target_pointer_payload_json, dict) or not target_pointer_payload_json:
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_EXPLAINABILITY_PAYLOAD, message="target pointer payload required")
    if not isinstance(source_snapshot_refs_json, list) or not source_snapshot_refs_json:
        raise AnalyticsDomainError(code=E5A_RECOMMENDATION_SOURCE_MISSING, message="source snapshot refs required")
    _require_enum("confidence_class", confidence_class, ANALYTICS_MF5_CONFIDENCE_CLASSES, E5A_INVALID_RECOMMENDATION_CONFIDENCE)
    _require_enum("severity_class", severity_class, ANALYTICS_MF5_SEVERITY_CLASSES, E5A_INVALID_RECOMMENDATION_SEVERITY)
    _require_enum("target_domain", target_domain, ANALYTICS_MF5_TARGET_DOMAINS, E5A_INVALID_RECOMMENDATION_TARGET_DOMAIN)
    return {
        "primary_reason_code": primary_reason_code.strip(),
        "primary_reason_text": primary_reason_text.strip(),
        "supporting_signals_json": supporting_signals_json,
        "comparison_context_json": comparison_context_json,
        "confidence_class": confidence_class,
        "severity_class": severity_class,
        "next_action_hint": next_action_hint.strip(),
        "target_domain": target_domain,
        "target_pointer_payload_json": target_pointer_payload_json,
        "source_snapshot_refs_json": source_snapshot_refs_json,
    }


def build_target_domain_pointer(*, target_domain: str, scope_type: str, scope_ref: str, context_ref: str | None = None) -> dict[str, Any]:
    return {
        "target_domain": _require_enum("target_domain", target_domain, ANALYTICS_MF5_TARGET_DOMAINS, E5A_INVALID_RECOMMENDATION_TARGET_DOMAIN),
        "scope_type": _require_enum("recommendation_scope_type", scope_type, ANALYTICS_MF5_SCOPE_TYPES, E5A_INVALID_RECOMMENDATION_SCOPE),
        "scope_ref": str(scope_ref),
        "context_ref": str(context_ref or "").strip() or None,
    }


def persist_recommendation_snapshot(conn: Any, *, recommendation: RecommendationOutput, run_id: int | None = None) -> int:
    _require_enum("recommendation_scope_type", recommendation.scope_type, ANALYTICS_MF5_SCOPE_TYPES, E5A_INVALID_RECOMMENDATION_SCOPE)
    _require_enum("recommendation_family", recommendation.recommendation_family, ANALYTICS_MF5_RECOMMENDATION_FAMILIES, E5A_INVALID_RECOMMENDATION_FAMILY)
    _require_enum("severity_class", recommendation.severity_class, ANALYTICS_MF5_SEVERITY_CLASSES, E5A_INVALID_RECOMMENDATION_SEVERITY)
    _require_enum("confidence_class", recommendation.confidence_class, ANALYTICS_MF5_CONFIDENCE_CLASSES, E5A_INVALID_RECOMMENDATION_CONFIDENCE)
    _require_enum("lifecycle_status", recommendation.lifecycle_status, ANALYTICS_MF5_LIFECYCLE_STATUSES, E5A_INVALID_RECOMMENDATION_STATUS)
    _require_enum("target_domain", recommendation.target_domain, ANALYTICS_MF5_TARGET_DOMAINS, E5A_INVALID_RECOMMENDATION_TARGET_DOMAIN)
    if not recommendation.source_snapshot_refs:
        raise AnalyticsDomainError(code=E5A_RECOMMENDATION_SOURCE_MISSING, message="source snapshots required")
    if not recommendation.explainability_payload.get("next_action_hint"):
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_EXPLAINABILITY_PAYLOAD, message="next action required")
    now = now_ts()
    row = conn.execute(
        """
        INSERT INTO analytics_recommendation_snapshots(
            run_id, recommendation_scope_type, recommendation_scope_ref, recommendation_family, issue_key,
            title_text, summary_text, severity_class, confidence_class, lifecycle_status,
            target_domain, target_pointer_payload_json, explainability_payload_json,
            source_snapshot_refs_json, is_current, created_at, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            recommendation.scope_type,
            recommendation.scope_ref,
            recommendation.recommendation_family,
            recommendation.issue_key,
            recommendation.title_text,
            recommendation.summary_text,
            recommendation.severity_class,
            recommendation.confidence_class,
            recommendation.lifecycle_status,
            recommendation.target_domain,
            json.dumps(recommendation.target_pointer_payload, sort_keys=True),
            json.dumps(recommendation.explainability_payload, sort_keys=True),
            json.dumps(recommendation.source_snapshot_refs, sort_keys=True),
            1,
            now,
            now,
        ),
    )
    return int(row.lastrowid)


def _build_recommendation_from_prediction(row: dict[str, Any]) -> RecommendationOutput:
    family_map = {
        "VIEW_GROWTH_PREDICTION": ("CHANNEL_OPTIMIZATION", "PUBLISH", "Review growth context and release cadence."),
        "WATCH_TIME_GROWTH_PREDICTION": ("CONTENT_PLANNING_SUGGESTION", "PLANNER", "Open planning surface and rebalance format mix."),
        "CTR_PREDICTION": ("TITLE_METADATA_IMPROVEMENT", "METADATA", "Review title/metadata packaging for discovery."),
        "STRONG_WEAK_RELEASE_PREDICTION": ("WEAK_RELEASE_ATTENTION", "PUBLISH", "Review weak release for manual intervention."),
        "BEST_PUBLISH_WINDOW_PREDICTION": ("PUBLISH_TIMING_SUGGESTION", "PUBLISH", "Adjust publish timing window before next run."),
        "CHANNEL_TREND_PREDICTION": ("CHANNEL_OPTIMIZATION", "PUBLISH", "Inspect channel trend and prioritize growth candidates."),
        "ANOMALY_DROP_RISK_PREDICTION": ("ANOMALY_RISK_ALERT", "OPERATIONAL_TROUBLESHOOTING", "Inspect risk indicators and mitigation queue."),
    }
    prediction_family = str(row["prediction_family"])
    rec_family, target_domain, next_action = family_map[prediction_family]
    variance = str(row["variance_class"])
    severity = "CRITICAL" if variance == "RISK" else ("WARNING" if variance == "ANOMALY" else "INFO")
    confidence = str(row["confidence_class"])
    source_refs = json.loads(str(row["source_snapshot_refs_json"]))
    pointer = build_target_domain_pointer(target_domain=target_domain, scope_type=str(row["scope_type"]), scope_ref=str(row["scope_ref"]), context_ref=prediction_family)
    explainability = build_explainability_payload(
        primary_reason_code=f"PRED::{prediction_family}",
        primary_reason_text=f"Prediction suggests {prediction_family}",
        supporting_signals_json=json.loads(str(row["signals_used_json"])),
        comparison_context_json=json.loads(str(row["comparison_basis_json"])),
        confidence_class=confidence,
        severity_class=severity,
        next_action_hint=next_action,
        target_domain=target_domain,
        target_pointer_payload_json=pointer,
        source_snapshot_refs_json=source_refs,
    )
    return RecommendationOutput(
        scope_type=str(row["scope_type"]),
        scope_ref=str(row["scope_ref"]),
        recommendation_family=rec_family,
        issue_key=f"{prediction_family}:{variance}",
        title_text=f"{rec_family.replace('_', ' ').title()}",
        summary_text=f"{prediction_family} classified as {variance.lower()}.",
        severity_class=severity,
        confidence_class=confidence,
        target_domain=target_domain,
        target_pointer_payload=pointer,
        explainability_payload=explainability,
        source_snapshot_refs=source_refs,
    )


def _build_recommendation_from_comparison(row: dict[str, Any]) -> RecommendationOutput:
    variance = str(row["variance_class"])
    if variance == "NORMAL":
        severity = "INFO"
        confidence = "MEDIUM"
    elif variance == "ANOMALY":
        severity = "WARNING"
        confidence = "MEDIUM"
    else:
        severity = "CRITICAL"
        confidence = "HIGH"
    source_refs = json.loads(str(row["source_snapshot_refs_json"]))
    family = "VISUAL_IMPROVEMENT" if str(row.get("comparison_family")) == "RELEASE_VS_CHANNEL_BASELINE" else "CONTENT_PACKAGING_SUGGESTION"
    target_domain = "VISUALS" if family == "VISUAL_IMPROVEMENT" else "METADATA"
    next_action = "Inspect visual packaging context." if family == "VISUAL_IMPROVEMENT" else "Inspect metadata and packaging context."
    pointer = build_target_domain_pointer(target_domain=target_domain, scope_type=str(row["scope_type"]), scope_ref=str(row["scope_ref"]), context_ref=str(row["comparison_family"]))
    explainability = build_explainability_payload(
        primary_reason_code=f"CMP::{row['comparison_family']}",
        primary_reason_text="Comparison variance requires packaging attention.",
        supporting_signals_json=[json.loads(str(row["delta_payload_json"]))],
        comparison_context_json=json.loads(str(row["comparison_basis_json"])),
        confidence_class=confidence,
        severity_class=severity,
        next_action_hint=next_action,
        target_domain=target_domain,
        target_pointer_payload_json=pointer,
        source_snapshot_refs_json=source_refs,
    )
    return RecommendationOutput(
        scope_type=str(row["scope_type"]),
        scope_ref=str(row["scope_ref"]),
        recommendation_family=family,
        issue_key=f"{row['comparison_family']}:{variance}",
        title_text="Content packaging suggestion",
        summary_text=f"Comparison {row['comparison_family']} is {variance.lower()}.",
        severity_class=severity,
        confidence_class=confidence,
        target_domain=target_domain,
        target_pointer_payload=pointer,
        explainability_payload=explainability,
        source_snapshot_refs=source_refs,
    )


def _build_recommendation_from_kpi(row: dict[str, Any]) -> RecommendationOutput:
    status = str(row["status_class"])
    severity = "CRITICAL" if status == "RISK" else ("WARNING" if status == "ANOMALY" else "INFO")
    confidence = "MEDIUM" if status != "RISK" else "HIGH"
    source_refs = json.loads(str(row["source_snapshot_refs_json"] or "[]")) or [f"operational_kpi:{row['id']}"]
    pointer = build_target_domain_pointer(target_domain="OPERATIONAL_TROUBLESHOOTING", scope_type=str(row["scope_type"]), scope_ref=str(row["scope_ref"]), context_ref=str(row["kpi_code"]))
    explainability = build_explainability_payload(
        primary_reason_code=f"KPI::{row['kpi_code']}",
        primary_reason_text=f"KPI {row['kpi_code']} status is {status.lower()}.",
        supporting_signals_json=[json.loads(str(row["value_payload_json"]))],
        comparison_context_json={"kpi_family": row["kpi_family"]},
        confidence_class=confidence,
        severity_class=severity,
        next_action_hint="Check operational troubleshooting runbook.",
        target_domain="OPERATIONAL_TROUBLESHOOTING",
        target_pointer_payload_json=pointer,
        source_snapshot_refs_json=source_refs,
    )
    return RecommendationOutput(
        scope_type=str(row["scope_type"]),
        scope_ref=str(row["scope_ref"]),
        recommendation_family="OPERATIONAL_REMEDIATION",
        issue_key=f"{row['kpi_code']}:{status}",
        title_text="Operational remediation",
        summary_text=f"KPI {row['kpi_code']} requires attention.",
        severity_class=severity,
        confidence_class=confidence,
        target_domain="OPERATIONAL_TROUBLESHOOTING",
        target_pointer_payload=pointer,
        explainability_payload=explainability,
        source_snapshot_refs=source_refs,
    )


SynthesisBuilder = Callable[[dict[str, Any]], RecommendationOutput]


def synthesis_registry() -> dict[str, SynthesisBuilder]:
    return {
        "prediction": _build_recommendation_from_prediction,
        "comparison": _build_recommendation_from_comparison,
        "kpi": _build_recommendation_from_kpi,
    }


def synthesize_recommendations(conn: Any, *, scope_type: str, scope_ref: str) -> list[RecommendationOutput]:
    _require_enum("recommendation_scope_type", scope_type, ANALYTICS_MF5_SCOPE_TYPES, E5A_INVALID_RECOMMENDATION_SCOPE)
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=scope_type, scope_ref=scope_ref)
    reg = synthesis_registry()
    out: list[RecommendationOutput] = []
    predictions = conn.execute(
        "SELECT * FROM analytics_prediction_snapshots WHERE scope_type = ? AND scope_ref = ? AND is_current = 1",
        (scope_type, canonical_scope_ref),
    ).fetchall()
    comparisons = conn.execute(
        "SELECT * FROM analytics_comparison_snapshots WHERE scope_type = ? AND scope_ref = ? AND is_current = 1 AND variance_class IN ('ANOMALY', 'RISK')",
        (scope_type, canonical_scope_ref),
    ).fetchall()
    kpis = conn.execute(
        "SELECT * FROM analytics_operational_kpi_snapshots WHERE scope_type = ? AND scope_ref = ? AND is_current = 1 AND status_class IN ('ANOMALY', 'RISK')",
        (scope_type, canonical_scope_ref),
    ).fetchall()
    for row in predictions:
        out.append(reg["prediction"](dict(row)))
    for row in comparisons:
        out.append(reg["comparison"](dict(row)))
    for row in kpis:
        out.append(reg["kpi"](dict(row)))
    return out
