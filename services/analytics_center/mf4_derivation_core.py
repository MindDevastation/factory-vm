from __future__ import annotations

import json
from dataclasses import dataclass
from statistics import mean
from typing import Any

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_BASELINE_SOURCE_SNAPSHOTS_MISSING,
    E5A_INVALID_BASELINE_FAMILY,
    E5A_INVALID_BASELINE_SCOPE,
    E5A_INVALID_COMPARISON_FAMILY,
    E5A_INVALID_CONFIDENCE_CLASS,
    E5A_INVALID_PREDICTION_EXPLAINABILITY_PAYLOAD,
    E5A_INVALID_PREDICTION_FAMILY,
    E5A_INVALID_VARIANCE_CLASS,
)
from services.analytics_center.literals import (
    ANALYTICS_MF4_BASELINE_FAMILIES,
    ANALYTICS_MF4_COMPARISON_FAMILIES,
    ANALYTICS_MF4_CONFIDENCE_CLASSES,
    ANALYTICS_MF4_PREDICTION_FAMILIES,
    ANALYTICS_MF4_SCOPE_TYPES,
    ANALYTICS_MF4_VARIANCE_CLASSES,
)
from services.analytics_center.helpers import canonicalize_scope_ref
from services.common.db import now_ts


@dataclass(frozen=True)
class Mf4BaselineOutput:
    scope_type: str
    scope_ref: str
    baseline_family: str
    variance_class: str
    baseline_payload: dict[str, Any]
    comparison_basis: dict[str, Any]
    source_snapshot_refs: list[str]


@dataclass(frozen=True)
class Mf4ComparisonOutput:
    scope_type: str
    scope_ref: str
    comparison_family: str
    variance_class: str
    delta_payload: dict[str, Any]
    comparison_basis: dict[str, Any]
    source_snapshot_refs: list[str]
    baseline_family: str


@dataclass(frozen=True)
class Mf4PredictionOutput:
    scope_type: str
    scope_ref: str
    prediction_family: str
    variance_class: str
    confidence_class: str
    predicted_label: str
    predicted_value: dict[str, Any]
    signals_used: list[dict[str, Any]]
    comparison_basis: dict[str, Any]
    explainability_payload: dict[str, Any]
    source_snapshot_refs: list[str]
    comparison_family: str


def resolve_baseline_window(*, strategy: str, observed_to: float, recent_n: int = 10, bounded_days: int = 30) -> dict[str, Any]:
    name = str(strategy or "").strip().upper()
    if name == "ROLLING_HISTORICAL":
        return {"window_type": "ROLLING_HISTORICAL", "from_ts": observed_to - (recent_n * 86400.0), "to_ts": observed_to, "recent_n": recent_n}
    if name == "BOUNDED_COMPARISON":
        return {"window_type": "BOUNDED_COMPARISON", "from_ts": observed_to - (bounded_days * 86400.0), "to_ts": observed_to, "bounded_days": bounded_days}
    if name == "RECENT_N":
        return {"window_type": "RECENT_N", "recent_n": recent_n}
    if name == "MONTHLY_BATCH":
        return {"window_type": "MONTHLY_BATCH", "month_bucket_start": observed_to - (31 * 86400.0), "to_ts": observed_to}
    if name == "LAST_KNOWN_CURRENT":
        return {"window_type": "LAST_KNOWN_CURRENT", "reference_ts": observed_to}
    raise AnalyticsDomainError(code=E5A_INVALID_BASELINE_SCOPE, message="invalid baseline window strategy")


def classify_variance(*, delta_ratio: float, anomaly_threshold: float, risk_threshold: float) -> str:
    if delta_ratio >= risk_threshold:
        return "RISK"
    if delta_ratio >= anomaly_threshold:
        return "ANOMALY"
    return "NORMAL"


def build_comparison_basis_and_explainability(
    *,
    primary_reason: str,
    supporting_signals: list[dict[str, Any]],
    remediation_hint_or_next_interpretation: str,
    scope: dict[str, Any],
    comparison_baseline: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not primary_reason:
        raise AnalyticsDomainError(code=E5A_INVALID_PREDICTION_EXPLAINABILITY_PAYLOAD, message="primary_reason is required")
    basis = {"scope": scope, "comparison_baseline": comparison_baseline, "signals_used": supporting_signals}
    explainability = {
        "primary_reason": primary_reason,
        "supporting_signals": supporting_signals,
        "remediation_hint_or_next_interpretation": remediation_hint_or_next_interpretation,
        "scope": scope,
        "comparison_baseline": comparison_baseline,
    }
    return basis, explainability


def _loads_json(text: str | None, default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


def _validate_scope(scope_type: str) -> str:
    normalized = str(scope_type or "").strip().upper()
    if normalized not in ANALYTICS_MF4_SCOPE_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_BASELINE_SCOPE, message="invalid baseline scope")
    return normalized


def _require_enum(name: str, value: str, allowed: tuple[str, ...], code: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise AnalyticsDomainError(code=code, message=f"invalid {name}")
    return normalized


def _collect_persisted_signals(conn: Any, *, scope_type: str, scope_ref: str, observed_to: float) -> dict[str, Any]:
    win = resolve_baseline_window(strategy="ROLLING_HISTORICAL", observed_to=observed_to, recent_n=14)
    analytics_rows = conn.execute(
        """
        SELECT id, payload_json, source_family, entity_type, entity_ref, captured_at
        FROM analytics_snapshots
        WHERE entity_type = ? AND entity_ref = ? AND captured_at >= ?
        ORDER BY captured_at DESC
        LIMIT 50
        """,
        (scope_type.replace("BATCH_MONTH", "BATCH"), scope_ref, float(win["from_ts"])),
    ).fetchall()
    if not analytics_rows:
        analytics_rows = conn.execute(
            """
            SELECT id, payload_json, source_family, entity_type, entity_ref, captured_at
            FROM analytics_snapshots
            WHERE entity_type = ? AND entity_ref = ?
            ORDER BY captured_at DESC
            LIMIT 20
            """,
            (scope_type.replace("BATCH_MONTH", "BATCH"), scope_ref),
        ).fetchall()
    if not analytics_rows:
        raise AnalyticsDomainError(code=E5A_BASELINE_SOURCE_SNAPSHOTS_MISSING, message="baseline source snapshots missing")

    external_rows = [r for r in analytics_rows if str(r["source_family"]) == "EXTERNAL_YOUTUBE"]
    internal_rows = [r for r in analytics_rows if str(r["source_family"]) != "EXTERNAL_YOUTUBE"]
    operational_rows = conn.execute(
        """
        SELECT id, kpi_family, status_class, value_payload_json
        FROM analytics_operational_kpi_snapshots
        WHERE scope_type = ? AND scope_ref = ? AND is_current = 1
        """,
        (scope_type, scope_ref),
    ).fetchall()
    return {
        "analytics_rows": analytics_rows,
        "external_rows": external_rows,
        "internal_rows": internal_rows,
        "operational_rows": operational_rows,
    }


def _value_score(rows: list[dict[str, Any]]) -> float:
    vals: list[float] = []
    for row in rows:
        payload = _loads_json(row.get("payload_json"), {})
        if isinstance(payload, dict):
            for v in payload.values():
                if isinstance(v, (int, float)):
                    vals.append(float(v))
    return mean(vals) if vals else 0.0


def derive_baselines(conn: Any, *, scope_type: str, scope_ref: str, observed_to: float | None = None) -> list[Mf4BaselineOutput]:
    scope = _validate_scope(scope_type)
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=scope, scope_ref=scope_ref)
    at = float(observed_to or now_ts())
    sig = _collect_persisted_signals(conn, scope_type=scope, scope_ref=canonical_scope_ref, observed_to=at)
    score_external = _value_score(sig["external_rows"])
    score_internal = _value_score(sig["internal_rows"])
    score_operational = mean(
        [
            _value_score([{"payload_json": row["value_payload_json"]}])
            for row in sig["operational_rows"]
            if row["value_payload_json"]
        ]
    ) if sig["operational_rows"] else 0.0
    combined = (score_external * 0.45) + (score_internal * 0.25) + (score_operational * 0.30)
    refs = [f"snapshot:{int(r['id'])}" for r in sig["analytics_rows"][:10]] + [f"kpi:{int(r['id'])}" for r in sig["operational_rows"][:10]]

    output: list[Mf4BaselineOutput] = []
    for family in ANALYTICS_MF4_BASELINE_FAMILIES:
        variance = classify_variance(delta_ratio=abs(combined) / 1000.0, anomaly_threshold=0.2, risk_threshold=0.5)
        output.append(
            Mf4BaselineOutput(
                scope_type=scope,
                scope_ref=canonical_scope_ref,
                baseline_family=family,
                variance_class=variance,
                baseline_payload={"aggregate_score": combined, "external_score": score_external, "internal_score": score_internal, "operational_score": score_operational},
                comparison_basis={"window": resolve_baseline_window(strategy="ROLLING_HISTORICAL", observed_to=at), "family": family},
                source_snapshot_refs=refs,
            )
        )
    return output


def derive_comparisons(conn: Any, *, baselines: list[Mf4BaselineOutput]) -> list[Mf4ComparisonOutput]:
    outputs: list[Mf4ComparisonOutput] = []
    baseline_map = {
        "RELEASE_VS_CHANNEL_BASELINE": "RELEASE_VS_CHANNEL",
        "CHANNEL_VS_SELF_HISTORY": "CHANNEL_HISTORICAL",
        "BATCH_MONTH_VS_RECENT_CHANNEL": "BATCH_MONTH_HISTORICAL",
        "CHANNEL_VS_PORTFOLIO": "PORTFOLIO_COMPARISON",
    }
    by_family = {b.baseline_family: b for b in baselines}
    for family in ANALYTICS_MF4_COMPARISON_FAMILIES:
        ref_family = baseline_map[family]
        b = by_family[ref_family]
        payload = dict(b.baseline_payload)
        delta = float(payload.get("external_score", 0.0)) - float(payload.get("internal_score", 0.0))
        delta_ratio = abs(delta) / max(1.0, abs(float(payload.get("aggregate_score", 1.0))))
        variance = classify_variance(delta_ratio=delta_ratio, anomaly_threshold=0.15, risk_threshold=0.4)
        outputs.append(
            Mf4ComparisonOutput(
                scope_type=b.scope_type,
                scope_ref=b.scope_ref,
                comparison_family=family,
                variance_class=variance,
                delta_payload={"delta_external_vs_internal": delta, "delta_ratio": delta_ratio, "relative_ranking_summary": "top-half" if delta >= 0 else "bottom-half"},
                comparison_basis={"baseline_family": b.baseline_family, "window": b.comparison_basis.get("window")},
                source_snapshot_refs=b.source_snapshot_refs,
                baseline_family=b.baseline_family,
            )
        )
    return outputs


def derive_predictions(conn: Any, *, comparisons: list[Mf4ComparisonOutput]) -> list[Mf4PredictionOutput]:
    if not comparisons:
        raise AnalyticsDomainError(code=E5A_BASELINE_SOURCE_SNAPSHOTS_MISSING, message="comparison outputs are required")
    by_family = {c.comparison_family: c for c in comparisons}

    def _prediction_for(family: str) -> Mf4PredictionOutput:
        pick = by_family["RELEASE_VS_CHANNEL_BASELINE"]
        if family == "CHANNEL_MOMENTUM":
            pick = by_family["CHANNEL_VS_SELF_HISTORY"]
        elif family == "CADENCE_DEGRADATION_RISK":
            pick = by_family["BATCH_MONTH_VS_RECENT_CHANNEL"]
        elif family == "OPERATIONAL_ANOMALY_RISK":
            pick = by_family["CHANNEL_VS_PORTFOLIO"]
        delta_ratio = float(pick.delta_payload["delta_ratio"])
        variance = classify_variance(delta_ratio=delta_ratio, anomaly_threshold=0.15, risk_threshold=0.4)
        confidence = "HIGH" if delta_ratio >= 0.4 else "MEDIUM" if delta_ratio >= 0.15 else "LOW"
        basis, explainability = build_comparison_basis_and_explainability(
            primary_reason=f"{family} derived from {pick.comparison_family}",
            supporting_signals=[
                {"signal": "delta_ratio", "value": delta_ratio},
                {"signal": "relative_ranking_summary", "value": pick.delta_payload["relative_ranking_summary"]},
            ],
            remediation_hint_or_next_interpretation="inspect baseline drift and operational queue before intervention",
            scope={"scope_type": pick.scope_type, "scope_ref": pick.scope_ref},
            comparison_baseline={"comparison_family": pick.comparison_family, "baseline_family": pick.baseline_family},
        )
        return Mf4PredictionOutput(
            scope_type=pick.scope_type,
            scope_ref=pick.scope_ref,
            prediction_family=family,
            variance_class=variance,
            confidence_class=confidence,
            predicted_label=variance,
            predicted_value={"risk_score": delta_ratio, "family": family},
            signals_used=explainability["supporting_signals"],
            comparison_basis=basis,
            explainability_payload=explainability,
            source_snapshot_refs=pick.source_snapshot_refs,
            comparison_family=pick.comparison_family,
        )

    prediction_registry = {family: (lambda fam=family: _prediction_for(fam)) for family in ANALYTICS_MF4_PREDICTION_FAMILIES}
    outputs = [prediction_registry[family]() for family in ANALYTICS_MF4_PREDICTION_FAMILIES]
    return outputs


def persist_mf4_derivation(
    conn: Any,
    *,
    baselines: list[Mf4BaselineOutput],
    comparisons: list[Mf4ComparisonOutput],
    predictions: list[Mf4PredictionOutput],
) -> dict[str, int]:
    baseline_ids: dict[str, int] = {}
    comparison_ids: dict[str, int] = {}
    prediction_ids: dict[str, int] = {}

    def _validate_source_refs(refs: list[str]) -> None:
        if not isinstance(refs, list) or not refs:
            raise AnalyticsDomainError(code=E5A_INVALID_PREDICTION_EXPLAINABILITY_PAYLOAD, message="source_snapshot_refs_json must be non-empty list")
        if not all(isinstance(ref, str) and ref.strip() for ref in refs):
            raise AnalyticsDomainError(code=E5A_INVALID_PREDICTION_EXPLAINABILITY_PAYLOAD, message="source_snapshot_refs_json entries must be non-empty strings")

    for b in baselines:
        _require_enum("baseline_family", b.baseline_family, ANALYTICS_MF4_BASELINE_FAMILIES, E5A_INVALID_BASELINE_FAMILY)
        _require_enum("variance_class", b.variance_class, ANALYTICS_MF4_VARIANCE_CLASSES, E5A_INVALID_VARIANCE_CLASS)
        _validate_source_refs(b.source_snapshot_refs)
        now = now_ts()
        conn.execute(
            "UPDATE analytics_baseline_snapshots SET is_current = 0, updated_at = ? WHERE scope_type = ? AND scope_ref = ? AND baseline_family = ? AND is_current = 1",
            (now, b.scope_type, b.scope_ref, b.baseline_family),
        )
        row = conn.execute(
            """
            INSERT INTO analytics_baseline_snapshots(
                run_id, scope_type, scope_ref, baseline_family, variance_class, baseline_payload_json, source_snapshot_refs_json, comparison_basis_json, is_current, created_at, updated_at
            ) VALUES(NULL,?,?,?,?,?,?,?,?,?,?)
            """,
            (b.scope_type, b.scope_ref, b.baseline_family, b.variance_class, json.dumps(b.baseline_payload, sort_keys=True), json.dumps(b.source_snapshot_refs, sort_keys=True), json.dumps(b.comparison_basis, sort_keys=True), 1, now, now),
        )
        baseline_ids[b.baseline_family] = int(row.lastrowid)

    for c in comparisons:
        _require_enum("comparison_family", c.comparison_family, ANALYTICS_MF4_COMPARISON_FAMILIES, E5A_INVALID_COMPARISON_FAMILY)
        _require_enum("variance_class", c.variance_class, ANALYTICS_MF4_VARIANCE_CLASSES, E5A_INVALID_VARIANCE_CLASS)
        _validate_source_refs(c.source_snapshot_refs)
        now = now_ts()
        conn.execute(
            "UPDATE analytics_comparison_snapshots SET is_current = 0, updated_at = ? WHERE scope_type = ? AND scope_ref = ? AND comparison_family = ? AND is_current = 1",
            (now, c.scope_type, c.scope_ref, c.comparison_family),
        )
        row = conn.execute(
            """
            INSERT INTO analytics_comparison_snapshots(
                run_id, scope_type, scope_ref, comparison_family, variance_class, baseline_snapshot_id, delta_payload_json, comparison_basis_json, source_snapshot_refs_json, is_current, created_at, updated_at
            ) VALUES(NULL,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (c.scope_type, c.scope_ref, c.comparison_family, c.variance_class, baseline_ids.get(c.baseline_family), json.dumps(c.delta_payload, sort_keys=True), json.dumps(c.comparison_basis, sort_keys=True), json.dumps(c.source_snapshot_refs, sort_keys=True), 1, now, now),
        )
        comparison_ids[c.comparison_family] = int(row.lastrowid)

    prediction_count = 0
    for p in predictions:
        _require_enum("prediction_family", p.prediction_family, ANALYTICS_MF4_PREDICTION_FAMILIES, E5A_INVALID_PREDICTION_FAMILY)
        _require_enum("variance_class", p.variance_class, ANALYTICS_MF4_VARIANCE_CLASSES, E5A_INVALID_VARIANCE_CLASS)
        _require_enum("confidence_class", p.confidence_class, ANALYTICS_MF4_CONFIDENCE_CLASSES, E5A_INVALID_CONFIDENCE_CLASS)
        _validate_source_refs(p.source_snapshot_refs)
        if not p.explainability_payload or not p.comparison_basis:
            raise AnalyticsDomainError(
                code=E5A_INVALID_PREDICTION_EXPLAINABILITY_PAYLOAD,
                message="prediction explainability and comparison basis are required",
            )
        now = now_ts()
        conn.execute(
            "UPDATE analytics_prediction_snapshots SET is_current = 0, updated_at = ? WHERE scope_type = ? AND scope_ref = ? AND prediction_family = ? AND is_current = 1",
            (now, p.scope_type, p.scope_ref, p.prediction_family),
        )
        row = conn.execute(
            """
            INSERT INTO analytics_prediction_snapshots(
                run_id, scope_type, scope_ref, prediction_family, variance_class, confidence_class, comparison_family, comparison_snapshot_id,
                predicted_label, predicted_value_json, signals_used_json, comparison_basis_json, explainability_payload_json, source_snapshot_refs_json, is_current, created_at, updated_at
            ) VALUES(NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                p.scope_type,
                p.scope_ref,
                p.prediction_family,
                p.variance_class,
                p.confidence_class,
                p.comparison_family,
                comparison_ids.get(p.comparison_family),
                p.predicted_label,
                json.dumps(p.predicted_value, sort_keys=True),
                json.dumps(p.signals_used, sort_keys=True),
                json.dumps(p.comparison_basis, sort_keys=True),
                json.dumps(p.explainability_payload, sort_keys=True),
                json.dumps(p.source_snapshot_refs, sort_keys=True),
                1,
                now,
                now,
            ),
        )
        prediction_ids[p.prediction_family] = int(row.lastrowid)
        prediction_count += 1
    return {
        "baseline_count": len(baselines),
        "comparison_count": len(comparisons),
        "prediction_count": prediction_count,
        "baseline_ids": baseline_ids,
        "comparison_ids": comparison_ids,
        "prediction_ids": prediction_ids,
    }
