from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Any, Callable

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_INVALID_KPI_EVIDENCE_PAYLOAD,
    E5A_INVALID_KPI_EXPLAINABILITY_PAYLOAD,
    E5A_INVALID_KPI_FAMILY,
    E5A_INVALID_KPI_STATUS,
    E5A_INVALID_OPERATIONAL_SCOPE,
    E5A_OPERATIONAL_SOURCE_SNAPSHOTS_MISSING,
)
from services.analytics_center.literals import (
    ANALYTICS_OPERATIONAL_KPI_FAMILIES,
    ANALYTICS_OPERATIONAL_KPI_STATUS_CLASSES,
    ANALYTICS_OPERATIONAL_SCOPE_TYPES,
)


@dataclass(frozen=True)
class KpiOutput:
    scope_type: str
    scope_ref: str
    kpi_family: str
    kpi_code: str
    status_class: str
    value_payload: dict[str, Any]
    explainability_payload: dict[str, Any] | None
    source_snapshot_refs: list[str]


def normalize_problem_listing_filters(*, scope_type: str | None = None, kpi_family: str | None = None, status_class: str | None = None) -> dict[str, str | None]:
    normalized_scope = None if scope_type is None else scope_type.strip().upper()
    normalized_family = None if kpi_family is None else kpi_family.strip().upper()
    normalized_status = None if status_class is None else status_class.strip().upper()
    if normalized_scope and normalized_scope not in ANALYTICS_OPERATIONAL_SCOPE_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_OPERATIONAL_SCOPE, message="invalid operational scope")
    if normalized_family and normalized_family not in ANALYTICS_OPERATIONAL_KPI_FAMILIES:
        raise AnalyticsDomainError(code=E5A_INVALID_KPI_FAMILY, message="invalid kpi family")
    if normalized_status and normalized_status not in ANALYTICS_OPERATIONAL_KPI_STATUS_CLASSES:
        raise AnalyticsDomainError(code=E5A_INVALID_KPI_STATUS, message="invalid status class")
    return {"scope_type": normalized_scope, "kpi_family": normalized_family, "status_class": normalized_status}


def build_explainability_payload(
    *,
    primary_reason_code: str,
    primary_reason_text: str,
    supporting_signals_json: list[dict[str, Any]],
    remediation_hint: str,
    baseline_scope_type: str,
    baseline_scope_ref: str,
    baseline_window_ref: str,
    evidence_payload_json: dict[str, Any],
) -> dict[str, Any]:
    if not primary_reason_code or not primary_reason_text:
        raise AnalyticsDomainError(code=E5A_INVALID_KPI_EXPLAINABILITY_PAYLOAD, message="primary reason is required")
    if not isinstance(supporting_signals_json, list):
        raise AnalyticsDomainError(code=E5A_INVALID_KPI_EXPLAINABILITY_PAYLOAD, message="supporting_signals_json must be list")
    if not isinstance(evidence_payload_json, dict):
        raise AnalyticsDomainError(code=E5A_INVALID_KPI_EVIDENCE_PAYLOAD, message="evidence payload must be object")
    return {
        "primary_reason_code": primary_reason_code,
        "primary_reason_text": primary_reason_text,
        "supporting_signals_json": supporting_signals_json,
        "remediation_hint": remediation_hint,
        "baseline_scope_type": baseline_scope_type,
        "baseline_scope_ref": baseline_scope_ref,
        "baseline_window_ref": baseline_window_ref,
        "evidence_payload_json": evidence_payload_json,
    }


def build_source_snapshot_refs(*, scope_type: str, scope_ref: str, signal_rows: list[dict[str, Any]]) -> list[str]:
    refs = [f"{scope_type}:{scope_ref}"]
    refs.extend(f"signal:{row.get('signal')}" for row in signal_rows if row.get("signal"))
    return refs


def _classify_status(*, ratio: float, warn_threshold: float, risk_threshold: float) -> str:
    if ratio >= risk_threshold:
        return "RISK"
    if ratio >= warn_threshold:
        return "ANOMALY"
    return "NORMAL"


def _validate_output(output: KpiOutput) -> None:
    if output.kpi_family not in ANALYTICS_OPERATIONAL_KPI_FAMILIES:
        raise AnalyticsDomainError(code=E5A_INVALID_KPI_FAMILY, message="invalid kpi family")
    if output.status_class not in ANALYTICS_OPERATIONAL_KPI_STATUS_CLASSES:
        raise AnalyticsDomainError(code=E5A_INVALID_KPI_STATUS, message="invalid kpi status")
    if output.scope_type not in ANALYTICS_OPERATIONAL_SCOPE_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_OPERATIONAL_SCOPE, message="invalid scope")
    if output.status_class in {"ANOMALY", "RISK"} and not output.explainability_payload:
        raise AnalyticsDomainError(code=E5A_INVALID_KPI_EXPLAINABILITY_PAYLOAD, message="explainability required for anomaly/risk")


def derive_operational_kpis(conn: Any, *, scope_type: str, scope_ref: str) -> list[KpiOutput]:
    scope = scope_type.strip().upper()
    if scope not in ANALYTICS_OPERATIONAL_SCOPE_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_OPERATIONAL_SCOPE, message="invalid operational scope")

    signals = _collect_signals(conn, scope_type=scope, scope_ref=scope_ref)
    outputs: list[KpiOutput] = []
    formula_registry: dict[str, Callable[[dict[str, Any]], KpiOutput]] = {
        "PIPELINE_TIMING": lambda s: _compute_pipeline_timing(scope, scope_ref, s),
        "QA_STATUS": lambda s: _compute_qa_status(scope, scope_ref, s),
        "UPLOAD_OUTCOME": lambda s: _compute_upload_outcome(scope, scope_ref, s),
        "PUBLISH_OUTCOME": lambda s: _compute_publish_outcome(scope, scope_ref, s),
        "RETRY_BURDEN": lambda s: _compute_retry_burden(scope, scope_ref, s),
        "READINESS": lambda s: _compute_readiness(scope, scope_ref, s),
        "DRIFT_RECONCILE": lambda s: _compute_drift_reconcile(scope, scope_ref, s),
        "CADENCE_ADHERENCE": lambda s: _compute_cadence(scope, scope_ref, s),
        "BATCH_COMPLETENESS": lambda s: _compute_batch_completeness(scope, scope_ref, s),
    }
    for family in ANALYTICS_OPERATIONAL_KPI_FAMILIES:
        out = formula_registry[family](signals)
        _validate_output(out)
        outputs.append(out)
    return outputs


def _collect_signals(conn: Any, *, scope_type: str, scope_ref: str) -> dict[str, Any]:
    where = ""
    params: tuple[Any, ...] = ()
    if scope_type == "RELEASE":
        where = "WHERE j.release_id = ?"
        params = (int(scope_ref),)
    jobs = conn.execute(
        f"""
        SELECT j.id, j.state, j.created_at, j.updated_at, j.retry_of_job_id,
               j.publish_state, j.publish_last_error_code,
               r.id AS release_id, r.planned_at
        FROM jobs j
        JOIN releases r ON r.id = j.release_id
        {where}
        """,
        params,
    ).fetchall()
    if not jobs and scope_type in {"RELEASE", "CHANNEL"}:
        raise AnalyticsDomainError(code=E5A_OPERATIONAL_SOURCE_SNAPSHOTS_MISSING, message="operational source snapshots missing")

    qa = conn.execute("SELECT COUNT(*) AS c, SUM(CASE WHEN hard_ok = 1 THEN 1 ELSE 0 END) AS pass_count FROM qa_reports").fetchone()
    worker = conn.execute("SELECT COUNT(*) AS c FROM worker_heartbeats").fetchone()
    planned = conn.execute("SELECT COUNT(*) AS c, SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_count FROM planned_releases").fetchone()
    return {
        "jobs": [dict(j) for j in jobs],
        "qa_count": int(qa["c"] or 0),
        "qa_pass_count": int(qa["pass_count"] or 0),
        "worker_count": int(worker["c"] or 0),
        "planned_count": int(planned["c"] or 0),
        "planned_failed_count": int(planned["failed_count"] or 0),
    }


def _latencies(jobs: list[dict[str, Any]]) -> list[float]:
    return [max(0.0, float(j.get("updated_at") or 0.0) - float(j.get("created_at") or 0.0)) for j in jobs]


def _make_output(scope_type: str, scope_ref: str, family: str, code: str, status: str, value_payload: dict[str, Any], reason: str, signals: dict[str, Any]) -> KpiOutput:
    explainability = None
    if status in {"ANOMALY", "RISK"}:
        explainability = build_explainability_payload(
            primary_reason_code=f"{family}_{status}",
            primary_reason_text=reason,
            supporting_signals_json=[{"signal": family, "status": status}],
            remediation_hint="inspect operational queue and worker/qa details",
            baseline_scope_type=scope_type,
            baseline_scope_ref=scope_ref,
            baseline_window_ref="latest",
            evidence_payload_json=value_payload,
        )
    refs = build_source_snapshot_refs(scope_type=scope_type, scope_ref=scope_ref, signal_rows=[{"signal": family}])
    return KpiOutput(scope_type, scope_ref, family, code, status, value_payload, explainability, refs)


def _compute_pipeline_timing(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    lats = _latencies(s["jobs"])
    delayed = sum(1 for x in lats if x > 3600)
    ratio = (delayed / len(lats)) if lats else 0.0
    status = _classify_status(ratio=ratio, warn_threshold=0.2, risk_threshold=0.5)
    payload = {"average_pipeline_latency": (sum(lats) / len(lats)) if lats else 0.0, "median_pipeline_latency": median(lats) if lats else 0.0, "delayed_release_count": delayed, "delayed_release_ratio": ratio, "long_tail_latency_indicator": max(lats) if lats else 0.0}
    return _make_output(scope_type, scope_ref, "PIPELINE_TIMING", "pipeline_latency", status, payload, "pipeline delays concentrated", s)


def _compute_qa_status(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    total = max(1, s["qa_count"])
    fail_ratio = 1 - (s["qa_pass_count"] / total)
    status = _classify_status(ratio=fail_ratio, warn_threshold=0.2, risk_threshold=0.4)
    payload = {"qa_pass_ratio": s["qa_pass_count"] / total, "qa_failure_ratio": fail_ratio, "qa_blocked_open_count": total - s["qa_pass_count"], "qa_problem_concentration_flag": fail_ratio >= 0.2}
    return _make_output(scope_type, scope_ref, "QA_STATUS", "qa_quality", status, payload, "qa failure ratio elevated", s)


def _compute_upload_outcome(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    jobs = s["jobs"]
    total = max(1, len(jobs))
    failures = sum(1 for j in jobs if str(j.get("state", "")).endswith("FAILED"))
    ratio = failures / total
    status = _classify_status(ratio=ratio, warn_threshold=0.15, risk_threshold=0.35)
    payload = {"upload_success_ratio": 1 - ratio, "upload_failure_ratio": ratio, "upload_failure_concentration_flag": ratio >= 0.15}
    return _make_output(scope_type, scope_ref, "UPLOAD_OUTCOME", "upload_outcome", status, payload, "upload failures concentrated", s)


def _compute_publish_outcome(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    jobs = s["jobs"]
    total = max(1, len(jobs))
    failed = sum(1 for j in jobs if j.get("publish_last_error_code"))
    ratio = failed / total
    status = _classify_status(ratio=ratio, warn_threshold=0.1, risk_threshold=0.3)
    payload = {"publish_success_ratio": 1 - ratio, "publish_failure_ratio": ratio, "publish_blocked_problem_concentration": ratio, "publish_adherence_summary": "stable" if ratio < 0.1 else "degraded"}
    return _make_output(scope_type, scope_ref, "PUBLISH_OUTCOME", "publish_outcome", status, payload, "publish failures elevated", s)


def _compute_retry_burden(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    jobs = s["jobs"]
    total = max(1, len(jobs))
    retries = sum(1 for j in jobs if j.get("retry_of_job_id") is not None)
    ratio = retries / total
    status = _classify_status(ratio=ratio, warn_threshold=0.15, risk_threshold=0.35)
    payload = {"retry_count": retries, "retry_rate": ratio, "heavy_retry_concentration_flag": ratio >= 0.15, "retry_burden_status": status}
    return _make_output(scope_type, scope_ref, "RETRY_BURDEN", "retry_burden", status, payload, "retry burden elevated", s)


def _compute_readiness(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    total = max(1, s["planned_count"])
    blocked = s["planned_failed_count"]
    ratio = blocked / total
    status = _classify_status(ratio=ratio, warn_threshold=0.1, risk_threshold=0.25)
    payload = {"readiness_healthy_count": total - blocked, "readiness_blocked_count": blocked, "readiness_health_ratio": (total - blocked) / total, "readiness_blocker_concentration_flag": ratio >= 0.1}
    return _make_output(scope_type, scope_ref, "READINESS", "readiness", status, payload, "readiness blockers detected", s)


def _compute_drift_reconcile(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    jobs = s["jobs"]
    drift = sum(1 for j in jobs if str(j.get("publish_state") or "") == "publish_state_drift_detected")
    total = max(1, len(jobs))
    ratio = drift / total
    status = _classify_status(ratio=ratio, warn_threshold=0.05, risk_threshold=0.2)
    payload = {"drift_count": drift, "drift_frequency": ratio, "reconcile_failure_problem_count": drift, "drift_burden_status": status}
    return _make_output(scope_type, scope_ref, "DRIFT_RECONCILE", "drift_reconcile", status, payload, "drift burden observed", s)


def _compute_cadence(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    jobs = s["jobs"]
    delayed = sum(1 for j in jobs if (float(j.get("updated_at") or 0.0) - float(j.get("created_at") or 0.0)) > 86400)
    total = max(1, len(jobs))
    ratio = delayed / total
    status = _classify_status(ratio=ratio, warn_threshold=0.1, risk_threshold=0.3)
    payload = {"on_time_count": total - delayed, "delayed_count": delayed, "schedule_adherence_ratio": (total - delayed) / total, "cadence_degradation_flag": ratio >= 0.1}
    return _make_output(scope_type, scope_ref, "CADENCE_ADHERENCE", "cadence_adherence", status, payload, "cadence degraded", s)


def _compute_batch_completeness(scope_type: str, scope_ref: str, s: dict[str, Any]) -> KpiOutput:
    total = max(1, s["planned_count"])
    incomplete = s["planned_failed_count"]
    ratio = (total - incomplete) / total
    risk_ratio = incomplete / total
    status = _classify_status(ratio=risk_ratio, warn_threshold=0.1, risk_threshold=0.25)
    payload = {"batch_completeness_ratio": ratio, "incomplete_batch_count": incomplete, "batch_risk_concentration_flag": risk_ratio >= 0.1}
    return _make_output(scope_type, scope_ref, "BATCH_COMPLETENESS", "batch_completeness", status, payload, "batch incompleteness concentrated", s)
