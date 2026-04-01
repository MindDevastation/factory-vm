from __future__ import annotations

import json
import logging
from typing import Any

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_BASELINE_REFERENCE_MISSING,
    E5A_INVALID_RUN_STATE,
    E5A_PREDICTION_RECOMPUTE_CONFLICT,
)
from services.analytics_center.literals import (
    ANALYTICS_MF4_COMPARISON_FAMILIES,
    ANALYTICS_MF4_PREDICTION_FAMILIES,
    ANALYTICS_MF4_RUN_KINDS,
    ANALYTICS_MF4_SCOPE_TYPES,
    ANALYTICS_MF4_VARIANCE_CLASSES,
    ANALYTICS_OPERATIONAL_RECOMPUTE_MODES,
    ANALYTICS_OPERATIONAL_RUN_STATES,
)
from services.analytics_center.mf4_derivation_core import (
    Mf4BaselineOutput,
    Mf4ComparisonOutput,
    derive_baselines,
    derive_comparisons,
    derive_predictions,
    persist_mf4_derivation,
)
from services.common.db import now_ts

logger = logging.getLogger(__name__)


def _emit_prediction_event(
    conn: Any,
    *,
    event_type: str,
    target_scope_type: str,
    target_scope_ref: str,
    run_kind: str,
    anomaly_count: int,
    risk_count: int,
    prediction_family: str | None = None,
    comparison_family: str | None = None,
    variance_class: str | None = None,
    confidence_class: str | None = None,
    snapshot_id: int | None = None,
    payload_json: dict[str, Any] | None = None,
) -> None:
    payload = payload_json or {}
    conn.execute(
        """
        INSERT INTO analytics_prediction_events(
            event_type, target_scope_type, target_scope_ref, run_kind, prediction_family, comparison_family,
            variance_class, confidence_class, snapshot_id, anomaly_count, risk_count, payload_json, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            event_type,
            target_scope_type,
            target_scope_ref,
            run_kind,
            prediction_family,
            comparison_family,
            variance_class,
            confidence_class,
            snapshot_id,
            int(anomaly_count),
            int(risk_count),
            json.dumps(payload, sort_keys=True),
            now_ts(),
        ),
    )
    logger.info(
        "mf4_event event_type=%s target_scope_type=%s target_scope_ref=%s run_kind=%s prediction_family=%s comparison_family=%s variance_class=%s confidence_class=%s snapshot_id=%s anomaly_count=%s risk_count=%s",
        event_type,
        target_scope_type,
        target_scope_ref,
        run_kind,
        prediction_family or "-",
        comparison_family or "-",
        variance_class or "-",
        confidence_class or "-",
        snapshot_id if snapshot_id is not None else -1,
        int(anomaly_count),
        int(risk_count),
    )


def _validate_scope(scope_type: str) -> str:
    normalized = str(scope_type or "").strip().upper()
    if normalized not in ANALYTICS_MF4_SCOPE_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid scope")
    return normalized


def _validate_run_kind(run_kind: str) -> str:
    normalized = str(run_kind or "").strip().upper()
    if normalized not in ANALYTICS_MF4_RUN_KINDS:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid run kind")
    return normalized


def _validate_recompute_mode(recompute_mode: str) -> str:
    normalized = str(recompute_mode or "").strip().upper()
    if normalized not in ANALYTICS_OPERATIONAL_RECOMPUTE_MODES:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid recompute mode")
    return normalized


def _validate_run_state(run_state: str) -> str:
    normalized = str(run_state or "").strip().upper()
    if normalized not in ANALYTICS_OPERATIONAL_RUN_STATES:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid run state")
    return normalized


def create_prediction_run(conn: Any, *, run_kind: str, target_scope_type: str, target_scope_ref: str, recompute_mode: str) -> int:
    kind = _validate_run_kind(run_kind)
    scope = _validate_scope(target_scope_type)
    mode = _validate_recompute_mode(recompute_mode)
    running = conn.execute(
        """
        SELECT id FROM analytics_prediction_runs
        WHERE run_kind = ? AND target_scope_type = ? AND target_scope_ref = ? AND recompute_mode = ? AND run_state = 'RUNNING'
        LIMIT 1
        """,
        (kind, scope, target_scope_ref, mode),
    ).fetchone()
    if running is not None:
        raise AnalyticsDomainError(code=E5A_PREDICTION_RECOMPUTE_CONFLICT, message="running prediction recompute already exists")
    row = conn.execute(
        """
        INSERT INTO analytics_prediction_runs(
            run_kind, target_scope_type, target_scope_ref, recompute_mode, run_state, started_at
        ) VALUES(?,?,?,?,?,?)
        """,
        (kind, scope, target_scope_ref, mode, "RUNNING", now_ts()),
    )
    return int(row.lastrowid)


def finalize_prediction_run(
    conn: Any,
    *,
    run_id: int,
    run_state: str,
    baseline_count: int,
    comparison_count: int,
    prediction_count: int,
    anomaly_count: int,
    risk_count: int,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> None:
    state = _validate_run_state(run_state)
    if state == "RUNNING":
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="terminal state required")
    row = conn.execute("SELECT run_state FROM analytics_prediction_runs WHERE id = ?", (int(run_id),)).fetchone()
    if row is None:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="run not found")
    if str(row["run_state"]).upper() != "RUNNING":
        raise AnalyticsDomainError(code=E5A_PREDICTION_RECOMPUTE_CONFLICT, message="run already finalized")
    conn.execute(
        """
        UPDATE analytics_prediction_runs
        SET run_state = ?, completed_at = ?, baseline_count = ?, comparison_count = ?, prediction_count = ?, anomaly_count = ?, risk_count = ?, error_code = ?, error_detail = ?
        WHERE id = ?
        """,
        (state, now_ts(), int(baseline_count), int(comparison_count), int(prediction_count), int(anomaly_count), int(risk_count), error_code, error_detail, int(run_id)),
    )


def _rows_to_baselines(rows: list[dict[str, Any]]) -> list[Mf4BaselineOutput]:
    outputs: list[Mf4BaselineOutput] = []
    for row in rows:
        outputs.append(
            Mf4BaselineOutput(
                scope_type=str(row["scope_type"]),
                scope_ref=str(row["scope_ref"]),
                baseline_family=str(row["baseline_family"]),
                variance_class=str(row["variance_class"]),
                baseline_payload=json.loads(str(row["baseline_payload_json"])),
                comparison_basis=json.loads(str(row["comparison_basis_json"])),
                source_snapshot_refs=json.loads(str(row["source_snapshot_refs_json"])),
            )
        )
    return outputs


def _rows_to_comparisons(rows: list[dict[str, Any]]) -> list[Mf4ComparisonOutput]:
    outputs: list[Mf4ComparisonOutput] = []
    baseline_map = {
        "RELEASE_VS_CHANNEL_BASELINE": "RELEASE_VS_CHANNEL",
        "CHANNEL_VS_SELF_HISTORY": "CHANNEL_HISTORICAL",
        "BATCH_MONTH_VS_RECENT_CHANNEL": "BATCH_MONTH_HISTORICAL",
        "CHANNEL_VS_PORTFOLIO": "PORTFOLIO_COMPARISON",
    }
    for row in rows:
        fam = str(row["comparison_family"])
        outputs.append(
            Mf4ComparisonOutput(
                scope_type=str(row["scope_type"]),
                scope_ref=str(row["scope_ref"]),
                comparison_family=fam,
                variance_class=str(row["variance_class"]),
                delta_payload=json.loads(str(row["delta_payload_json"])),
                comparison_basis=json.loads(str(row["comparison_basis_json"])),
                source_snapshot_refs=json.loads(str(row["source_snapshot_refs_json"])),
                baseline_family=baseline_map[fam],
            )
        )
    return outputs


def recompute_mf4(conn: Any, *, run_kind: str, target_scope_type: str, target_scope_ref: str, recompute_mode: str) -> int:
    run_id = create_prediction_run(
        conn,
        run_kind=run_kind,
        target_scope_type=target_scope_type,
        target_scope_ref=target_scope_ref,
        recompute_mode=recompute_mode,
    )
    baseline_count = 0
    comparison_count = 0
    prediction_count = 0
    anomaly_count = 0
    risk_count = 0
    try:
        kind = _validate_run_kind(run_kind)
        if kind in {"BASELINE_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            _emit_prediction_event(
                conn,
                event_type="MF4_BASELINE_RECOMPUTE_STARTED",
                target_scope_type=target_scope_type,
                target_scope_ref=target_scope_ref,
                run_kind=kind,
                anomaly_count=0,
                risk_count=0,
                payload_json={"run_id": run_id},
            )
        if kind in {"COMPARISON_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            _emit_prediction_event(
                conn,
                event_type="MF4_COMPARISON_RECOMPUTE_STARTED",
                target_scope_type=target_scope_type,
                target_scope_ref=target_scope_ref,
                run_kind=kind,
                anomaly_count=0,
                risk_count=0,
                payload_json={"run_id": run_id},
            )
        if kind in {"PREDICTION_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            _emit_prediction_event(
                conn,
                event_type="MF4_PREDICTION_RECOMPUTE_STARTED",
                target_scope_type=target_scope_type,
                target_scope_ref=target_scope_ref,
                run_kind=kind,
                anomaly_count=0,
                risk_count=0,
                payload_json={"run_id": run_id},
            )
        if kind in {"BASELINE_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            baselines = derive_baselines(conn, scope_type=target_scope_type, scope_ref=target_scope_ref)
        else:
            baseline_rows = conn.execute(
                "SELECT * FROM analytics_baseline_snapshots WHERE scope_type = ? AND scope_ref = ? AND is_current = 1",
                (target_scope_type, target_scope_ref),
            ).fetchall()
            if not baseline_rows:
                raise AnalyticsDomainError(code=E5A_BASELINE_REFERENCE_MISSING, message="baseline reference missing")
            baselines = _rows_to_baselines(list(baseline_rows))

        if kind in {"COMPARISON_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            comparisons = derive_comparisons(conn, baselines=baselines)
        else:
            comparison_rows = conn.execute(
                "SELECT * FROM analytics_comparison_snapshots WHERE scope_type = ? AND scope_ref = ? AND is_current = 1",
                (target_scope_type, target_scope_ref),
            ).fetchall()
            if not comparison_rows:
                raise AnalyticsDomainError(code=E5A_BASELINE_REFERENCE_MISSING, message="comparison baseline missing")
            comparisons = _rows_to_comparisons(list(comparison_rows))

        if kind in {"BASELINE_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            counts = persist_mf4_derivation(conn, baselines=baselines, comparisons=[], predictions=[])
            baseline_count += int(counts["baseline_count"])
            for baseline in baselines:
                snapshot_id = int(counts["baseline_ids"][baseline.baseline_family])
                _emit_prediction_event(
                    conn,
                    event_type="MF4_BASELINE_SNAPSHOT_CREATED",
                    target_scope_type=target_scope_type,
                    target_scope_ref=target_scope_ref,
                    run_kind=kind,
                    comparison_family=None,
                    prediction_family=None,
                    variance_class=baseline.variance_class,
                    confidence_class=None,
                    snapshot_id=snapshot_id,
                    anomaly_count=anomaly_count,
                    risk_count=risk_count,
                    payload_json={"baseline_family": baseline.baseline_family},
                )
        if kind in {"COMPARISON_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            counts = persist_mf4_derivation(conn, baselines=[], comparisons=comparisons, predictions=[])
            comparison_count += int(counts["comparison_count"])
            for comp in comparisons:
                snapshot_id = int(counts["comparison_ids"][comp.comparison_family])
                _emit_prediction_event(
                    conn,
                    event_type="MF4_COMPARISON_SNAPSHOT_CREATED",
                    target_scope_type=target_scope_type,
                    target_scope_ref=target_scope_ref,
                    run_kind=kind,
                    comparison_family=comp.comparison_family,
                    variance_class=comp.variance_class,
                    snapshot_id=snapshot_id,
                    anomaly_count=anomaly_count,
                    risk_count=risk_count,
                    payload_json={"delta_payload": comp.delta_payload},
                )
                if comp.variance_class == "ANOMALY":
                    _emit_prediction_event(
                        conn,
                        event_type="MF4_ANOMALY_CLASSIFIED",
                        target_scope_type=target_scope_type,
                        target_scope_ref=target_scope_ref,
                        run_kind=kind,
                        comparison_family=comp.comparison_family,
                        variance_class=comp.variance_class,
                        snapshot_id=snapshot_id,
                        anomaly_count=anomaly_count + 1,
                        risk_count=risk_count,
                    )
                if comp.variance_class == "RISK":
                    _emit_prediction_event(
                        conn,
                        event_type="MF4_RISK_CLASSIFIED",
                        target_scope_type=target_scope_type,
                        target_scope_ref=target_scope_ref,
                        run_kind=kind,
                        comparison_family=comp.comparison_family,
                        variance_class=comp.variance_class,
                        snapshot_id=snapshot_id,
                        anomaly_count=anomaly_count,
                        risk_count=risk_count + 1,
                    )
        if kind in {"PREDICTION_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            predictions = derive_predictions(conn, comparisons=comparisons)
        else:
            predictions = []
        if kind in {"PREDICTION_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            counts = persist_mf4_derivation(conn, baselines=[], comparisons=[], predictions=predictions)
            prediction_count += int(counts["prediction_count"])
            for pred in predictions:
                snapshot_id = int(counts["prediction_ids"][pred.prediction_family])
                _emit_prediction_event(
                    conn,
                    event_type="MF4_PREDICTION_SNAPSHOT_CREATED",
                    target_scope_type=target_scope_type,
                    target_scope_ref=target_scope_ref,
                    run_kind=kind,
                    prediction_family=pred.prediction_family,
                    comparison_family=pred.comparison_family,
                    variance_class=pred.variance_class,
                    confidence_class=pred.confidence_class,
                    snapshot_id=snapshot_id,
                    anomaly_count=anomaly_count,
                    risk_count=risk_count,
                    payload_json={"predicted_value": pred.predicted_value},
                )
                _emit_prediction_event(
                    conn,
                    event_type="MF4_EXPLAINABILITY_PAYLOAD_ATTACHED",
                    target_scope_type=target_scope_type,
                    target_scope_ref=target_scope_ref,
                    run_kind=kind,
                    prediction_family=pred.prediction_family,
                    comparison_family=pred.comparison_family,
                    variance_class=pred.variance_class,
                    confidence_class=pred.confidence_class,
                    snapshot_id=snapshot_id,
                    anomaly_count=anomaly_count,
                    risk_count=risk_count,
                    payload_json={
                        "comparison_basis": pred.comparison_basis,
                        "signals_used": pred.signals_used,
                        "next_interpretation": pred.explainability_payload.get("remediation_hint_or_next_interpretation"),
                    },
                )
        for comp in comparisons:
            if comp.variance_class == "ANOMALY":
                anomaly_count += 1
            if comp.variance_class == "RISK":
                risk_count += 1
        for pred in predictions:
            if pred.variance_class == "ANOMALY":
                anomaly_count += 1
            if pred.variance_class == "RISK":
                risk_count += 1
        finalize_prediction_run(
            conn,
            run_id=run_id,
            run_state="SUCCEEDED",
            baseline_count=baseline_count,
            comparison_count=comparison_count,
            prediction_count=prediction_count,
            anomaly_count=anomaly_count,
            risk_count=risk_count,
        )
        if kind in {"BASELINE_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            _emit_prediction_event(
                conn,
                event_type="MF4_BASELINE_RECOMPUTE_COMPLETED",
                target_scope_type=target_scope_type,
                target_scope_ref=target_scope_ref,
                run_kind=kind,
                anomaly_count=anomaly_count,
                risk_count=risk_count,
                payload_json={"run_id": run_id},
            )
        if kind in {"COMPARISON_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            _emit_prediction_event(
                conn,
                event_type="MF4_COMPARISON_RECOMPUTE_COMPLETED",
                target_scope_type=target_scope_type,
                target_scope_ref=target_scope_ref,
                run_kind=kind,
                anomaly_count=anomaly_count,
                risk_count=risk_count,
                payload_json={"run_id": run_id},
            )
        if kind in {"PREDICTION_RECOMPUTE", "FULL_STACK_RECOMPUTE"}:
            _emit_prediction_event(
                conn,
                event_type="MF4_PREDICTION_RECOMPUTE_COMPLETED",
                target_scope_type=target_scope_type,
                target_scope_ref=target_scope_ref,
                run_kind=kind,
                anomaly_count=anomaly_count,
                risk_count=risk_count,
                payload_json={"run_id": run_id},
            )
    except Exception as exc:
        final_state = "PARTIAL" if (baseline_count + comparison_count + prediction_count) > 0 else "FAILED"
        finalize_prediction_run(
            conn,
            run_id=run_id,
            run_state=final_state,
            baseline_count=baseline_count,
            comparison_count=comparison_count,
            prediction_count=prediction_count,
            anomaly_count=anomaly_count,
            risk_count=risk_count,
            error_code=getattr(exc, "code", None),
            error_detail=str(exc),
        )
        _emit_prediction_event(
            conn,
            event_type="MF4_RECOMPUTE_PARTIAL_FAILURE_RECORDED",
            target_scope_type=target_scope_type,
            target_scope_ref=target_scope_ref,
            run_kind=run_kind,
            anomaly_count=anomaly_count,
            risk_count=risk_count,
            payload_json={"run_id": run_id, "run_state": final_state, "error_code": getattr(exc, "code", None), "error_detail": str(exc)},
        )
        if final_state == "FAILED":
            raise
    return run_id


def read_mf4_baselines(conn: Any, *, scope_type: str, scope_ref: str, baseline_family: str | None = None, current_only: bool = True) -> list[dict[str, Any]]:
    clauses = ["scope_type = ?", "scope_ref = ?"]
    params: list[Any] = [scope_type, scope_ref]
    if baseline_family:
        clauses.append("baseline_family = ?")
        params.append(baseline_family)
    if current_only:
        clauses.append("is_current = 1")
    query = "SELECT * FROM analytics_baseline_snapshots WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC, id DESC"
    return [dict(r) for r in conn.execute(query, tuple(params)).fetchall()]


def read_mf4_comparisons(conn: Any, *, scope_type: str, scope_ref: str, comparison_family: str | None = None, current_only: bool = True) -> list[dict[str, Any]]:
    clauses = ["scope_type = ?", "scope_ref = ?"]
    params: list[Any] = [scope_type, scope_ref]
    if comparison_family:
        if comparison_family not in ANALYTICS_MF4_COMPARISON_FAMILIES:
            raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid comparison family")
        clauses.append("comparison_family = ?")
        params.append(comparison_family)
    if current_only:
        clauses.append("is_current = 1")
    query = "SELECT * FROM analytics_comparison_snapshots WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC, id DESC"
    return [dict(r) for r in conn.execute(query, tuple(params)).fetchall()]


def read_mf4_predictions(conn: Any, *, scope_type: str, scope_ref: str, prediction_family: str | None = None, current_only: bool = True) -> list[dict[str, Any]]:
    clauses = ["scope_type = ?", "scope_ref = ?"]
    params: list[Any] = [scope_type, scope_ref]
    if prediction_family:
        if prediction_family not in ANALYTICS_MF4_PREDICTION_FAMILIES:
            raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid prediction family")
        clauses.append("prediction_family = ?")
        params.append(prediction_family)
    if current_only:
        clauses.append("is_current = 1")
    query = "SELECT * FROM analytics_prediction_snapshots WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC, id DESC"
    return [dict(r) for r in conn.execute(query, tuple(params)).fetchall()]


def normalize_problem_risk_filters(*, scope_type: str | None = None, prediction_family: str | None = None, status_class: str | None = None) -> dict[str, str | None]:
    normalized_scope = None if scope_type is None else str(scope_type).strip().upper()
    normalized_family = None if prediction_family is None else str(prediction_family).strip().upper()
    normalized_status = None if status_class is None else str(status_class).strip().upper()
    if normalized_scope and normalized_scope not in ANALYTICS_MF4_SCOPE_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid scope filter")
    if normalized_family and normalized_family not in ANALYTICS_MF4_PREDICTION_FAMILIES:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid prediction family filter")
    if normalized_status and normalized_status not in ANALYTICS_MF4_VARIANCE_CLASSES:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid status class filter")
    return {"scope_type": normalized_scope, "prediction_family": normalized_family, "status_class": normalized_status}


def list_mf4_problems(conn: Any, *, scope_type: str | None = None, prediction_family: str | None = None, status_class: str | None = None) -> list[dict[str, Any]]:
    filters = normalize_problem_risk_filters(scope_type=scope_type, prediction_family=prediction_family, status_class=status_class)
    clauses = ["variance_class IN ('ANOMALY', 'RISK')"]
    params: list[Any] = []
    if filters["scope_type"]:
        clauses.append("scope_type = ?")
        params.append(filters["scope_type"])
    if filters["prediction_family"]:
        clauses.append("prediction_family = ?")
        params.append(filters["prediction_family"])
    if filters["status_class"]:
        clauses.append("variance_class = ?")
        params.append(filters["status_class"])
    query = "SELECT * FROM analytics_prediction_snapshots WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC, id DESC"
    return [dict(r) for r in conn.execute(query, tuple(params)).fetchall()]
