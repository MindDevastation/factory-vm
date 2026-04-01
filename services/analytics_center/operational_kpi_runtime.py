from __future__ import annotations

import logging
import sqlite3
from typing import Any

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_INVALID_RECOMPUTE_MODE,
    E5A_INVALID_RUN_STATE,
    E5A_OPERATIONAL_KPI_SUPERSESSION_CONFLICT,
    E5A_OPERATIONAL_RECOMPUTE_CONFLICT,
)
from services.analytics_center.literals import (
    ANALYTICS_OPERATIONAL_RECOMPUTE_MODES,
    ANALYTICS_OPERATIONAL_RUN_STATES,
)
from services.analytics_center.operational_kpi import derive_operational_kpis, normalize_problem_listing_filters
from services.analytics_center.write_service import SnapshotWriteInput, write_snapshot
from services.common.db import now_ts

logger = logging.getLogger(__name__)


def _emit_operational_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    target_scope_type: str,
    target_scope_ref: str,
    recompute_mode: str,
    run_state: str,
    anomaly_count: int,
    risk_count: int,
    kpi_family: str | None = None,
    kpi_code: str | None = None,
    status_class: str | None = None,
    snapshot_id: int | None = None,
    payload_json: dict[str, Any] | None = None,
) -> None:
    payload = payload_json or {}
    conn.execute(
        """
        INSERT INTO analytics_operational_kpi_events(
            event_type, target_scope_type, target_scope_ref, kpi_family, kpi_code, status_class, snapshot_id,
            recompute_mode, run_state, anomaly_count, risk_count, payload_json, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            event_type,
            target_scope_type,
            target_scope_ref,
            kpi_family,
            kpi_code,
            status_class,
            snapshot_id,
            recompute_mode,
            run_state,
            int(anomaly_count),
            int(risk_count),
            __import__("json").dumps(payload, sort_keys=True),
            now_ts(),
        ),
    )
    logger.info(
        "operational_kpi_event event_type=%s target_scope_type=%s target_scope_ref=%s kpi_family=%s kpi_code=%s status_class=%s snapshot_id=%s recompute_mode=%s run_state=%s anomaly_count=%s risk_count=%s",
        event_type,
        target_scope_type,
        target_scope_ref,
        kpi_family or "-",
        kpi_code or "-",
        status_class or "-",
        snapshot_id if snapshot_id is not None else -1,
        recompute_mode,
        run_state,
        int(anomaly_count),
        int(risk_count),
    )


def _validate_recompute_mode(mode: str) -> str:
    normalized = str(mode or "").strip().upper()
    if normalized not in ANALYTICS_OPERATIONAL_RECOMPUTE_MODES:
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMPUTE_MODE, message="invalid recompute mode")
    return normalized


def _validate_run_state(state: str) -> str:
    normalized = str(state or "").strip().upper()
    if normalized not in ANALYTICS_OPERATIONAL_RUN_STATES:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="invalid run state")
    return normalized


def create_recompute_run(
    conn: sqlite3.Connection,
    *,
    target_scope_type: str,
    target_scope_ref: str,
    recompute_mode: str,
    observed_from: float | None,
    observed_to: float | None,
) -> int:
    mode = _validate_recompute_mode(recompute_mode)
    running = conn.execute(
        """
        SELECT id FROM analytics_operational_kpi_runs
        WHERE target_scope_type = ? AND target_scope_ref = ? AND recompute_mode = ? AND run_state = 'RUNNING'
        LIMIT 1
        """,
        (target_scope_type, target_scope_ref, mode),
    ).fetchone()
    if running is not None:
        raise AnalyticsDomainError(code=E5A_OPERATIONAL_RECOMPUTE_CONFLICT, message="RUNNING recompute already exists")
    row = conn.execute(
        """
        INSERT INTO analytics_operational_kpi_runs(
            target_scope_type, target_scope_ref, recompute_mode, run_state,
            observed_from, observed_to, started_at
        ) VALUES(?,?,?,?,?,?,?)
        """,
        (target_scope_type, target_scope_ref, mode, "RUNNING", observed_from, observed_to, now_ts()),
    )
    return int(row.lastrowid)


def finalize_recompute_run(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    run_state: str,
    computed_kpi_count: int,
    anomaly_count: int,
    risk_count: int,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> None:
    final_state = _validate_run_state(run_state)
    if final_state == "RUNNING":
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="finalize requires terminal state")
    row = conn.execute("SELECT run_state FROM analytics_operational_kpi_runs WHERE id = ?", (int(run_id),)).fetchone()
    if row is None:
        raise AnalyticsDomainError(code=E5A_INVALID_RUN_STATE, message="run not found")
    if str(row["run_state"]).upper() != "RUNNING":
        raise AnalyticsDomainError(code=E5A_OPERATIONAL_RECOMPUTE_CONFLICT, message="run already finalized")
    conn.execute(
        """
        UPDATE analytics_operational_kpi_runs
        SET run_state = ?, completed_at = ?, computed_kpi_count = ?, anomaly_count = ?, risk_count = ?, error_code = ?, error_detail = ?
        WHERE id = ?
        """,
        (final_state, now_ts(), int(computed_kpi_count), int(anomaly_count), int(risk_count), error_code, error_detail, int(run_id)),
    )


def recompute_operational_kpis(
    conn: sqlite3.Connection,
    *,
    target_scope_type: str,
    target_scope_ref: str,
    recompute_mode: str,
    observed_from: float | None = None,
    observed_to: float | None = None,
) -> int:
    scope_type = str(target_scope_type or "").strip().upper()
    mode = _validate_recompute_mode(recompute_mode)
    run_id = create_recompute_run(
        conn,
        target_scope_type=scope_type,
        target_scope_ref=target_scope_ref,
        recompute_mode=mode,
        observed_from=observed_from,
        observed_to=observed_to,
    )
    _emit_operational_event(
        conn,
        event_type="OPERATIONAL_KPI_RECOMPUTE_STARTED",
        target_scope_type=scope_type,
        target_scope_ref=target_scope_ref,
        recompute_mode=mode,
        run_state="RUNNING",
        anomaly_count=0,
        risk_count=0,
        payload_json={"run_id": run_id, "observed_from": observed_from, "observed_to": observed_to},
    )
    success = 0
    anomalies = 0
    risks = 0
    try:
        kpis = derive_operational_kpis(conn, scope_type=scope_type, scope_ref=target_scope_ref)
        for kpi in kpis:
            if kpi.status_class in {"ANOMALY", "RISK"} and not kpi.explainability_payload:
                raise AnalyticsDomainError(code=E5A_OPERATIONAL_KPI_SUPERSESSION_CONFLICT, message="explainability required")
            snapshot_id, superseded_count = _persist_operational_kpi_snapshot(
                conn,
                run_id=run_id,
                kpi=kpi,
                observed_from=observed_from,
                observed_to=observed_to,
            )
            _emit_operational_event(
                conn,
                event_type="OPERATIONAL_KPI_SNAPSHOT_CREATED",
                target_scope_type=scope_type,
                target_scope_ref=target_scope_ref,
                kpi_family=kpi.kpi_family,
                kpi_code=kpi.kpi_code,
                status_class=kpi.status_class,
                snapshot_id=snapshot_id,
                recompute_mode=mode,
                run_state="RUNNING",
                anomaly_count=anomalies,
                risk_count=risks,
                payload_json={"run_id": run_id},
            )
            if superseded_count > 0:
                _emit_operational_event(
                    conn,
                    event_type="OPERATIONAL_KPI_SNAPSHOT_SUPERSEDED",
                    target_scope_type=scope_type,
                    target_scope_ref=target_scope_ref,
                    kpi_family=kpi.kpi_family,
                    kpi_code=kpi.kpi_code,
                    status_class=kpi.status_class,
                    snapshot_id=snapshot_id,
                    recompute_mode=mode,
                    run_state="RUNNING",
                    anomaly_count=anomalies,
                    risk_count=risks,
                    payload_json={"run_id": run_id, "superseded_count": superseded_count},
                )
            success += 1
            if kpi.status_class == "ANOMALY":
                anomalies += 1
                _emit_operational_event(
                    conn,
                    event_type="OPERATIONAL_KPI_ANOMALY_DETECTED",
                    target_scope_type=scope_type,
                    target_scope_ref=target_scope_ref,
                    kpi_family=kpi.kpi_family,
                    kpi_code=kpi.kpi_code,
                    status_class=kpi.status_class,
                    snapshot_id=snapshot_id,
                    recompute_mode=mode,
                    run_state="RUNNING",
                    anomaly_count=anomalies,
                    risk_count=risks,
                    payload_json={"run_id": run_id},
                )
            if kpi.status_class == "RISK":
                risks += 1
                _emit_operational_event(
                    conn,
                    event_type="OPERATIONAL_KPI_RISK_DETECTED",
                    target_scope_type=scope_type,
                    target_scope_ref=target_scope_ref,
                    kpi_family=kpi.kpi_family,
                    kpi_code=kpi.kpi_code,
                    status_class=kpi.status_class,
                    snapshot_id=snapshot_id,
                    recompute_mode=mode,
                    run_state="RUNNING",
                    anomaly_count=anomalies,
                    risk_count=risks,
                    payload_json={"run_id": run_id},
                )
            if kpi.explainability_payload:
                _emit_operational_event(
                    conn,
                    event_type="OPERATIONAL_KPI_EXPLAINABILITY_PAYLOAD_ATTACHED",
                    target_scope_type=scope_type,
                    target_scope_ref=target_scope_ref,
                    kpi_family=kpi.kpi_family,
                    kpi_code=kpi.kpi_code,
                    status_class=kpi.status_class,
                    snapshot_id=snapshot_id,
                    recompute_mode=mode,
                    run_state="RUNNING",
                    anomaly_count=anomalies,
                    risk_count=risks,
                    payload_json={"run_id": run_id},
                )
        finalize_recompute_run(
            conn,
            run_id=run_id,
            run_state="SUCCEEDED",
            computed_kpi_count=success,
            anomaly_count=anomalies,
            risk_count=risks,
        )
        _emit_operational_event(
            conn,
            event_type="OPERATIONAL_KPI_RECOMPUTE_COMPLETED",
            target_scope_type=scope_type,
            target_scope_ref=target_scope_ref,
            recompute_mode=mode,
            run_state="SUCCEEDED",
            anomaly_count=anomalies,
            risk_count=risks,
            payload_json={"run_id": run_id, "computed_kpi_count": success},
        )
    except Exception as exc:
        state = "PARTIAL" if success > 0 else "FAILED"
        finalize_recompute_run(
            conn,
            run_id=run_id,
            run_state=state,
            computed_kpi_count=success,
            anomaly_count=anomalies,
            risk_count=risks,
            error_code=getattr(exc, "code", None),
            error_detail=str(exc),
        )
        _emit_operational_event(
            conn,
            event_type=(
                "OPERATIONAL_KPI_RECOMPUTE_PARTIAL_RECORDED"
                if state == "PARTIAL"
                else "OPERATIONAL_KPI_RECOMPUTE_FAILURE_RECORDED"
            ),
            target_scope_type=scope_type,
            target_scope_ref=target_scope_ref,
            recompute_mode=mode,
            run_state=state,
            anomaly_count=anomalies,
            risk_count=risks,
            payload_json={"run_id": run_id, "error_code": getattr(exc, "code", None), "error_detail": str(exc)},
        )
        if state == "FAILED":
            raise
    return run_id


def _persist_operational_kpi_snapshot(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    kpi: Any,
    observed_from: float | None,
    observed_to: float | None,
) -> tuple[int, int]:
    now = now_ts()
    mf1_entity_type = kpi.scope_type.replace("BATCH_MONTH", "BATCH")
    mf1_entity_ref = str(kpi.scope_ref)
    if mf1_entity_type == "CHANNEL":
        ch = conn.execute("SELECT id FROM channels WHERE slug = ? LIMIT 1", (str(kpi.scope_ref),)).fetchone()
        if ch is not None:
            mf1_entity_ref = str(ch["id"])
    try:
        supersede_row = conn.execute(
            """
            UPDATE analytics_operational_kpi_snapshots
            SET is_current = 0, updated_at = ?
            WHERE scope_type = ? AND scope_ref = ? AND kpi_family = ? AND kpi_code = ?
              AND COALESCE(observed_from, -1) = COALESCE(?, -1)
              AND COALESCE(observed_to, -1) = COALESCE(?, -1)
              AND is_current = 1
            """,
            (now, kpi.scope_type, kpi.scope_ref, kpi.kpi_family, kpi.kpi_code, observed_from, observed_to),
        )
        row = conn.execute(
            """
            INSERT INTO analytics_operational_kpi_snapshots(
                run_id, scope_type, scope_ref, kpi_family, kpi_code, status_class,
                observed_from, observed_to, is_current,
                value_payload_json, explainability_payload_json, source_snapshot_refs_json,
                created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                run_id,
                kpi.scope_type,
                kpi.scope_ref,
                kpi.kpi_family,
                kpi.kpi_code,
                kpi.status_class,
                observed_from,
                observed_to,
                1,
                __import__("json").dumps(kpi.value_payload, sort_keys=True),
                None if kpi.explainability_payload is None else __import__("json").dumps(kpi.explainability_payload, sort_keys=True),
                __import__("json").dumps(kpi.source_snapshot_refs, sort_keys=True),
                now,
                now,
            ),
        )
        write_snapshot(
            conn,
            SnapshotWriteInput(
                entity_type=mf1_entity_type,
                entity_ref=mf1_entity_ref,
                source_family="DERIVED_ROLLUP",
                window_type="BOUNDED_WINDOW",
                snapshot_status="CURRENT",
                freshness_status="FRESH",
                payload_json={"kpi_family": kpi.kpi_family, "kpi_code": kpi.kpi_code, "value": kpi.value_payload},
                explainability_json=kpi.explainability_payload or {},
                lineage_json={"source_snapshot_refs": kpi.source_snapshot_refs},
                anomaly_markers_json=[] if kpi.status_class == "NORMAL" else [kpi.status_class],
                captured_at=now,
                is_current=True,
                window_start_ts=observed_from,
                window_end_ts=observed_to,
            ),
        )
    except Exception as exc:
        raise AnalyticsDomainError(code=E5A_OPERATIONAL_KPI_SUPERSESSION_CONFLICT, message=str(exc)) from exc
    return int(row.lastrowid), int(supersede_row.rowcount or 0)


def read_operational_kpis(
    conn: sqlite3.Connection,
    *,
    scope_type: str,
    scope_ref: str,
    kpi_family: str | None = None,
    status_class: str | None = None,
    current_only: bool = True,
) -> list[dict[str, Any]]:
    filters = normalize_problem_listing_filters(scope_type=scope_type, kpi_family=kpi_family, status_class=status_class)
    clauses = ["scope_type = ?", "scope_ref = ?"]
    params: list[Any] = [filters["scope_type"], scope_ref]
    if filters["kpi_family"]:
        clauses.append("kpi_family = ?")
        params.append(filters["kpi_family"])
    if filters["status_class"]:
        clauses.append("status_class = ?")
        params.append(filters["status_class"])
    if current_only:
        clauses.append("is_current = 1")
    query = "SELECT * FROM analytics_operational_kpi_snapshots WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC, id DESC"
    return [dict(r) for r in conn.execute(query, tuple(params)).fetchall()]


def list_operational_problems(
    conn: sqlite3.Connection,
    *,
    scope_type: str | None = None,
    kpi_family: str | None = None,
    status_class: str | None = None,
) -> list[dict[str, Any]]:
    filters = normalize_problem_listing_filters(scope_type=scope_type, kpi_family=kpi_family, status_class=status_class)
    clauses = ["status_class IN ('ANOMALY', 'RISK')"]
    params: list[Any] = []
    if filters["scope_type"]:
        clauses.append("scope_type = ?")
        params.append(filters["scope_type"])
    if filters["kpi_family"]:
        clauses.append("kpi_family = ?")
        params.append(filters["kpi_family"])
    if filters["status_class"]:
        clauses.append("status_class = ?")
        params.append(filters["status_class"])
    query = "SELECT * FROM analytics_operational_kpi_snapshots WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC, id DESC"
    return [dict(r) for r in conn.execute(query, tuple(params)).fetchall()]
