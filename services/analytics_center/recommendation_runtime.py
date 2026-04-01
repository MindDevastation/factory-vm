from __future__ import annotations

import json
import logging
from typing import Any

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_INVALID_RECOMMENDATION_STATUS,
    E5A_RECOMMENDATION_RECOMPUTE_CONFLICT,
)
from services.analytics_center.literals import (
    ANALYTICS_MF5_LIFECYCLE_STATUSES,
    ANALYTICS_MF5_RECOMPUTE_MODES,
    ANALYTICS_MF5_RUN_STATES,
    ANALYTICS_MF5_SCOPE_TYPES,
)
from services.analytics_center.recommendation_core import RecommendationOutput, persist_recommendation_snapshot, synthesize_recommendations
from services.common.db import now_ts

logger = logging.getLogger(__name__)


def _emit_recommendation_event(
    conn: Any,
    *,
    event_type: str,
    recommendation_scope_type: str,
    recommendation_scope_ref: str,
    recommendation_family: str | None = None,
    target_domain: str | None = None,
    severity_class: str | None = None,
    confidence_class: str | None = None,
    lifecycle_status: str | None = None,
    recommendation_id: int | None = None,
    run_state: str | None = None,
    payload_json: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO analytics_recommendation_events(
            event_type, recommendation_scope_type, recommendation_scope_ref, recommendation_family,
            target_domain, severity_class, confidence_class, lifecycle_status, recommendation_id,
            run_state, payload_json, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            event_type,
            recommendation_scope_type,
            recommendation_scope_ref,
            recommendation_family,
            target_domain,
            severity_class,
            confidence_class,
            lifecycle_status,
            recommendation_id,
            run_state,
            json.dumps(payload_json or {}, sort_keys=True),
            now_ts(),
        ),
    )
    logger.info(
        "mf5_event event_type=%s recommendation_scope_type=%s recommendation_scope_ref=%s recommendation_family=%s target_domain=%s severity_class=%s confidence_class=%s lifecycle_status=%s recommendation_id=%s run_state=%s",
        event_type,
        recommendation_scope_type,
        recommendation_scope_ref,
        recommendation_family or "-",
        target_domain or "-",
        severity_class or "-",
        confidence_class or "-",
        lifecycle_status or "-",
        recommendation_id if recommendation_id is not None else -1,
        run_state or "-",
    )


def _rank_severity(v: str) -> int:
    return {"CRITICAL": 3, "WARNING": 2, "INFO": 1}.get(v, 0)


def _rank_confidence(v: str) -> int:
    return {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(v, 0)


def validate_lifecycle_transition(*, current: str, target: str) -> None:
    current_u = str(current).upper()
    target_u = str(target).upper()
    if current_u not in ANALYTICS_MF5_LIFECYCLE_STATUSES or target_u not in ANALYTICS_MF5_LIFECYCLE_STATUSES:
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_STATUS, message="invalid recommendation status")
    allowed = {"OPEN": {"ACKNOWLEDGED", "DISMISSED", "SUPERSEDED"}, "ACKNOWLEDGED": {"DISMISSED"}, "DISMISSED": set(), "SUPERSEDED": set()}
    if target_u not in allowed[current_u]:
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_STATUS, message="invalid recommendation lifecycle transition")


def create_recommendation_run(conn: Any, *, recommendation_scope_type: str, recommendation_scope_ref: str, recommendation_family: str, recompute_mode: str) -> int:
    if recommendation_scope_type not in ANALYTICS_MF5_SCOPE_TYPES or recompute_mode not in ANALYTICS_MF5_RECOMPUTE_MODES:
        raise AnalyticsDomainError(code=E5A_RECOMMENDATION_RECOMPUTE_CONFLICT, message="invalid recommendation run input")
    existing = conn.execute(
        """
        SELECT id FROM analytics_recommendation_runs
        WHERE recommendation_scope_type = ? AND recommendation_scope_ref = ? AND recommendation_family = ?
          AND recompute_mode = ? AND run_state = 'RUNNING'
        """,
        (recommendation_scope_type, recommendation_scope_ref, recommendation_family, recompute_mode),
    ).fetchone()
    if existing is not None:
        raise AnalyticsDomainError(code=E5A_RECOMMENDATION_RECOMPUTE_CONFLICT, message="recommendation recompute already running")
    now = now_ts()
    row = conn.execute(
        """
        INSERT INTO analytics_recommendation_runs(
            recommendation_scope_type, recommendation_scope_ref, recommendation_family,
            recompute_mode, run_state, started_at, completed_at,
            recommendation_count, open_count, warning_count, critical_count,
            error_code, error_detail, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (recommendation_scope_type, recommendation_scope_ref, recommendation_family, recompute_mode, "RUNNING", now, None, 0, 0, 0, 0, None, None, now),
    )
    run_id = int(row.lastrowid)
    _emit_recommendation_event(
        conn,
        event_type="MF5_RECOMMENDATION_RECOMPUTE_STARTED",
        recommendation_scope_type=recommendation_scope_type,
        recommendation_scope_ref=recommendation_scope_ref,
        recommendation_family=recommendation_family,
        run_state="RUNNING",
        payload_json={"run_id": run_id},
    )
    return run_id


def finalize_recommendation_run(conn: Any, *, run_id: int, run_state: str, recommendation_count: int, open_count: int, warning_count: int, critical_count: int, error_code: str | None = None, error_detail: str | None = None) -> None:
    if run_state not in ANALYTICS_MF5_RUN_STATES:
        raise AnalyticsDomainError(code=E5A_RECOMMENDATION_RECOMPUTE_CONFLICT, message="invalid run state")
    run = conn.execute("SELECT recommendation_scope_type, recommendation_scope_ref, recommendation_family, run_state FROM analytics_recommendation_runs WHERE id = ?", (int(run_id),)).fetchone()
    if run is None or str(run["run_state"]) != "RUNNING":
        raise AnalyticsDomainError(code=E5A_RECOMMENDATION_RECOMPUTE_CONFLICT, message="run not running")
    conn.execute(
        """
        UPDATE analytics_recommendation_runs
        SET run_state = ?, completed_at = ?, recommendation_count = ?, open_count = ?, warning_count = ?, critical_count = ?, error_code = ?, error_detail = ?
        WHERE id = ?
        """,
        (run_state, now_ts(), int(recommendation_count), int(open_count), int(warning_count), int(critical_count), error_code, error_detail, int(run_id)),
    )
    _emit_recommendation_event(
        conn,
        event_type="MF5_RECOMMENDATION_RECOMPUTE_COMPLETED" if run_state == "SUCCEEDED" else "MF5_RECOMMENDATION_RUN_PARTIAL_FAILURE_RECORDED",
        recommendation_scope_type=str(run["recommendation_scope_type"]),
        recommendation_scope_ref=str(run["recommendation_scope_ref"]),
        recommendation_family=str(run["recommendation_family"]),
        run_state=run_state,
        payload_json={"run_id": int(run_id), "error_code": error_code, "error_detail": error_detail},
    )


def _supersede_open_for_issue(conn: Any, *, recommendation: RecommendationOutput) -> None:
    rows = conn.execute(
        """
        SELECT id, target_domain, severity_class, confidence_class
        FROM analytics_recommendation_snapshots
        WHERE recommendation_scope_type = ? AND recommendation_scope_ref = ?
          AND recommendation_family = ? AND issue_key = ?
          AND is_current = 1 AND lifecycle_status = 'OPEN'
        """,
        (recommendation.scope_type, recommendation.scope_ref, recommendation.recommendation_family, recommendation.issue_key),
    ).fetchall()
    conn.execute(
        """
        UPDATE analytics_recommendation_snapshots
        SET lifecycle_status = 'SUPERSEDED', is_current = 0, updated_at = ?
        WHERE recommendation_scope_type = ? AND recommendation_scope_ref = ?
          AND recommendation_family = ? AND issue_key = ?
          AND is_current = 1 AND lifecycle_status = 'OPEN'
        """,
        (now_ts(), recommendation.scope_type, recommendation.scope_ref, recommendation.recommendation_family, recommendation.issue_key),
    )
    for row in rows:
        _emit_recommendation_event(
            conn,
            event_type="MF5_RECOMMENDATION_SUPERSEDED",
            recommendation_scope_type=recommendation.scope_type,
            recommendation_scope_ref=recommendation.scope_ref,
            recommendation_family=recommendation.recommendation_family,
            target_domain=str(row["target_domain"]),
            severity_class=str(row["severity_class"]),
            confidence_class=str(row["confidence_class"]),
            lifecycle_status="SUPERSEDED",
            recommendation_id=int(row["id"]),
        )


def recompute_recommendations(conn: Any, *, recommendation_scope_type: str, recommendation_scope_ref: str, recommendation_family: str, recompute_mode: str) -> int:
    run_id = create_recommendation_run(
        conn,
        recommendation_scope_type=recommendation_scope_type,
        recommendation_scope_ref=recommendation_scope_ref,
        recommendation_family=recommendation_family,
        recompute_mode=recompute_mode,
    )
    count = warning = critical = 0
    try:
        recs = [r for r in synthesize_recommendations(conn, scope_type=recommendation_scope_type, scope_ref=recommendation_scope_ref) if r.recommendation_family == recommendation_family]
        for rec in recs:
            _supersede_open_for_issue(conn, recommendation=rec)
            rec_id = persist_recommendation_snapshot(conn, recommendation=rec, run_id=run_id)
            _emit_recommendation_event(conn, event_type="MF5_RECOMMENDATION_CREATED", recommendation_scope_type=rec.scope_type, recommendation_scope_ref=rec.scope_ref, recommendation_family=rec.recommendation_family, target_domain=rec.target_domain, severity_class=rec.severity_class, confidence_class=rec.confidence_class, lifecycle_status=rec.lifecycle_status, recommendation_id=rec_id, run_state="RUNNING")
            _emit_recommendation_event(conn, event_type="MF5_EXPLAINABILITY_PAYLOAD_ATTACHED", recommendation_scope_type=rec.scope_type, recommendation_scope_ref=rec.scope_ref, recommendation_family=rec.recommendation_family, target_domain=rec.target_domain, severity_class=rec.severity_class, confidence_class=rec.confidence_class, lifecycle_status=rec.lifecycle_status, recommendation_id=rec_id, run_state="RUNNING")
            _emit_recommendation_event(conn, event_type="MF5_TARGET_POINTER_ATTACHED", recommendation_scope_type=rec.scope_type, recommendation_scope_ref=rec.scope_ref, recommendation_family=rec.recommendation_family, target_domain=rec.target_domain, severity_class=rec.severity_class, confidence_class=rec.confidence_class, lifecycle_status=rec.lifecycle_status, recommendation_id=rec_id, run_state="RUNNING")
            count += 1
            warning += 1 if rec.severity_class == "WARNING" else 0
            critical += 1 if rec.severity_class == "CRITICAL" else 0
        finalize_recommendation_run(conn, run_id=run_id, run_state="SUCCEEDED", recommendation_count=count, open_count=count, warning_count=warning, critical_count=critical)
    except Exception as exc:
        state = "PARTIAL" if count > 0 else "FAILED"
        finalize_recommendation_run(
            conn,
            run_id=run_id,
            run_state=state,
            recommendation_count=count,
            open_count=count,
            warning_count=warning,
            critical_count=critical,
            error_code=getattr(exc, "code", None),
            error_detail=str(exc),
        )
        if state == "FAILED":
            raise
    return run_id


def read_recommendations(conn: Any, *, scope_type: str | None = None, recommendation_family: str | None = None, severity_class: str | None = None, confidence_class: str | None = None, lifecycle_status: str | None = None, target_domain: str | None = None, current_only: bool = False) -> list[dict[str, Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    if scope_type:
        clauses.append("recommendation_scope_type = ?")
        params.append(scope_type)
    if recommendation_family:
        clauses.append("recommendation_family = ?")
        params.append(recommendation_family)
    if severity_class:
        clauses.append("severity_class = ?")
        params.append(severity_class)
    if confidence_class:
        clauses.append("confidence_class = ?")
        params.append(confidence_class)
    if lifecycle_status:
        clauses.append("lifecycle_status = ?")
        params.append(lifecycle_status)
    if target_domain:
        clauses.append("target_domain = ?")
        params.append(target_domain)
    if current_only:
        clauses.append("is_current = 1")
    rows = conn.execute("SELECT * FROM analytics_recommendation_snapshots WHERE " + " AND ".join(clauses), tuple(params)).fetchall()
    return [dict(r) for r in rows]


def list_prioritized_recommendation_queue(conn: Any, *, scope_type: str | None = None) -> list[dict[str, Any]]:
    rows = read_recommendations(conn, scope_type=scope_type, lifecycle_status="OPEN", current_only=True)
    rows.sort(key=lambda r: (-_rank_severity(str(r["severity_class"])), -_rank_confidence(str(r["confidence_class"])), -float(r["created_at"]), {"RELEASE": 4, "CHANNEL": 3, "BATCH_MONTH": 2, "PORTFOLIO": 1}.get(str(r["recommendation_scope_type"]), 0)))
    return rows


def group_recommendations(conn: Any, *, by: str) -> dict[str, list[dict[str, Any]]]:
    if by not in {"scope", "family", "target_domain"}:
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_STATUS, message="invalid group by")
    rows = read_recommendations(conn, current_only=True)
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row["recommendation_scope_type"] if by == "scope" else row["recommendation_family"] if by == "family" else row["target_domain"])
        out.setdefault(key, []).append(row)
    return out


def update_recommendation_lifecycle(conn: Any, *, recommendation_id: int, target_status: str) -> None:
    row = conn.execute(
        "SELECT recommendation_scope_type, recommendation_scope_ref, recommendation_family, target_domain, severity_class, confidence_class, lifecycle_status FROM analytics_recommendation_snapshots WHERE id = ?",
        (int(recommendation_id),),
    ).fetchone()
    if row is None:
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_STATUS, message="recommendation not found")
    validate_lifecycle_transition(current=str(row["lifecycle_status"]), target=target_status)
    is_current = 0 if str(target_status).upper() in {"DISMISSED", "SUPERSEDED"} else 1
    conn.execute("UPDATE analytics_recommendation_snapshots SET lifecycle_status = ?, is_current = ?, updated_at = ? WHERE id = ?", (target_status.upper(), is_current, now_ts(), int(recommendation_id)))
    _emit_recommendation_event(
        conn,
        event_type="MF5_RECOMMENDATION_ACKNOWLEDGED" if str(target_status).upper() == "ACKNOWLEDGED" else "MF5_RECOMMENDATION_DISMISSED",
        recommendation_scope_type=str(row["recommendation_scope_type"]),
        recommendation_scope_ref=str(row["recommendation_scope_ref"]),
        recommendation_family=str(row["recommendation_family"]),
        target_domain=str(row["target_domain"]),
        severity_class=str(row["severity_class"]),
        confidence_class=str(row["confidence_class"]),
        lifecycle_status=str(target_status).upper(),
        recommendation_id=int(recommendation_id),
    )


def inspect_recommendation(conn: Any, *, recommendation_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM analytics_recommendation_snapshots WHERE id = ?", (int(recommendation_id),)).fetchone()
    if row is None:
        raise AnalyticsDomainError(code=E5A_INVALID_RECOMMENDATION_STATUS, message="recommendation not found")
    payload = dict(row)
    payload["target_pointer_payload_json"] = json.loads(str(payload["target_pointer_payload_json"]))
    payload["explainability_payload_json"] = json.loads(str(payload["explainability_payload_json"]))
    payload["source_snapshot_refs_json"] = json.loads(str(payload["source_snapshot_refs_json"]))
    return payload
