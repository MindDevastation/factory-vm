from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from services.analytics_center.errors import AnalyticsDomainError, E5A_INVALID_REPORT_ARTIFACT_TYPE, E5A_INVALID_REPORT_SCOPE
from services.analytics_center.helpers import canonicalize_scope_ref
from services.analytics_center.literals import ANALYTICS_MF6_ARTIFACT_TYPES, ANALYTICS_MF6_GENERATION_STATUSES, ANALYTICS_MF6_REPORT_SCOPE_TYPES
from services.common.db import now_ts
from services.track_analysis_report.xlsx_export import export_report_to_xlsx_bytes

_SCOPE_ENTITY_TYPE = {
    "OVERVIEW": None,
    "CHANNEL": "CHANNEL",
    "RELEASE": "RELEASE",
    "BATCH_MONTH": "BATCH",
}

_ARTIFACTS_ROOT = Path(tempfile.gettempdir()) / "factory_analytics_reports"


_SUPPORTED_SOURCE_FAMILY_SCOPES: dict[str, set[str]] = {
    "OVERVIEW": set(),
    "CHANNEL": set(),
    "RELEASE": set(),
    "BATCH_MONTH": set(),
}

_ALLOWED_SOURCE_FAMILIES = {"INTERNAL_OPERATIONAL", "EXTERNAL_YOUTUBE"}


def validate_mf6_source_family_filter(*, context_scope: str, source_family: str | None) -> str | None:
    raw = str(source_family or "").strip()
    if not raw:
        return None
    normalized = raw.upper()
    if normalized not in _ALLOWED_SOURCE_FAMILIES:
        raise AnalyticsDomainError(code="E5A_INVALID_ANALYTICS_FILTER_COMBINATION", message="invalid source_family filter value")
    if normalized not in _SUPPORTED_SOURCE_FAMILY_SCOPES.get(str(context_scope).upper(), set()):
        raise AnalyticsDomainError(code="E5A_INVALID_ANALYTICS_FILTER_COMBINATION", message="source_family filter is not supported for this scope")
    return normalized


def validate_report_request(*, report_scope_type: str, artifact_type: str) -> None:
    if report_scope_type not in ANALYTICS_MF6_REPORT_SCOPE_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_REPORT_SCOPE, message="invalid report scope")
    if artifact_type not in ANALYTICS_MF6_ARTIFACT_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_REPORT_ARTIFACT_TYPE, message="invalid report artifact type")


def find_duplicate_report_request(conn: Any, *, report_scope_type: str, report_scope_ref: str | None, report_family: str, filter_payload: dict[str, Any], artifact_type: str) -> dict[str, Any] | None:
    payload = json.dumps(filter_payload, sort_keys=True)
    row = conn.execute(
        """
        SELECT * FROM analytics_report_records
        WHERE report_scope_type = ? AND COALESCE(report_scope_ref, '') = COALESCE(?, '')
          AND report_family = ? AND filter_payload_json = ? AND artifact_type = ?
          AND generation_status = 'READY'
        ORDER BY id DESC LIMIT 1
        """,
        (report_scope_type, report_scope_ref, report_family, payload, artifact_type),
    ).fetchone()
    return None if row is None else dict(row)


def _scope_params(*, conn: Any, report_scope_type: str, report_scope_ref: str | None, scope_type_col: str, scope_ref_col: str) -> tuple[str, tuple[Any, ...]]:
    if report_scope_type == "OVERVIEW":
        return "", ()
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=report_scope_type, scope_ref=str(report_scope_ref or ""))
    return f"WHERE {scope_type_col} = ? AND {scope_ref_col} = ?", (report_scope_type, canonical_scope_ref)


def _scope_snapshot_params(*, conn: Any, report_scope_type: str, report_scope_ref: str | None) -> tuple[str, tuple[Any, ...]]:
    entity_type = _SCOPE_ENTITY_TYPE[report_scope_type]
    if entity_type is None:
        return "", ()
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=report_scope_type, scope_ref=str(report_scope_ref or ""))
    return "WHERE entity_type = ? AND entity_ref = ?", (entity_type, canonical_scope_ref)


def _load_current_rows(conn: Any, *, table: str, where_sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    rows = conn.execute(f"SELECT * FROM {table} {where_sql} {'AND' if where_sql else 'WHERE'} is_current = 1 ORDER BY created_at DESC, id DESC", params).fetchall()
    return [dict(r) for r in rows]


def _load_current_snapshot_rows(conn: Any, *, where_sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    rows = conn.execute(f"SELECT * FROM analytics_snapshots {where_sql} {'AND' if where_sql else 'WHERE'} is_current = 1 ORDER BY captured_at DESC, id DESC", params).fetchall()
    return [dict(r) for r in rows]


def _apply_report_filters(dataset: dict[str, Any], *, filter_payload: dict[str, Any]) -> dict[str, Any]:
    filtered = dict(dataset)
    recs = list(filtered["recommendations"])
    predictions = list(filtered["predictions"])
    comparisons = list(filtered["comparisons"])
    planning_outputs = list(filtered["planning_outputs"])

    severity = str(filter_payload.get("severity") or "").upper()
    confidence = str(filter_payload.get("confidence") or "").upper()
    recommendation_family = str(filter_payload.get("recommendation_family") or "")
    target_domain = str(filter_payload.get("target_domain") or "")

    if severity:
        recs = [r for r in recs if str(r.get("severity_class") or "").upper() == severity]
    if confidence:
        recs = [r for r in recs if str(r.get("confidence_class") or "").upper() == confidence]
        predictions = [r for r in predictions if str(r.get("confidence_class") or "").upper() == confidence]
    if recommendation_family:
        recs = [r for r in recs if str(r.get("recommendation_family") or "") == recommendation_family]
    if target_domain:
        recs = [r for r in recs if str(r.get("target_domain") or "") == target_domain]

    source_family = str(filter_payload.get("source_family") or "")
    if source_family:
        comparisons = [r for r in comparisons if str(r.get("source_family") or "") == source_family]

    filtered["recommendations"] = recs
    filtered["predictions"] = predictions
    filtered["comparisons"] = comparisons
    filtered["planning_outputs"] = planning_outputs
    return filtered


def _build_report_dataset(conn: Any, *, report_scope_type: str, report_scope_ref: str | None, report_family: str, filter_payload: dict[str, Any]) -> dict[str, Any]:
    validate_mf6_source_family_filter(context_scope=report_scope_type, source_family=filter_payload.get("source_family"))
    snap_where, snap_params = _scope_snapshot_params(conn=conn, report_scope_type=report_scope_type, report_scope_ref=report_scope_ref)
    kpi_where, kpi_params = _scope_params(conn=conn, report_scope_type=report_scope_type, report_scope_ref=report_scope_ref, scope_type_col="scope_type", scope_ref_col="scope_ref")
    cmp_where, cmp_params = _scope_params(conn=conn, report_scope_type=report_scope_type, report_scope_ref=report_scope_ref, scope_type_col="scope_type", scope_ref_col="scope_ref")
    pred_where, pred_params = _scope_params(conn=conn, report_scope_type=report_scope_type, report_scope_ref=report_scope_ref, scope_type_col="scope_type", scope_ref_col="scope_ref")
    rec_where, rec_params = _scope_params(conn=conn, report_scope_type=report_scope_type, report_scope_ref=report_scope_ref, scope_type_col="recommendation_scope_type", scope_ref_col="recommendation_scope_ref")

    dataset = {
        "snapshots": _load_current_snapshot_rows(conn, where_sql=snap_where, params=snap_params),
        "operational_kpis": _load_current_rows(conn, table="analytics_operational_kpi_snapshots", where_sql=kpi_where, params=kpi_params),
        "comparisons": _load_current_rows(conn, table="analytics_comparison_snapshots", where_sql=cmp_where, params=cmp_params),
        "predictions": _load_current_rows(conn, table="analytics_prediction_snapshots", where_sql=pred_where, params=pred_params),
        "recommendations": _load_current_rows(conn, table="analytics_recommendation_snapshots", where_sql=rec_where, params=rec_params),
        "planning_outputs": [
            row
            for row in _load_current_rows(conn, table="analytics_recommendation_snapshots", where_sql=rec_where, params=rec_params)
            if str(row.get("target_domain") or "") == "PLANNER"
            or str(row.get("recommendation_family") or "") == "CONTENT_PLANNING_SUGGESTION"
        ],
    }
    required_non_empty = {"snapshots", "operational_kpis", "comparisons", "predictions", "recommendations"}
    missing = sorted([name for name, rows in dataset.items() if name in required_non_empty and len(rows) == 0])
    if missing:
        raise AnalyticsDomainError(code=E5A_INVALID_REPORT_SCOPE, message=f"missing required source data: {', '.join(missing)}")

    filtered = _apply_report_filters(dataset, filter_payload=filter_payload)
    for key in ("comparisons", "predictions", "recommendations"):
        if len(filtered[key]) == 0:
            raise AnalyticsDomainError(code=E5A_INVALID_REPORT_SCOPE, message=f"missing required source data: {key}")

    return {
        "report_scope_type": report_scope_type,
        "report_scope_ref": report_scope_ref,
        "report_family": report_family,
        "filter_payload": dict(filter_payload),
        "dataset": filtered,
        "dataset_counts": {k: len(v) for k, v in filtered.items()},
    }


def _ensure_artifact_dir() -> Path:
    _ARTIFACTS_ROOT.mkdir(parents=True, exist_ok=True)
    return _ARTIFACTS_ROOT


def _generate_artifact(*, record_id: int, artifact_type: str, dataset: dict[str, Any]) -> str:
    root = _ensure_artifact_dir()
    if artifact_type == "STRUCTURED_REPORT":
        path = root / f"report_{record_id}_structured.json"
        path.write_text(json.dumps(dataset, sort_keys=True), encoding="utf-8")
        return str(path)
    if artifact_type == "API_REPORT":
        payload = {"report_payload": dataset, "generated_for": "API_REPORT"}
        path = root / f"report_{record_id}_api_payload.json"
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        return str(path)

    columns = [
        {"group": "REPORT", "key": "report_scope_type"},
        {"group": "REPORT", "key": "report_scope_ref"},
        {"group": "SOURCE", "key": "source_table"},
        {"group": "SOURCE", "key": "source_row_id"},
        {"group": "SIGNAL", "key": "signal_family"},
        {"group": "SIGNAL", "key": "status_or_class"},
    ]
    rows: list[dict[str, Any]] = []
    for row in dataset["dataset"]["operational_kpis"]:
        rows.append({"report_scope_type": dataset["report_scope_type"], "report_scope_ref": dataset["report_scope_ref"], "source_table": "analytics_operational_kpi_snapshots", "source_row_id": row.get("id"), "signal_family": row.get("kpi_family"), "status_or_class": row.get("status_class")})
    for row in dataset["dataset"]["comparisons"]:
        rows.append({"report_scope_type": dataset["report_scope_type"], "report_scope_ref": dataset["report_scope_ref"], "source_table": "analytics_comparison_snapshots", "source_row_id": row.get("id"), "signal_family": row.get("comparison_family"), "status_or_class": row.get("variance_class")})
    for row in dataset["dataset"]["predictions"]:
        rows.append({"report_scope_type": dataset["report_scope_type"], "report_scope_ref": dataset["report_scope_ref"], "source_table": "analytics_prediction_snapshots", "source_row_id": row.get("id"), "signal_family": row.get("prediction_family"), "status_or_class": row.get("variance_class")})
    for row in dataset["dataset"]["recommendations"]:
        rows.append({"report_scope_type": dataset["report_scope_type"], "report_scope_ref": dataset["report_scope_ref"], "source_table": "analytics_recommendation_snapshots", "source_row_id": row.get("id"), "signal_family": row.get("recommendation_family"), "status_or_class": row.get("severity_class")})
    for row in dataset["dataset"]["planning_outputs"]:
        rows.append({"report_scope_type": dataset["report_scope_type"], "report_scope_ref": dataset["report_scope_ref"], "source_table": "analytics_planning_outputs", "source_row_id": row.get("id"), "signal_family": row.get("recommendation_family"), "status_or_class": row.get("lifecycle_status")})

    path = root / f"report_{record_id}.xlsx"
    content = export_report_to_xlsx_bytes({"columns": columns, "rows": rows}, sheet_title=f"analytics_{dataset['report_scope_type'].lower()}")
    path.write_bytes(content)
    return str(path)


def create_report_record(conn: Any, *, report_scope_type: str, report_scope_ref: str | None, report_family: str, filter_payload: dict[str, Any], artifact_type: str, created_by: str) -> int:
    validate_report_request(report_scope_type=report_scope_type, artifact_type=artifact_type)
    dedupe = find_duplicate_report_request(
        conn,
        report_scope_type=report_scope_type,
        report_scope_ref=report_scope_ref,
        report_family=report_family,
        filter_payload=filter_payload,
        artifact_type=artifact_type,
    )
    if dedupe is not None:
        return int(dedupe["id"])
    now = now_ts()
    row = conn.execute(
        """
        INSERT INTO analytics_report_records(
            report_scope_type, report_scope_ref, report_family, filter_payload_json,
            artifact_type, artifact_ref, generation_status, created_at, created_by
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            report_scope_type,
            report_scope_ref,
            report_family,
            json.dumps(filter_payload, sort_keys=True),
            artifact_type,
            None,
            "PENDING",
            now,
            created_by,
        ),
    )
    report_id = int(row.lastrowid)
    try:
        dataset = _build_report_dataset(conn, report_scope_type=report_scope_type, report_scope_ref=report_scope_ref, report_family=report_family, filter_payload=filter_payload)
        artifact_ref = _generate_artifact(record_id=report_id, artifact_type=artifact_type, dataset=dataset)
        conn.execute("UPDATE analytics_report_records SET artifact_ref = ?, generation_status = 'READY' WHERE id = ?", (artifact_ref, report_id))
    except Exception:
        conn.execute("UPDATE analytics_report_records SET generation_status = 'FAILED' WHERE id = ?", (report_id,))
        raise
    return report_id


def list_report_records(conn: Any, *, report_scope_type: str | None = None, generation_status: str | None = None) -> list[dict[str, Any]]:
    if generation_status and generation_status not in ANALYTICS_MF6_GENERATION_STATUSES:
        raise AnalyticsDomainError(code=E5A_INVALID_REPORT_SCOPE, message="invalid generation status")
    clauses = ["1=1"]
    params: list[Any] = []
    if report_scope_type:
        clauses.append("report_scope_type = ?")
        params.append(report_scope_type)
    if generation_status:
        clauses.append("generation_status = ?")
        params.append(generation_status)
    rows = conn.execute("SELECT * FROM analytics_report_records WHERE " + " AND ".join(clauses) + " ORDER BY created_at DESC, id DESC", tuple(params)).fetchall()
    return [dict(r) for r in rows]


def build_related_domain_jump(*, target_domain: str, scope_ref: str, next_action: str) -> dict[str, str]:
    allowed = {
        "PUBLISH": "/ui/publish/queue",
        "METADATA": "/ui/metadata/defaults",
        "VISUALS": "/ui/visual/release",
        "PLANNER": "/planner",
        "OPERATIONAL_TROUBLESHOOTING": "/ui/health/workers",
    }
    if target_domain not in allowed:
        raise AnalyticsDomainError(code=E5A_INVALID_REPORT_SCOPE, message="invalid related-domain target")
    return {"target_domain": target_domain, "path": allowed[target_domain], "scope_ref": scope_ref, "next_action": next_action}
