from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_INVALID_REPORT_ARTIFACT_TYPE,
    E5A_INVALID_REPORT_SCOPE,
)
from services.analytics_center.helpers import canonicalize_scope_ref
from services.analytics_center.literals import (
    ANALYTICS_MF6_ARTIFACT_TYPES,
    ANALYTICS_MF6_GENERATION_STATUSES,
    ANALYTICS_MF6_REPORT_SCOPE_TYPES,
)
from services.common.db import now_ts
from services.track_analysis_report.xlsx_export import export_report_to_xlsx_bytes

_SCOPE_ENTITY_TYPE = {
    "OVERVIEW": None,
    "CHANNEL": "CHANNEL",
    "RELEASE": "RELEASE",
    "BATCH_MONTH": "BATCH",
}

_ARTIFACTS_ROOT = Path(tempfile.gettempdir()) / "factory_analytics_reports"


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


def _load_latest_snapshot(conn: Any, *, report_scope_type: str, report_scope_ref: str | None) -> dict[str, Any] | None:
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=report_scope_type, scope_ref=str(report_scope_ref or ""))
    entity_type = _SCOPE_ENTITY_TYPE[report_scope_type]
    if entity_type is None:
        row = conn.execute("SELECT * FROM analytics_snapshots ORDER BY captured_at DESC, id DESC LIMIT 1").fetchone()
    else:
        row = conn.execute(
            """
            SELECT * FROM analytics_snapshots
            WHERE entity_type = ? AND entity_ref = ?
            ORDER BY captured_at DESC, id DESC LIMIT 1
            """,
            (entity_type, canonical_scope_ref),
        ).fetchone()
    return None if row is None else dict(row)


def _load_latest_by_scope(conn: Any, *, table: str, scope_type_col: str, scope_ref_col: str, report_scope_type: str, report_scope_ref: str | None) -> dict[str, Any] | None:
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=report_scope_type, scope_ref=str(report_scope_ref or ""))
    if report_scope_type == "OVERVIEW":
        row = conn.execute(f"SELECT * FROM {table} ORDER BY created_at DESC, id DESC LIMIT 1").fetchone()
    else:
        row = conn.execute(
            f"SELECT * FROM {table} WHERE {scope_type_col} = ? AND {scope_ref_col} = ? ORDER BY created_at DESC, id DESC LIMIT 1",
            (report_scope_type, canonical_scope_ref),
        ).fetchone()
    return None if row is None else dict(row)


def _build_report_dataset(conn: Any, *, report_scope_type: str, report_scope_ref: str | None, report_family: str, filter_payload: dict[str, Any]) -> dict[str, Any]:
    latest_snapshot = _load_latest_snapshot(conn, report_scope_type=report_scope_type, report_scope_ref=report_scope_ref)
    latest_kpi = _load_latest_by_scope(
        conn,
        table="analytics_operational_kpi_snapshots",
        scope_type_col="scope_type",
        scope_ref_col="scope_ref",
        report_scope_type=report_scope_type,
        report_scope_ref=report_scope_ref,
    )
    latest_comparison = _load_latest_by_scope(
        conn,
        table="analytics_comparison_snapshots",
        scope_type_col="scope_type",
        scope_ref_col="scope_ref",
        report_scope_type=report_scope_type,
        report_scope_ref=report_scope_ref,
    )
    latest_prediction = _load_latest_by_scope(
        conn,
        table="analytics_prediction_snapshots",
        scope_type_col="scope_type",
        scope_ref_col="scope_ref",
        report_scope_type=report_scope_type,
        report_scope_ref=report_scope_ref,
    )
    latest_recommendation = _load_latest_by_scope(
        conn,
        table="analytics_recommendation_snapshots",
        scope_type_col="recommendation_scope_type",
        scope_ref_col="recommendation_scope_ref",
        report_scope_type=report_scope_type,
        report_scope_ref=report_scope_ref,
    )
    required_sources = {
        "analytics_snapshots": latest_snapshot,
        "analytics_operational_kpi_snapshots": latest_kpi,
        "analytics_comparison_snapshots": latest_comparison,
        "analytics_prediction_snapshots": latest_prediction,
        "analytics_recommendation_snapshots": latest_recommendation,
    }
    missing = sorted([name for name, value in required_sources.items() if value is None])
    if missing:
        raise AnalyticsDomainError(
            code=E5A_INVALID_REPORT_SCOPE,
            message=f"missing required source data: {', '.join(missing)}",
        )
    return {
        "report_scope_type": report_scope_type,
        "report_scope_ref": report_scope_ref,
        "report_family": report_family,
        "filter_payload": dict(filter_payload),
        "latest_snapshot": latest_snapshot,
        "latest_operational_kpi": latest_kpi,
        "latest_comparison": latest_comparison,
        "latest_prediction": latest_prediction,
        "latest_recommendation": latest_recommendation,
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
        {"group": "SNAPSHOT", "key": "snapshot_id"},
        {"group": "KPI", "key": "kpi_code"},
        {"group": "PREDICTION", "key": "predicted_label"},
        {"group": "RECOMMENDATION", "key": "recommendation_issue_key"},
    ]
    row = {
        "report_scope_type": dataset["report_scope_type"],
        "report_scope_ref": dataset["report_scope_ref"],
        "snapshot_id": dataset["latest_snapshot"]["id"],
        "kpi_code": dataset["latest_operational_kpi"]["kpi_code"],
        "predicted_label": dataset["latest_prediction"]["predicted_label"],
        "recommendation_issue_key": dataset["latest_recommendation"]["issue_key"],
    }
    path = root / f"report_{record_id}.xlsx"
    content = export_report_to_xlsx_bytes({"columns": columns, "rows": [row]}, sheet_title=f"analytics_{dataset['report_scope_type'].lower()}")
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
        dataset = _build_report_dataset(
            conn,
            report_scope_type=report_scope_type,
            report_scope_ref=report_scope_ref,
            report_family=report_family,
            filter_payload=filter_payload,
        )
        artifact_ref = _generate_artifact(record_id=report_id, artifact_type=artifact_type, dataset=dataset)
        conn.execute(
            "UPDATE analytics_report_records SET artifact_ref = ?, generation_status = 'READY' WHERE id = ?",
            (artifact_ref, report_id),
        )
    except Exception:
        conn.execute(
            "UPDATE analytics_report_records SET generation_status = 'FAILED' WHERE id = ?",
            (report_id,),
        )
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
