from __future__ import annotations

import json
from typing import Any

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_INVALID_REPORT_ARTIFACT_TYPE,
    E5A_INVALID_REPORT_SCOPE,
)
from services.analytics_center.literals import (
    ANALYTICS_MF6_ARTIFACT_TYPES,
    ANALYTICS_MF6_GENERATION_STATUSES,
    ANALYTICS_MF6_REPORT_SCOPE_TYPES,
)
from services.common.db import now_ts


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
            f"artifact://{report_scope_type.lower()}/{int(now)}",
            "READY",
            now,
            created_by,
        ),
    )
    return int(row.lastrowid)


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
