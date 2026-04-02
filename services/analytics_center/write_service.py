from __future__ import annotations

import sqlite3
import logging
from dataclasses import dataclass
from typing import Any

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_CURRENT_SNAPSHOT_CONFLICT,
    E5A_EXTERNAL_IDENTITY_CONFLICT,
    E5A_INVALID_BASELINE_REFERENCE,
    E5A_INVALID_ENTITY_TYPE,
    E5A_INVALID_FRESHNESS_STATUS,
    E5A_INVALID_SCOPE_LINK,
    E5A_INVALID_SNAPSHOT_STATUS,
    E5A_INVALID_SOURCE_FAMILY,
    E5A_INVALID_WINDOW_TYPE,
    E5A_LINKAGE_CONFLICT,
)
from services.analytics_center.helpers import (
    normalized_scope_identity,
    supersede_existing_current_snapshot,
    validate_json_payload,
)
from services.analytics_center.literals import (
    ANALYTICS_ENTITY_TYPES,
    ANALYTICS_FRESHNESS_STATUSES,
    ANALYTICS_ROLLUP_RELATION_TYPES,
    ANALYTICS_SNAPSHOT_STATUSES,
    ANALYTICS_SOURCE_FAMILIES,
    ANALYTICS_WINDOW_TYPES,
)
from services.common.db import now_ts

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnapshotWriteInput:
    entity_type: str
    entity_ref: str
    source_family: str
    window_type: str
    snapshot_status: str
    freshness_status: str
    payload_json: Any
    explainability_json: Any
    lineage_json: Any
    anomaly_markers_json: Any
    captured_at: float
    is_current: bool = False
    comparison_baseline_snapshot_id: int | None = None
    window_start_ts: float | None = None
    window_end_ts: float | None = None


def _require_literal(value: str, allowed: tuple[str, ...], code: str, label: str) -> str:
    normalized = value.strip().upper()
    if normalized not in allowed:
        raise AnalyticsDomainError(code=code, message=f"invalid {label}: {value}")
    return normalized


def _validate_anchor(conn: sqlite3.Connection, *, entity_type: str, entity_ref: str) -> None:
    try:
        ref_id = int(entity_ref)
    except (TypeError, ValueError):
        ref_id = -1

    queries = {
        "CHANNEL": "SELECT 1 FROM channels WHERE id=? LIMIT 1",
        "RELEASE": "SELECT 1 FROM releases WHERE id=? LIMIT 1",
        "JOB_RUNTIME": "SELECT 1 FROM jobs WHERE id=? LIMIT 1",
    }
    if entity_type in queries:
        if ref_id < 1 or conn.execute(queries[entity_type], (ref_id,)).fetchone() is None:
            raise AnalyticsDomainError(code=E5A_INVALID_SCOPE_LINK, message="missing canonical anchor")
        return
    if entity_type == "BATCH" and not str(entity_ref).strip():
        raise AnalyticsDomainError(code=E5A_INVALID_SCOPE_LINK, message="batch entity_ref is required")
    if entity_type == "PORTFOLIO" and not str(entity_ref).strip():
        raise AnalyticsDomainError(code=E5A_INVALID_SCOPE_LINK, message="portfolio entity_ref is required")


def write_scope_link(
    conn: sqlite3.Connection,
    *,
    entity_type: str,
    entity_ref: str,
    channel_id: int | None,
    release_id: int | None,
    job_id: int | None,
    batch_ref: str | None,
    portfolio_ref: str | None,
    payload_json: Any,
) -> int:
    et = _require_literal(entity_type, ANALYTICS_ENTITY_TYPES, E5A_INVALID_ENTITY_TYPE, "entity_type")
    _validate_anchor(conn, entity_type=et, entity_ref=entity_ref)
    scope_key = normalized_scope_identity(entity_type=et, entity_ref=entity_ref, source_family="INTERNAL_OPERATIONAL", window_type="LAST_KNOWN_CURRENT")
    now = now_ts()
    payload = validate_json_payload(payload_json, field_name="payload_json")
    existed = conn.execute(
        "SELECT id FROM analytics_scope_links WHERE entity_type = ? AND entity_ref = ?",
        (et, str(entity_ref)),
    ).fetchone()
    try:
        row = conn.execute(
            """
            INSERT INTO analytics_scope_links(
                entity_type, entity_ref, channel_id, release_id, job_id, batch_ref, portfolio_ref,
                normalized_scope_key, payload_json, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (et, str(entity_ref), channel_id, release_id, job_id, batch_ref, portfolio_ref, scope_key, payload, now, now),
        )
    except sqlite3.IntegrityError as exc:
        raise AnalyticsDomainError(code=E5A_LINKAGE_CONFLICT, message="scope link conflict") from exc
    _record_event(
        conn,
        event_type="LINKAGE_UPDATED" if existed else "LINKAGE_CREATED",
        entity_type=et,
        entity_ref=str(entity_ref),
        payload_json=payload,
    )
    return int(row.lastrowid)


def write_external_identity(
    conn: sqlite3.Connection,
    *,
    entity_type: str,
    entity_ref: str,
    source_family: str,
    external_namespace: str,
    external_id: str,
    payload_json: Any,
) -> int:
    et = _require_literal(entity_type, ANALYTICS_ENTITY_TYPES, E5A_INVALID_ENTITY_TYPE, "entity_type")
    sf = _require_literal(source_family, ANALYTICS_SOURCE_FAMILIES, E5A_INVALID_SOURCE_FAMILY, "source_family")
    _validate_anchor(conn, entity_type=et, entity_ref=entity_ref)
    now = now_ts()
    payload = validate_json_payload(payload_json, field_name="payload_json")
    try:
        row = conn.execute(
            """
            INSERT INTO analytics_external_identities(
                entity_type, entity_ref, source_family, external_namespace, external_id,
                payload_json, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (et, str(entity_ref), sf, external_namespace, external_id, payload, now, now),
        )
    except sqlite3.IntegrityError as exc:
        raise AnalyticsDomainError(code=E5A_EXTERNAL_IDENTITY_CONFLICT, message="external identity conflict") from exc
    return int(row.lastrowid)


def write_snapshot(conn: sqlite3.Connection, payload: SnapshotWriteInput) -> int:
    et = _require_literal(payload.entity_type, ANALYTICS_ENTITY_TYPES, E5A_INVALID_ENTITY_TYPE, "entity_type")
    sf = _require_literal(payload.source_family, ANALYTICS_SOURCE_FAMILIES, E5A_INVALID_SOURCE_FAMILY, "source_family")
    wt = _require_literal(payload.window_type, ANALYTICS_WINDOW_TYPES, E5A_INVALID_WINDOW_TYPE, "window_type")
    ss = _require_literal(payload.snapshot_status, ANALYTICS_SNAPSHOT_STATUSES, E5A_INVALID_SNAPSHOT_STATUS, "snapshot_status")
    fs = _require_literal(payload.freshness_status, ANALYTICS_FRESHNESS_STATUSES, E5A_INVALID_FRESHNESS_STATUS, "freshness_status")
    _validate_anchor(conn, entity_type=et, entity_ref=payload.entity_ref)

    if payload.comparison_baseline_snapshot_id is not None:
        baseline = conn.execute(
            "SELECT id FROM analytics_snapshots WHERE id = ?",
            (int(payload.comparison_baseline_snapshot_id),),
        ).fetchone()
        if baseline is None:
            raise AnalyticsDomainError(code=E5A_INVALID_BASELINE_REFERENCE, message="baseline snapshot not found")

    normalized_scope_key = normalized_scope_identity(entity_type=et, entity_ref=payload.entity_ref, source_family=sf, window_type=wt)
    snapshot_payload = validate_json_payload(payload.payload_json, field_name="payload_json")
    explainability_json = validate_json_payload(payload.explainability_json, field_name="explainability_json")
    lineage_json = validate_json_payload(payload.lineage_json, field_name="lineage_json")
    anomaly_markers_json = validate_json_payload(payload.anomaly_markers_json, field_name="anomaly_markers_json")

    now = now_ts()
    try:
        conn.execute("BEGIN")
        if payload.is_current:
            current_before = conn.execute(
                "SELECT id FROM analytics_snapshots WHERE normalized_scope_key = ? AND is_current = 1 LIMIT 1",
                (normalized_scope_key,),
            ).fetchone()
            supersede_existing_current_snapshot(conn, normalized_scope_key=normalized_scope_key, superseded_at=now)
        row = conn.execute(
            """
            INSERT INTO analytics_snapshots(
                entity_type, entity_ref, normalized_scope_key, source_family, window_type,
                window_start_ts, window_end_ts, snapshot_status, freshness_status,
                is_current, payload_json, explainability_json, lineage_json, anomaly_markers_json,
                comparison_baseline_snapshot_id, captured_at, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                et,
                str(payload.entity_ref),
                normalized_scope_key,
                sf,
                wt,
                payload.window_start_ts,
                payload.window_end_ts,
                ss,
                fs,
                1 if payload.is_current else 0,
                snapshot_payload,
                explainability_json,
                lineage_json,
                anomaly_markers_json,
                payload.comparison_baseline_snapshot_id,
                payload.captured_at,
                now,
                now,
            ),
        )
        snapshot_id = int(row.lastrowid)
        _record_event(
            conn,
            event_type="SNAPSHOT_CREATED",
            entity_type=et,
            entity_ref=str(payload.entity_ref),
            source_family=sf,
            window_type=wt,
            snapshot_status=ss,
            freshness_status=fs,
            snapshot_id=snapshot_id,
            comparison_baseline_snapshot_id=payload.comparison_baseline_snapshot_id,
            payload_json=snapshot_payload,
        )
        if payload.is_current:
            _record_event(
                conn,
                event_type="SNAPSHOT_MARKED_CURRENT",
                entity_type=et,
                entity_ref=str(payload.entity_ref),
                source_family=sf,
                window_type=wt,
                snapshot_status=ss,
                freshness_status=fs,
                snapshot_id=snapshot_id,
                comparison_baseline_snapshot_id=payload.comparison_baseline_snapshot_id,
                payload_json=snapshot_payload,
            )
            if current_before is not None:
                _record_event(
                    conn,
                    event_type="SNAPSHOT_SUPERSEDED",
                    entity_type=et,
                    entity_ref=str(payload.entity_ref),
                    source_family=sf,
                    window_type=wt,
                    snapshot_status="SUPERSEDED",
                    freshness_status=fs,
                    snapshot_id=int(current_before["id"]),
                    payload_json='{"reason":"new_current_snapshot"}',
                )
        if payload.comparison_baseline_snapshot_id is not None:
            _record_event(
                conn,
                event_type="BASELINE_REFERENCE_ATTACHED",
                entity_type=et,
                entity_ref=str(payload.entity_ref),
                source_family=sf,
                window_type=wt,
                snapshot_status=ss,
                freshness_status=fs,
                snapshot_id=snapshot_id,
                comparison_baseline_snapshot_id=payload.comparison_baseline_snapshot_id,
                payload_json=snapshot_payload,
            )
        _record_event(
            conn,
            event_type="LINEAGE_PAYLOAD_PERSISTED",
            entity_type=et,
            entity_ref=str(payload.entity_ref),
            source_family=sf,
            window_type=wt,
            snapshot_status=ss,
            freshness_status=fs,
            snapshot_id=snapshot_id,
            payload_json=lineage_json,
        )
        if ss == "PARTIAL":
            _record_event(
                conn,
                event_type="PARTIAL_SNAPSHOT_STORED",
                entity_type=et,
                entity_ref=str(payload.entity_ref),
                source_family=sf,
                window_type=wt,
                snapshot_status=ss,
                freshness_status=fs,
                snapshot_id=snapshot_id,
                payload_json=snapshot_payload,
            )
        if ss == "FAILED":
            _record_event(
                conn,
                event_type="FAILED_SNAPSHOT_STORED",
                entity_type=et,
                entity_ref=str(payload.entity_ref),
                source_family=sf,
                window_type=wt,
                snapshot_status=ss,
                freshness_status=fs,
                snapshot_id=snapshot_id,
                payload_json=snapshot_payload,
            )
        conn.execute("COMMIT")
    except sqlite3.IntegrityError as exc:
        conn.execute("ROLLBACK")
        raise AnalyticsDomainError(code=E5A_CURRENT_SNAPSHOT_CONFLICT, message="current snapshot conflict") from exc
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return int(row.lastrowid)


def write_rollup_link(
    conn: sqlite3.Connection,
    *,
    parent_snapshot_id: int,
    child_snapshot_id: int,
    relation_type: str,
    payload_json: Any,
) -> int:
    rt = _require_literal(relation_type, ANALYTICS_ROLLUP_RELATION_TYPES, E5A_INVALID_SCOPE_LINK, "relation_type")
    payload = validate_json_payload(payload_json, field_name="payload_json")
    now = now_ts()
    try:
        row = conn.execute(
            """
            INSERT INTO analytics_rollup_links(
                parent_snapshot_id, child_snapshot_id, relation_type, payload_json, created_at, updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (int(parent_snapshot_id), int(child_snapshot_id), rt, payload, now, now),
        )
    except sqlite3.IntegrityError as exc:
        raise AnalyticsDomainError(code=E5A_LINKAGE_CONFLICT, message="rollup linkage conflict") from exc
    _record_event(
        conn,
        event_type="LINKAGE_CREATED",
        entity_type="PORTFOLIO",
        entity_ref=f"{parent_snapshot_id}:{child_snapshot_id}",
        payload_json=payload,
    )
    return int(row.lastrowid)


def _record_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    entity_type: str,
    entity_ref: str,
    payload_json: str,
    source_family: str | None = None,
    window_type: str | None = None,
    snapshot_status: str | None = None,
    freshness_status: str | None = None,
    snapshot_id: int | None = None,
    comparison_baseline_snapshot_id: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO analytics_events(
            event_type, entity_type, entity_ref, source_family, window_type,
            snapshot_status, freshness_status, snapshot_id, comparison_baseline_snapshot_id, payload_json, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            event_type,
            entity_type,
            entity_ref,
            source_family,
            window_type,
            snapshot_status,
            freshness_status,
            snapshot_id,
            comparison_baseline_snapshot_id,
            payload_json,
            now_ts(),
        ),
    )
    logger.info(
        "analytics_event=%s entity_type=%s entity_ref=%s source_family=%s window_type=%s snapshot_status=%s freshness_status=%s snapshot_id=%s comparison_baseline_snapshot_id=%s",
        event_type,
        entity_type,
        entity_ref,
        source_family,
        window_type,
        snapshot_status,
        freshness_status,
        snapshot_id,
        comparison_baseline_snapshot_id,
    )
