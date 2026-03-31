from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from services.analytics_center.helpers import normalized_scope_identity
from services.analytics_center.literals import (
    ANALYTICS_ENTITY_TYPES,
    ANALYTICS_SOURCE_FAMILIES,
    ANALYTICS_WINDOW_TYPES,
)


@dataclass(frozen=True)
class SnapshotReadFilters:
    entity_type: str | None = None
    entity_ref: str | None = None
    source_family: str | None = None
    window_type: str | None = None
    current_only: bool = False
    captured_from: float | None = None
    captured_to: float | None = None


def normalize_read_filters(filters: SnapshotReadFilters) -> SnapshotReadFilters:
    def norm(value: str | None, allowed: tuple[str, ...]) -> str | None:
        if value is None:
            return None
        candidate = value.strip().upper()
        if candidate not in allowed:
            raise ValueError(f"invalid filter literal: {candidate}")
        return candidate

    return SnapshotReadFilters(
        entity_type=norm(filters.entity_type, ANALYTICS_ENTITY_TYPES),
        entity_ref=None if filters.entity_ref is None else filters.entity_ref.strip(),
        source_family=norm(filters.source_family, ANALYTICS_SOURCE_FAMILIES),
        window_type=norm(filters.window_type, ANALYTICS_WINDOW_TYPES),
        current_only=bool(filters.current_only),
        captured_from=filters.captured_from,
        captured_to=filters.captured_to,
    )


def read_snapshots(conn: sqlite3.Connection, filters: SnapshotReadFilters) -> list[dict[str, Any]]:
    f = normalize_read_filters(filters)
    where: list[str] = []
    params: list[Any] = []
    if f.entity_type:
        where.append("entity_type = ?")
        params.append(f.entity_type)
    if f.entity_ref:
        where.append("entity_ref = ?")
        params.append(f.entity_ref)
    if f.source_family:
        where.append("source_family = ?")
        params.append(f.source_family)
    if f.window_type:
        where.append("window_type = ?")
        params.append(f.window_type)
    if f.current_only:
        where.append("is_current = 1")
    if f.captured_from is not None:
        where.append("captured_at >= ?")
        params.append(f.captured_from)
    if f.captured_to is not None:
        where.append("captured_at <= ?")
        params.append(f.captured_to)

    query = "SELECT * FROM analytics_snapshots"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY captured_at DESC, id DESC"
    return [dict(row) for row in conn.execute(query, tuple(params)).fetchall()]


def read_linkage_for_scope(conn: sqlite3.Connection, *, entity_type: str, entity_ref: str) -> dict[str, Any] | None:
    et = entity_type.strip().upper()
    row = conn.execute(
        "SELECT * FROM analytics_scope_links WHERE entity_type = ? AND entity_ref = ?",
        (et, entity_ref),
    ).fetchone()
    if row is None:
        return None
    result = dict(row)
    result["rollup_links"] = [
        dict(r)
        for r in conn.execute(
            """
            SELECT rl.*
            FROM analytics_rollup_links rl
            JOIN analytics_snapshots s ON s.id = rl.parent_snapshot_id OR s.id = rl.child_snapshot_id
            WHERE s.entity_type = ? AND s.entity_ref = ?
            ORDER BY rl.id ASC
            """,
            (et, entity_ref),
        ).fetchall()
    ]
    return result


def resolve_current_snapshot(
    conn: sqlite3.Connection,
    *,
    entity_type: str,
    entity_ref: str,
    source_family: str,
    window_type: str,
) -> dict[str, Any] | None:
    scope_key = normalized_scope_identity(
        entity_type=entity_type,
        entity_ref=entity_ref,
        source_family=source_family,
        window_type=window_type,
    )
    row = conn.execute(
        """
        SELECT *
        FROM analytics_snapshots
        WHERE normalized_scope_key = ? AND is_current = 1
        ORDER BY captured_at DESC, id DESC
        LIMIT 1
        """,
        (scope_key,),
    ).fetchone()
    return None if row is None else dict(row)
