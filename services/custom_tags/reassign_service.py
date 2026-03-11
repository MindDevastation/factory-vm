from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.custom_tags import auto_assign, rules_service


_EXECUTE_SCOPE_LOCK = threading.Lock()
_EXECUTE_SCOPES_IN_FLIGHT: set[str] = set()


@dataclass
class ReassignError(Exception):
    code: str
    message: str
    status_code: int
    details: dict[str, Any] | None = None


class InvalidInputError(ReassignError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(code="CTA_INVALID_INPUT", message=message, status_code=400, details=details)


class TagNotFoundError(ReassignError):
    def __init__(self, *, tag_code: str):
        super().__init__(
            code="CTA_TAG_NOT_FOUND",
            message="custom tag not found",
            status_code=404,
            details={"tag_code": tag_code},
        )


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(value: str) -> dict[str, Any]:
    parsed = json.loads(value)
    if isinstance(parsed, dict):
        return parsed
    return {}


def _resolve_tag_by_code(conn: sqlite3.Connection, tag_code: str) -> dict[str, Any]:
    code = tag_code.strip()
    if not code:
        raise InvalidInputError("tag_code must not be empty", {"field": "tag_code"})
    rows = conn.execute(
        "SELECT id, code, category, is_active FROM custom_tags WHERE code = ? ORDER BY id ASC",
        (code,),
    ).fetchall()
    if not rows:
        raise TagNotFoundError(tag_code=code)
    if len(rows) > 1:
        raise InvalidInputError("tag_code is ambiguous across categories", {"field": "tag_code", "tag_code": code})
    return dict(rows[0])


def _scope_key(channel_slug: str | None, tag_id: int | None) -> str:
    return f"channel={channel_slug or '*'}|tag_id={tag_id if tag_id is not None else '*'}"


def _select_analyzed_tracks(conn: sqlite3.Connection, *, channel_slug: str | None) -> list[dict[str, Any]]:
    where = ["t.analyzed_at IS NOT NULL"]
    args: list[Any] = []
    if channel_slug is not None:
        where.append("t.channel_slug = ?")
        args.append(channel_slug)
    where_sql = " AND ".join(where)
    return conn.execute(
        f"""
        SELECT t.id, t.channel_slug, f.payload_json AS features_json, g.payload_json AS tags_json, s.payload_json AS scores_json
        FROM tracks t
        JOIN track_features f ON f.track_pk = t.id
        JOIN track_tags g ON g.track_pk = t.id
        JOIN track_scores s ON s.track_pk = t.id
        WHERE {where_sql}
        ORDER BY t.id ASC
        """,
        tuple(args),
    ).fetchall()


def _load_active_tags(conn: sqlite3.Connection, *, channel_slug: str, tag_id: int | None = None) -> tuple[list[dict[str, Any]], dict[int, list[dict[str, Any]]]]:
    where = ["t.is_active = 1"]
    args: list[Any] = [channel_slug]
    if tag_id is not None:
        where.append("t.id = ?")
        args.append(tag_id)
    where_sql = " AND ".join(where)
    tag_rows = conn.execute(
        f"""
        SELECT t.id, t.category,
               EXISTS(
                   SELECT 1
                   FROM custom_tag_channel_bindings b
                   WHERE b.tag_id = t.id
                     AND b.channel_slug = ?
               ) AS is_channel_bound
        FROM custom_tags t
        WHERE {where_sql}
        ORDER BY t.id ASC
        """,
        tuple(args),
    ).fetchall()

    if not tag_rows:
        return [], {}

    tag_ids = [int(row["id"]) for row in tag_rows]
    placeholders = ",".join("?" for _ in tag_ids)
    rules_rows = conn.execute(
        f"""
        SELECT id, tag_id, source_path, operator, value_json, match_mode, required
        FROM custom_tag_rules
        WHERE is_active = 1 AND tag_id IN ({placeholders})
        ORDER BY priority DESC, id ASC
        """,
        tuple(tag_ids),
    ).fetchall()
    rules_by_tag: dict[int, list[dict[str, Any]]] = {tid: [] for tid in tag_ids}
    for row in rules_rows:
        rules_by_tag[int(row["tag_id"])].append(dict(row))
    return [dict(r) for r in tag_rows], rules_by_tag


def _compute_candidate_ids(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    analyzer_payload: dict[str, Any],
    tag_id: int | None,
) -> set[int]:
    tag_rows, rules_by_tag = _load_active_tags(conn, channel_slug=channel_slug, tag_id=tag_id)
    candidate_ids: set[int] = set()
    for tag_row in tag_rows:
        row_tag_id = int(tag_row["id"])
        if auto_assign._tag_is_candidate(tag_row, rules_by_tag.get(row_tag_id, []), analyzer_payload):
            candidate_ids.add(row_tag_id)
    return candidate_ids


def preview_rule_matches(
    conn: sqlite3.Connection,
    *,
    tag_code: str,
    rule: dict[str, Any],
    channel_slug: str | None,
) -> dict[str, Any]:
    tag = _resolve_tag_by_code(conn, tag_code)
    normalized_rule = rules_service._normalize_rule_payload(rule, tag_id=int(tag["id"]))
    normalized_rule["tag_id"] = int(tag["id"])
    if channel_slug is not None and not channel_slug.strip():
        raise InvalidInputError("scope.channel_slug must not be empty", {"field": "scope.channel_slug"})

    tracks = _select_analyzed_tracks(conn, channel_slug=channel_slug.strip() if channel_slug else None)
    matched: list[int] = []
    tag_row = {
        "id": int(tag["id"]),
        "category": str(tag["category"]),
        "is_channel_bound": False,
    }
    for row in tracks:
        tag_row["is_channel_bound"] = bool(
            conn.execute(
                "SELECT 1 FROM custom_tag_channel_bindings WHERE tag_id = ? AND channel_slug = ? LIMIT 1",
                (int(tag["id"]), str(row["channel_slug"])),
            ).fetchone()
        )
        analyzer_payload = {
            "track_features": {"payload_json": _load_json(str(row["features_json"]))},
            "track_tags": {"payload_json": _load_json(str(row["tags_json"]))},
            "track_scores": {"payload_json": _load_json(str(row["scores_json"]))},
        }
        if auto_assign._tag_is_candidate(tag_row, [normalized_rule], analyzer_payload):
            matched.append(int(row["id"]))

    count = len(matched)
    return {
        "matched_tracks_count": count,
        "sample_track_ids": matched[:3],
        "summary": f"{count} analyzed tracks would match",
    }


def _preview_or_execute(
    conn: sqlite3.Connection,
    *,
    channel_slug: str | None,
    tag_code: str | None,
    execute: bool,
) -> dict[str, Any]:
    normalized_channel = channel_slug.strip() if isinstance(channel_slug, str) and channel_slug.strip() else None
    resolved_tag_id: int | None = None
    if tag_code is not None:
        resolved_tag_id = int(_resolve_tag_by_code(conn, tag_code)["id"])

    rows = _select_analyzed_tracks(conn, channel_slug=normalized_channel)

    new_assignments = 0
    removed_assignments = 0
    unchanged_tracks = 0

    for row in rows:
        track_pk = int(row["id"])
        analyzer_payload = {
            "track_features": {"payload_json": _load_json(str(row["features_json"]))},
            "track_tags": {"payload_json": _load_json(str(row["tags_json"]))},
            "track_scores": {"payload_json": _load_json(str(row["scores_json"]))},
        }

        candidate_ids = _compute_candidate_ids(
            conn,
            channel_slug=str(row["channel_slug"]),
            analyzer_payload=analyzer_payload,
            tag_id=resolved_tag_id,
        )

        existing_rows = conn.execute(
            """
            SELECT id, tag_id, state
            FROM track_custom_tag_assignments
            WHERE track_pk = ?
            """,
            (track_pk,),
        ).fetchall()
        existing_by_tag = {int(er["tag_id"]): dict(er) for er in existing_rows}

        if resolved_tag_id is not None:
            scoped_tag_ids = {resolved_tag_id}
        else:
            scoped_tag_ids = {int(er["tag_id"]) for er in existing_rows if str(er["state"]) == "AUTO"} | set(candidate_ids)

        local_added = 0
        local_removed = 0

        for tid in sorted(candidate_ids):
            existing = existing_by_tag.get(tid)
            if existing is None:
                local_added += 1
                if execute:
                    now_text = _now_text()
                    conn.execute(
                        """
                        INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                        VALUES(?,?,?,?,?)
                        """,
                        (track_pk, tid, "AUTO", now_text, now_text),
                    )
                continue
            if str(existing["state"]) == "AUTO":
                continue

        for tid in sorted(scoped_tag_ids):
            if tid in candidate_ids:
                continue
            existing = existing_by_tag.get(tid)
            if existing is None:
                continue
            if str(existing["state"]) == "AUTO":
                local_removed += 1
                if execute:
                    conn.execute("DELETE FROM track_custom_tag_assignments WHERE id = ?", (int(existing["id"]),))

        new_assignments += local_added
        removed_assignments += local_removed
        if local_added == 0 and local_removed == 0:
            unchanged_tracks += 1

    return {
        "summary": {
            "new_assignments": new_assignments,
            "removed_assignments": removed_assignments,
            "unchanged_tracks": unchanged_tracks,
        }
    }


def preview_reassign(conn: sqlite3.Connection, *, channel_slug: str | None, tag_code: str | None = None) -> dict[str, Any]:
    return _preview_or_execute(conn, channel_slug=channel_slug, tag_code=tag_code, execute=False)


def execute_reassign(conn: sqlite3.Connection, *, channel_slug: str | None, tag_code: str | None = None) -> dict[str, Any]:
    resolved_tag_id = int(_resolve_tag_by_code(conn, tag_code)["id"]) if tag_code is not None else None
    scope_key = _scope_key(channel_slug.strip() if isinstance(channel_slug, str) and channel_slug.strip() else None, resolved_tag_id)
    with _EXECUTE_SCOPE_LOCK:
        if scope_key in _EXECUTE_SCOPES_IN_FLIGHT:
            return {
                "summary": {"new_assignments": 0, "removed_assignments": 0, "unchanged_tracks": 0},
                "noop": True,
            }
        _EXECUTE_SCOPES_IN_FLIGHT.add(scope_key)

    try:
        return _preview_or_execute(conn, channel_slug=channel_slug, tag_code=tag_code, execute=True)
    finally:
        with _EXECUTE_SCOPE_LOCK:
            _EXECUTE_SCOPES_IN_FLIGHT.discard(scope_key)
