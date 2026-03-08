from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


VALID_STATES = {"AUTO", "MANUAL", "SUPPRESSED"}
_VALID_CATEGORIES = ("VISUAL", "MOOD", "THEME")


@dataclass
class AssignmentError(Exception):
    code: str
    message: str
    status_code: int
    details: dict[str, Any] | None = None


class TrackNotFoundError(AssignmentError):
    def __init__(self, track_pk: int):
        super().__init__(
            code="CTA_TRACK_NOT_FOUND",
            message="track not found",
            status_code=404,
            details={"track_pk": track_pk},
        )


class TagNotFoundError(AssignmentError):
    def __init__(self, *, tag_id: int | None = None, tag_code: str | None = None, category: str | None = None):
        details: dict[str, Any] = {}
        if tag_id is not None:
            details["tag_id"] = tag_id
        if tag_code is not None:
            details["tag_code"] = tag_code
        if category is not None:
            details["category"] = category
        super().__init__(
            code="CTA_TAG_NOT_FOUND",
            message="custom tag not found",
            status_code=404,
            details=details,
        )


class InvalidInputError(AssignmentError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(code="CTA_INVALID_INPUT", message=message, status_code=400, details=details)


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_track(conn: sqlite3.Connection, track_pk: int) -> None:
    row = conn.execute("SELECT id FROM tracks WHERE id = ?", (track_pk,)).fetchone()
    if row is None:
        raise TrackNotFoundError(track_pk)


def _resolve_tag(conn: sqlite3.Connection, *, tag_id: int | None, tag_code: str | None, category: str | None) -> dict[str, Any]:
    if tag_id is not None:
        row = conn.execute(
            "SELECT id, code, label, category, is_active FROM custom_tags WHERE id = ?",
            (tag_id,),
        ).fetchone()
        if row is None:
            raise TagNotFoundError(tag_id=tag_id)
        return row

    assert tag_code is not None
    assert category is not None
    normalized_category = category.strip().upper()
    if normalized_category not in _VALID_CATEGORIES:
        raise InvalidInputError("category must be one of VISUAL, MOOD, THEME", {"field": "category"})
    normalized_code = tag_code.strip()
    if not normalized_code:
        raise InvalidInputError("tag_code must not be empty", {"field": "tag_code"})

    row = conn.execute(
        "SELECT id, code, label, category, is_active FROM custom_tags WHERE category = ? AND code = ?",
        (normalized_category, normalized_code),
    ).fetchone()
    if row is None:
        raise TagNotFoundError(tag_code=normalized_code, category=normalized_category)
    return row


def _build_effective_tags(assignments: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {"VISUAL": [], "MOOD": [], "THEME": []}
    for row in assignments:
        state = str(row["state"])
        if state not in {"AUTO", "MANUAL"}:
            continue
        category = str(row["category"])
        output[category].append(
            {
                "id": int(row["tag_id"]),
                "code": str(row["code"]),
                "label": str(row["label"]),
                "source": "manual" if state == "MANUAL" else "auto",
            }
        )
    return output


def get_track_custom_tags(conn: sqlite3.Connection, *, track_pk: int) -> dict[str, Any]:
    _require_track(conn, track_pk)

    rows = conn.execute(
        """
        SELECT a.tag_id, t.code, t.label, t.category, a.state
        FROM track_custom_tag_assignments a
        JOIN custom_tags t ON t.id = a.tag_id
        WHERE a.track_pk = ?
        ORDER BY t.category ASC, t.code ASC, t.id ASC
        """,
        (track_pk,),
    ).fetchall()

    assignments = [
        {
            "tag_id": int(row["tag_id"]),
            "code": str(row["code"]),
            "label": str(row["label"]),
            "category": str(row["category"]),
            "state": str(row["state"]),
        }
        for row in rows
    ]

    return {
        "track_pk": str(track_pk),
        "effective_tags": _build_effective_tags(assignments),
        "assignments": assignments,
    }


def upsert_manual_assignment(
    conn: sqlite3.Connection,
    *,
    track_pk: int,
    tag_id: int | None,
    tag_code: str | None,
    category: str | None,
) -> dict[str, Any]:
    _require_track(conn, track_pk)
    tag = _resolve_tag(conn, tag_id=tag_id, tag_code=tag_code, category=category)

    existing = conn.execute(
        "SELECT id, state, assigned_at FROM track_custom_tag_assignments WHERE track_pk = ? AND tag_id = ?",
        (track_pk, int(tag["id"])),
    ).fetchone()

    now_text = _now_text()
    if existing is None:
        if not bool(tag["is_active"]):
            raise InvalidInputError("cannot manually assign inactive tag", {"tag_id": int(tag["id"])})
        conn.execute(
            """
            INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
            VALUES(?,?,?,?,?)
            """,
            (track_pk, int(tag["id"]), "MANUAL", now_text, now_text),
        )
    elif str(existing["state"]) != "MANUAL":
        conn.execute(
            "UPDATE track_custom_tag_assignments SET state = ?, updated_at = ? WHERE id = ?",
            ("MANUAL", now_text, int(existing["id"])),
        )

    return {
        "track_pk": str(track_pk),
        "tag_id": int(tag["id"]),
        "state": "MANUAL",
    }


def suppress_assignment(conn: sqlite3.Connection, *, track_pk: int, tag_id: int) -> dict[str, Any]:
    _require_track(conn, track_pk)
    _resolve_tag(conn, tag_id=tag_id, tag_code=None, category=None)

    existing = conn.execute(
        "SELECT id, state FROM track_custom_tag_assignments WHERE track_pk = ? AND tag_id = ?",
        (track_pk, tag_id),
    ).fetchone()

    now_text = _now_text()
    if existing is None:
        conn.execute(
            """
            INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
            VALUES(?,?,?,?,?)
            """,
            (track_pk, tag_id, "SUPPRESSED", now_text, now_text),
        )
    elif str(existing["state"]) != "SUPPRESSED":
        conn.execute(
            "UPDATE track_custom_tag_assignments SET state = ?, updated_at = ? WHERE id = ?",
            ("SUPPRESSED", now_text, int(existing["id"])),
        )

    return {
        "track_pk": str(track_pk),
        "tag_id": tag_id,
        "state": "SUPPRESSED",
    }
