from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


class PlannedReleaseError(Exception):
    """Base error for planned release service."""


class PlannedReleaseNotFoundError(PlannedReleaseError):
    """Raised when a planned release row cannot be found."""


class PlannedReleaseLockedError(PlannedReleaseError):
    """Raised when mutation is attempted on a non-PLANNED release."""


@dataclass(frozen=True)
class PlannedReleaseListParams:
    channel_slug: str | None = None
    content_type: str | None = None
    status: str | None = None
    search: str | None = None
    sort_by: str = "created_at"
    sort_dir: str = "desc"
    limit: int = 50
    offset: int = 0


class PlannedReleaseService:
    SORT_ALLOWLIST = {
        "id",
        "channel_slug",
        "content_type",
        "title",
        "publish_at",
        "status",
        "created_at",
        "updated_at",
    }

    EDITABLE_FIELDS = {
        "channel_slug",
        "content_type",
        "title",
        "publish_at",
        "notes",
    }

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def list(self, params: PlannedReleaseListParams) -> dict[str, Any]:
        sort_by = params.sort_by if params.sort_by in self.SORT_ALLOWLIST else "created_at"
        sort_dir = "ASC" if params.sort_dir.lower() == "asc" else "DESC"
        limit = max(1, min(int(params.limit), 500))
        offset = max(0, int(params.offset))

        where_clauses: list[str] = []
        values: list[Any] = []

        if params.channel_slug:
            where_clauses.append("channel_slug = ?")
            values.append(params.channel_slug)
        if params.content_type:
            where_clauses.append("content_type = ?")
            values.append(params.content_type)
        if params.status:
            where_clauses.append("status = ?")
            values.append(params.status)
        if params.search:
            where_clauses.append("LOWER(COALESCE(title, '')) LIKE ?")
            values.append(f"%{params.search.lower()}%")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        total = int(
            self._conn.execute(
                f"SELECT COUNT(1) AS c FROM planned_releases {where_sql}", tuple(values)
            ).fetchone()["c"]
        )

        rows = self._conn.execute(
            f"""
            SELECT id, channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at
            FROM planned_releases
            {where_sql}
            ORDER BY {sort_by} {sort_dir}, id ASC
            LIMIT ? OFFSET ?
            """,
            tuple(values + [limit, offset]),
        ).fetchall()

        return {
            "items": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def get_by_id(self, release_id: int) -> dict[str, Any]:
        row = self._conn.execute(
            """
            SELECT id, channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at
            FROM planned_releases
            WHERE id = ?
            """,
            (release_id,),
        ).fetchone()
        if row is None:
            raise PlannedReleaseNotFoundError(f"planned release {release_id} not found")
        return row

    def create(
        self,
        *,
        channel_slug: str,
        content_type: str,
        title: str | None,
        publish_at: str | None,
        notes: str | None,
    ) -> dict[str, Any]:
        now_iso = self._now_iso()
        cur = self._conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, 'PLANNED', ?, ?)
            """,
            (channel_slug, content_type, title, publish_at, notes, now_iso, now_iso),
        )
        return self.get_by_id(int(cur.lastrowid))

    def update(self, release_id: int, updates: dict[str, Any]) -> dict[str, Any]:
        if not updates:
            return self.get_by_id(release_id)

        set_parts: list[str] = []
        values: list[Any] = []
        for key, value in updates.items():
            if key not in self.EDITABLE_FIELDS:
                continue
            set_parts.append(f"{key} = ?")
            values.append(value)

        if not set_parts:
            return self.get_by_id(release_id)

        set_parts.append("updated_at = ?")
        values.append(self._now_iso())
        values.append(release_id)

        cur = self._conn.execute(
            f"UPDATE planned_releases SET {', '.join(set_parts)} WHERE id = ? AND status = 'PLANNED'",
            tuple(values),
        )

        if cur.rowcount == 0:
            row = self._conn.execute(
                "SELECT status FROM planned_releases WHERE id = ?",
                (release_id,),
            ).fetchone()
            if row is None:
                raise PlannedReleaseNotFoundError(f"planned release {release_id} not found")
            if str(row["status"]) != "PLANNED":
                raise PlannedReleaseLockedError(f"planned release {release_id} is locked")

        return self.get_by_id(release_id)

    def delete(self, release_id: int) -> None:
        cur = self._conn.execute(
            "DELETE FROM planned_releases WHERE id = ? AND status = 'PLANNED'",
            (release_id,),
        )
        if cur.rowcount == 1:
            return

        row = self._conn.execute(
            "SELECT status FROM planned_releases WHERE id = ?",
            (release_id,),
        ).fetchone()
        if row is None:
            raise PlannedReleaseNotFoundError(f"planned release {release_id} not found")
        if str(row["status"]) != "PLANNED":
            raise PlannedReleaseLockedError(f"planned release {release_id} is locked")

        raise RuntimeError(
            f"planned release {release_id} delete affected 0 rows while status remained PLANNED"
        )

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
