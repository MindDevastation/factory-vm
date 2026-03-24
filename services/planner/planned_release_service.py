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


class PlannedReleaseConflictError(PlannedReleaseError):
    """Raised when strict bulk mode detects one or more conflicts."""


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

    def list_candidate_ids(self, params: PlannedReleaseListParams) -> list[dict[str, Any]]:
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
        rows = self._conn.execute(
            f"""
            SELECT id, created_at
            FROM planned_releases
            {where_sql}
            ORDER BY created_at DESC, id ASC
            """,
            tuple(values),
        ).fetchall()
        return [{"id": int(row["id"]), "created_at": str(row["created_at"])} for row in rows]

    def list_by_ids(self, release_ids: list[int]) -> list[dict[str, Any]]:
        if not release_ids:
            return []
        unique_ids = list(dict.fromkeys(int(item) for item in release_ids))
        placeholders = ",".join("?" for _ in unique_ids)
        rows = self._conn.execute(
            f"""
            SELECT id, channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at
            FROM planned_releases
            WHERE id IN ({placeholders})
            """,
            tuple(unique_ids),
        ).fetchall()
        by_id = {int(row["id"]): row for row in rows}
        return [by_id[item] for item in release_ids if item in by_id]

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

    def bulk_delete(self, release_ids: list[int]) -> int:
        unique_ids = list(dict.fromkeys(release_ids))
        if not unique_ids:
            return 0

        placeholders = ",".join("?" for _ in unique_ids)
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self._conn.execute(
                f"SELECT id, status FROM planned_releases WHERE id IN ({placeholders})",
                tuple(unique_ids),
            ).fetchall()

            found_ids = {int(row["id"]) for row in rows}
            missing_ids = [release_id for release_id in unique_ids if release_id not in found_ids]
            if missing_ids:
                raise PlannedReleaseNotFoundError(f"planned release {missing_ids[0]} not found")

            locked_ids = [int(row["id"]) for row in rows if str(row["status"]) != "PLANNED"]
            if locked_ids:
                raise PlannedReleaseLockedError(f"planned release {locked_ids[0]} is locked")

            cur = self._conn.execute(
                f"DELETE FROM planned_releases WHERE id IN ({placeholders})",
                tuple(unique_ids),
            )
            if cur.rowcount != len(unique_ids):
                raise RuntimeError("planned release bulk delete affected unexpected rowcount")
            self._conn.commit()
            return len(unique_ids)
        except Exception:
            self._conn.rollback()
            raise

    def bulk_create_or_replace(
        self,
        *,
        channel_slug: str,
        content_type: str,
        title: str | None,
        notes: str | None,
        publish_ats: list[str | None],
        mode: str = "strict",
    ) -> dict[str, Any]:
        now_iso = self._now_iso()
        seen_keys: set[str] = set()
        for publish_at in publish_ats:
            if publish_at is None:
                continue
            key = f"{channel_slug}\x1f{publish_at}"
            if key in seen_keys:
                if mode == "strict":
                    raise PlannedReleaseConflictError("duplicate publish_at in request")
                continue
            seen_keys.add(key)

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            if mode == "strict":
                for publish_at in publish_ats:
                    if publish_at is None:
                        continue
                    existing = self._conn.execute(
                        """
                        SELECT id
                        FROM planned_releases
                        WHERE channel_slug = ? AND publish_at = ?
                        LIMIT 1
                        """,
                        (channel_slug, publish_at),
                    ).fetchone()
                    if existing is not None:
                        raise PlannedReleaseConflictError("bulk strict mode conflict")

            created_ids: list[int] = []
            updated_ids: list[int] = []

            if mode == "replace":
                existing_by_publish_at: dict[str, int] = {}
                touched_existing: set[int] = set()
                for publish_at in publish_ats:
                    if publish_at is not None and publish_at not in existing_by_publish_at:
                        existing = self._conn.execute(
                            """
                            SELECT id
                            FROM planned_releases
                            WHERE channel_slug = ? AND publish_at = ?
                            LIMIT 1
                            """,
                            (channel_slug, publish_at),
                        ).fetchone()
                        if existing is not None:
                            existing_by_publish_at[publish_at] = int(existing["id"])

                    if publish_at is not None and publish_at in existing_by_publish_at:
                        row_id = existing_by_publish_at[publish_at]
                        if row_id in touched_existing:
                            continue
                        touched_existing.add(row_id)
                        self._conn.execute(
                            """
                            UPDATE planned_releases
                            SET content_type = ?, title = ?, notes = ?, updated_at = ?
                            WHERE id = ?
                            """,
                            (content_type, title, notes, now_iso, row_id),
                        )
                        updated_ids.append(row_id)
                    else:
                        cur = self._conn.execute(
                            """
                            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                            VALUES(?, ?, ?, ?, ?, 'PLANNED', ?, ?)
                            """,
                            (channel_slug, content_type, title, publish_at, notes, now_iso, now_iso),
                        )
                        created_ids.append(int(cur.lastrowid))
            else:
                for publish_at in publish_ats:
                    cur = self._conn.execute(
                        """
                        INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                        VALUES(?, ?, ?, ?, ?, 'PLANNED', ?, ?)
                        """,
                        (channel_slug, content_type, title, publish_at, notes, now_iso, now_iso),
                    )
                    created_ids.append(int(cur.lastrowid))

            self._conn.execute("COMMIT")
            return {
                "created_count": len(created_ids),
                "updated_count": len(updated_ids),
                "affected_ids": created_ids + updated_ids,
            }
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
