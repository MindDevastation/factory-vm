from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.common import db as dbm

_ITEM_KEY_RE = re.compile(r"^[a-z0-9_-]+$")
_SLOT_CODE_RE = re.compile(r"^[a-z0-9_-]+$")


class MonthlyPlanningTemplateError(Exception):
    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


@dataclass(frozen=True)
class MonthlyPlanningTemplateListParams:
    channel_id: int | None = None
    status: str | None = None
    q: str | None = None
    limit: int = 50
    offset: int = 0


class MonthlyPlanningTemplateService:
    STATUS_ACTIVE = "ACTIVE"
    STATUS_ARCHIVED = "ARCHIVED"
    MAX_TEMPLATE_NAME = 120
    MAX_ITEMS = 200
    MAX_ITEM_KEY = 64
    MAX_SLOT_CODE = 64
    MAX_TITLE = 200
    MAX_CONTENT_TYPE = 64
    MAX_NOTES = 1000

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def create_template(
        self,
        *,
        channel_id: Any,
        template_name: Any,
        content_type: Any,
        items: Any,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        normalized_channel_id = self._normalize_channel_id(channel_id)
        self._require_channel_exists(normalized_channel_id)

        normalized_name = self._normalize_template_name(template_name)
        normalized_content_type = self._normalize_content_type(content_type)
        normalized_items = self._normalize_items(items)

        now_iso = self._now_iso()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self._conn.execute(
                """
                INSERT INTO monthly_planning_templates(
                    channel_id, template_name, content_type, status, usage_summary_json,
                    created_at, updated_at, archived_at, created_by, updated_by, archived_by
                ) VALUES (?, ?, ?, ?, NULL, ?, ?, NULL, ?, ?, NULL)
                """,
                (
                    normalized_channel_id,
                    normalized_name,
                    normalized_content_type,
                    self.STATUS_ACTIVE,
                    now_iso,
                    now_iso,
                    created_by,
                    created_by,
                ),
            )
            template_id = int(cur.lastrowid)
            self._insert_items(template_id=template_id, items=normalized_items, now_iso=now_iso)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        return self.get_template(template_id)

    def list_templates(self, params: MonthlyPlanningTemplateListParams) -> dict[str, Any]:
        where: list[str] = []
        values: list[Any] = []

        if params.channel_id is not None:
            where.append("t.channel_id = ?")
            values.append(int(params.channel_id))

        if params.status:
            status = str(params.status).strip().upper()
            if status not in {self.STATUS_ACTIVE, self.STATUS_ARCHIVED}:
                raise MonthlyPlanningTemplateError(
                    "MPT_INVALID_TEMPLATE_STATUS",
                    "status must be ACTIVE or ARCHIVED.",
                )
            where.append("t.status = ?")
            values.append(status)

        if params.q:
            where.append("LOWER(t.template_name) LIKE ?")
            values.append(f"%{str(params.q).strip().lower()}%")

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        limit = max(1, min(int(params.limit), 500))
        offset = max(0, int(params.offset))

        total = int(
            self._conn.execute(
                f"SELECT COUNT(1) AS c FROM monthly_planning_templates t {where_sql}",
                tuple(values),
            ).fetchone()["c"]
        )

        rows = self._conn.execute(
            f"""
            SELECT
                t.id,
                t.channel_id,
                t.template_name,
                t.content_type,
                t.status,
                t.created_at,
                t.updated_at,
                t.archived_at,
                COUNT(i.id) AS item_count
            FROM monthly_planning_templates t
            LEFT JOIN monthly_planning_template_items i ON i.template_id = t.id
            {where_sql}
            GROUP BY t.id
            ORDER BY t.updated_at DESC, t.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(values + [limit, offset]),
        ).fetchall()

        return {
            "items": [self._template_summary_row_to_payload(row) for row in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def get_template(self, template_id: Any) -> dict[str, Any]:
        template_row = self._get_template_row_or_raise(template_id)
        items = self._conn.execute(
            """
            SELECT id, template_id, item_key, slot_code, position, title, day_of_month, notes, created_at, updated_at
            FROM monthly_planning_template_items
            WHERE template_id = ?
            ORDER BY position ASC, id ASC
            """,
            (int(template_id),),
        ).fetchall()

        payload = self._template_detail_row_to_payload(template_row)
        payload["items"] = [
            {
                "id": int(item["id"]),
                "item_key": str(item["item_key"]),
                "slot_code": str(item["slot_code"]),
                "position": int(item["position"]),
                "title": str(item["title"]),
                "day_of_month": None if item["day_of_month"] is None else int(item["day_of_month"]),
                "notes": item["notes"],
                "created_at": str(item["created_at"]),
                "updated_at": str(item["updated_at"]),
            }
            for item in items
        ]
        return payload

    def update_template(
        self,
        template_id: Any,
        *,
        template_name: Any,
        content_type: Any,
        items: Any,
        updated_by: str | None = None,
    ) -> dict[str, Any]:
        row = self._get_template_row_or_raise(template_id)
        if str(row["status"]) != self.STATUS_ACTIVE:
            raise MonthlyPlanningTemplateError(
                "MPT_TEMPLATE_ARCHIVED",
                "Archived template cannot be modified.",
            )

        normalized_name = self._normalize_template_name(template_name)
        normalized_content_type = self._normalize_content_type(content_type)
        normalized_items = self._normalize_items(items)

        now_iso = self._now_iso()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                UPDATE monthly_planning_templates
                SET template_name = ?, content_type = ?, updated_at = ?, updated_by = ?
                WHERE id = ?
                """,
                (normalized_name, normalized_content_type, now_iso, updated_by, int(template_id)),
            )
            self._conn.execute(
                "DELETE FROM monthly_planning_template_items WHERE template_id = ?",
                (int(template_id),),
            )
            self._insert_items(template_id=int(template_id), items=normalized_items, now_iso=now_iso)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        return self.get_template(template_id)

    def archive_template(self, template_id: Any, *, archived_by: str | None = None) -> dict[str, Any]:
        row = self._get_template_row_or_raise(template_id)
        if str(row["status"]) == self.STATUS_ARCHIVED:
            payload = self._template_detail_row_to_payload(row)
            payload["items"] = self.get_template(template_id)["items"]
            return payload

        now_iso = self._now_iso()
        self._conn.execute(
            """
            UPDATE monthly_planning_templates
            SET status = ?, archived_at = ?, archived_by = ?, updated_at = ?, updated_by = ?
            WHERE id = ?
            """,
            (
                self.STATUS_ARCHIVED,
                now_iso,
                archived_by,
                now_iso,
                archived_by,
                int(template_id),
            ),
        )
        return self.get_template(template_id)

    def _normalize_channel_id(self, channel_id: Any) -> int:
        if isinstance(channel_id, bool) or channel_id is None:
            raise MonthlyPlanningTemplateError("MPT_INVALID_CHANNEL_ID", "channel_id is required.")
        try:
            out = int(channel_id)
        except Exception as exc:
            raise MonthlyPlanningTemplateError("MPT_INVALID_CHANNEL_ID", "channel_id is required.") from exc
        if out <= 0:
            raise MonthlyPlanningTemplateError("MPT_INVALID_CHANNEL_ID", "channel_id is required.")
        return out

    def _require_channel_exists(self, channel_id: int) -> None:
        channel = dbm.get_channel_by_id(self._conn, channel_id)
        if channel is None:
            raise MonthlyPlanningTemplateError("MPT_INVALID_CHANNEL_ID", "channel_id is invalid.")

    def _normalize_template_name(self, template_name: Any) -> str:
        name = str(template_name or "").strip()
        if not name or len(name) > self.MAX_TEMPLATE_NAME:
            raise MonthlyPlanningTemplateError(
                "MPT_INVALID_TEMPLATE_NAME",
                "Template name is required and must be valid length.",
            )
        return name

    def _normalize_content_type(self, content_type: Any) -> str | None:
        if content_type is None:
            return None
        if not isinstance(content_type, str):
            raise MonthlyPlanningTemplateError("MPT_INVALID_CONTENT_TYPE", "content_type is invalid for current planner model.")
        value = content_type.strip()
        if not value or len(value) > self.MAX_CONTENT_TYPE:
            raise MonthlyPlanningTemplateError("MPT_INVALID_CONTENT_TYPE", "content_type is invalid for current planner model.")
        return value

    def _normalize_items(self, items: Any) -> list[dict[str, Any]]:
        if not isinstance(items, list):
            raise MonthlyPlanningTemplateError("MPT_EMPTY_TEMPLATE", "Template must contain at least one item.")
        if len(items) == 0:
            raise MonthlyPlanningTemplateError("MPT_EMPTY_TEMPLATE", "Template must contain at least one item.")
        if len(items) > self.MAX_ITEMS:
            raise MonthlyPlanningTemplateError("MPT_TOO_MANY_ITEMS", "Template cannot contain more than 200 items.")

        normalized: list[dict[str, Any]] = []
        item_keys: set[str] = set()
        slot_codes: set[str] = set()
        positions: set[int] = set()

        for raw in items:
            if not isinstance(raw, dict):
                raise MonthlyPlanningTemplateError("MPT_INVALID_ITEM", "Template item payload must be an object.")

            item_key = str(raw.get("item_key") or "").strip()
            if not item_key or len(item_key) > self.MAX_ITEM_KEY or _ITEM_KEY_RE.fullmatch(item_key) is None:
                raise MonthlyPlanningTemplateError("MPT_INVALID_ITEM_KEY", "item_key is invalid.")
            if item_key in item_keys:
                raise MonthlyPlanningTemplateError("MPT_DUPLICATE_ITEM_KEY", "Template item_key values must be unique within template.")
            item_keys.add(item_key)

            slot_code = str(raw.get("slot_code") or "").strip()
            if not slot_code or len(slot_code) > self.MAX_SLOT_CODE or _SLOT_CODE_RE.fullmatch(slot_code) is None:
                raise MonthlyPlanningTemplateError("MPT_INVALID_SLOT_CODE", "slot_code is invalid.")
            if slot_code in slot_codes:
                raise MonthlyPlanningTemplateError("MPT_DUPLICATE_SLOT_CODE", "Template slot_code values must be unique within template.")
            slot_codes.add(slot_code)

            position_raw = raw.get("position")
            if isinstance(position_raw, bool) or not isinstance(position_raw, int) or position_raw < 1:
                raise MonthlyPlanningTemplateError("MPT_DUPLICATE_POSITION", "Template item positions must be unique within template.")
            if position_raw in positions:
                raise MonthlyPlanningTemplateError("MPT_DUPLICATE_POSITION", "Template item positions must be unique within template.")
            positions.add(position_raw)

            title = str(raw.get("title") or "").strip()
            if not title or len(title) > self.MAX_TITLE:
                raise MonthlyPlanningTemplateError("MPT_INVALID_ITEM_TITLE", "title is invalid.")

            day_of_month = raw.get("day_of_month")
            if day_of_month is not None:
                if isinstance(day_of_month, bool) or not isinstance(day_of_month, int) or day_of_month < 1 or day_of_month > 31:
                    raise MonthlyPlanningTemplateError("MPT_INVALID_ITEM_DAY", "day_of_month must be between 1 and 31.")

            notes = raw.get("notes")
            if notes is not None:
                if not isinstance(notes, str):
                    raise MonthlyPlanningTemplateError("MPT_INVALID_ITEM_NOTES", "notes must be a string.")
                if len(notes) > self.MAX_NOTES:
                    raise MonthlyPlanningTemplateError("MPT_INVALID_ITEM_NOTES", "notes must not exceed 1000 chars.")

            normalized.append(
                {
                    "item_key": item_key,
                    "slot_code": slot_code,
                    "position": int(position_raw),
                    "title": title,
                    "day_of_month": day_of_month,
                    "notes": notes,
                }
            )

        return normalized

    def _get_template_row_or_raise(self, template_id: Any) -> dict[str, Any]:
        try:
            normalized = int(template_id)
        except Exception as exc:
            raise MonthlyPlanningTemplateError("MPT_TEMPLATE_NOT_FOUND", "Monthly planning template was not found.") from exc
        row = self._conn.execute(
            """
            SELECT id, channel_id, template_name, content_type, status, usage_summary_json, created_at, updated_at, archived_at
            FROM monthly_planning_templates
            WHERE id = ?
            """,
            (normalized,),
        ).fetchone()
        if row is None:
            raise MonthlyPlanningTemplateError("MPT_TEMPLATE_NOT_FOUND", "Monthly planning template was not found.")
        return row

    def _insert_items(self, *, template_id: int, items: list[dict[str, Any]], now_iso: str) -> None:
        for item in items:
            self._conn.execute(
                """
                INSERT INTO monthly_planning_template_items(
                    template_id, item_key, slot_code, position, title, day_of_month, notes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    template_id,
                    item["item_key"],
                    item["slot_code"],
                    item["position"],
                    item["title"],
                    item["day_of_month"],
                    item["notes"],
                    now_iso,
                    now_iso,
                ),
            )

    def _template_summary_row_to_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "channel_id": int(row["channel_id"]),
            "template_name": str(row["template_name"]),
            "content_type": row["content_type"],
            "status": str(row["status"]),
            "item_count": int(row["item_count"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "archived_at": row["archived_at"],
            **self._usage_summary_payload(row.get("usage_summary_json")),
        }

    def _template_detail_row_to_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "channel_id": int(row["channel_id"]),
            "template_name": str(row["template_name"]),
            "content_type": row["content_type"],
            "status": str(row["status"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "archived_at": row["archived_at"],
            **self._usage_summary_payload(row.get("usage_summary_json")),
        }

    def _usage_summary_payload(self, usage_summary_json: Any) -> dict[str, Any]:
        apply_run_count = 0
        last_applied_target_month = None
        last_applied_at = None
        if isinstance(usage_summary_json, str) and usage_summary_json.strip():
            try:
                parsed = json.loads(usage_summary_json)
                if isinstance(parsed, dict):
                    apply_run_count = int(parsed.get("apply_run_count") or 0)
                    last_applied_target_month = parsed.get("last_applied_target_month")
                    last_applied_at = parsed.get("last_applied_at")
            except Exception:
                pass
        return {
            "apply_run_count": apply_run_count,
            "last_applied_target_month": last_applied_target_month,
            "last_applied_at": last_applied_at,
        }

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
