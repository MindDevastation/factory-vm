from __future__ import annotations

import json
import re
import sqlite3
import calendar
import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from services.common import db as dbm

_ITEM_KEY_RE = re.compile(r"^[a-z0-9_-]+$")
_SLOT_CODE_RE = re.compile(r"^[a-z0-9_-]+$")
_TARGET_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


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

    def preview_apply(self, template_id: Any, *, channel_id: Any, target_month: Any) -> dict[str, Any]:
        template_row = self._get_template_row_or_raise(template_id)
        if str(template_row["status"]) != self.STATUS_ACTIVE:
            raise MonthlyPlanningTemplateError("MPT_TEMPLATE_ARCHIVED", "Archived template cannot be previewed or applied.")

        normalized_channel_id = self._normalize_channel_id(channel_id)
        self._require_channel_exists(normalized_channel_id)
        template_channel_id = int(template_row["channel_id"])
        if normalized_channel_id != template_channel_id:
            raise MonthlyPlanningTemplateError(
                "MPT_SCOPE_MISMATCH",
                "Template channel scope does not match target planning context.",
            )

        normalized_target_month = self._normalize_target_month(target_month)
        channel = dbm.get_channel_by_id(self._conn, normalized_channel_id)
        channel_slug = str(channel["slug"])
        template_content_type = template_row.get("content_type")

        items = self._conn.execute(
            """
            SELECT id, item_key, slot_code, position, title, day_of_month, notes
            FROM monthly_planning_template_items
            WHERE template_id = ?
            ORDER BY position ASC, id ASC
            """,
            (int(template_row["id"]),),
        ).fetchall()

        existing_rows = self._conn.execute(
            """
            SELECT id, content_type, publish_at, planning_slot_code, source_template_id, source_template_item_key, source_template_target_month
            FROM planned_releases
            WHERE channel_slug = ? AND publish_at IS NOT NULL AND substr(publish_at, 1, 7) = ?
            """,
            (channel_slug, normalized_target_month),
        ).fetchall()

        provenance_rows = self._conn.execute(
            """
            SELECT source_template_item_key
            FROM planned_releases
            WHERE source_template_id = ? AND source_template_target_month = ? AND source_template_item_key IS NOT NULL
            """,
            (int(template_row["id"]), normalized_target_month),
        ).fetchall()

        existing_by_slot: dict[str, list[dict[str, Any]]] = {}
        existing_by_provenance_item_key = {str(row["source_template_item_key"]) for row in provenance_rows}
        existing_for_overlap: list[dict[str, Any]] = []
        for row in existing_rows:
            rec = dict(row)
            slot = rec.get("planning_slot_code")
            if isinstance(slot, str) and slot.strip():
                existing_by_slot.setdefault(slot.strip(), []).append(rec)

            existing_for_overlap.append(rec)

        out_items: list[dict[str, Any]] = []
        summary = {
            "total_items": len(items),
            "would_create": 0,
            "blocked_duplicates": 0,
            "blocked_invalid_dates": 0,
            "overlap_warnings": 0,
        }

        for row in items:
            item_key = str(row["item_key"])
            slot_code = str(row["slot_code"])
            position = int(row["position"])
            day_of_month = row.get("day_of_month")
            reasons: list[dict[str, str]] = []
            overlap_warnings: list[dict[str, Any]] = []
            planned_date: str | None = None
            outcome = "WOULD_CREATE"

            if day_of_month is None:
                outcome = "BLOCKED_INVALID_DATE"
                reasons.append(
                    {
                        "code": "MPT_INVALID_ITEM_DAY_FOR_MONTH",
                        "message": "Item day_of_month exceeds target month length.",
                    }
                )
            else:
                resolved = self._resolve_planned_date(target_month=normalized_target_month, day_of_month=int(day_of_month))
                if resolved is None:
                    outcome = "BLOCKED_INVALID_DATE"
                    reasons.append(
                        {
                            "code": "MPT_INVALID_ITEM_DAY_FOR_MONTH",
                            "message": "Item day_of_month exceeds target month length.",
                        }
                    )
                else:
                    planned_date = resolved
                    has_duplicate = False
                    if slot_code in existing_by_slot:
                        has_duplicate = True
                        reasons.append(
                            {
                                "code": "MPT_DUPLICATE_PLANNING_SLOT",
                                "message": "Planned release with same slot identity already exists in target context.",
                            }
                        )
                    if item_key in existing_by_provenance_item_key:
                        has_duplicate = True
                        reasons.append(
                            {
                                "code": "MPT_DUPLICATE_PLANNING_SLOT",
                                "message": "Planned release with same template provenance already exists in target context.",
                            }
                        )
                    if has_duplicate:
                        outcome = "BLOCKED_DUPLICATE"
                    else:
                        overlap_warnings = self._build_overlap_warnings(
                            existing_rows=existing_for_overlap,
                            planned_date=planned_date,
                            item_content_type=template_content_type,
                        )

            if outcome == "WOULD_CREATE":
                summary["would_create"] += 1
            elif outcome == "BLOCKED_DUPLICATE":
                summary["blocked_duplicates"] += 1
            elif outcome == "BLOCKED_INVALID_DATE":
                summary["blocked_invalid_dates"] += 1
            summary["overlap_warnings"] += len(overlap_warnings)

            out_items.append(
                {
                    "item_key": item_key,
                    "slot_code": slot_code,
                    "position": position,
                    "title": str(row["title"]),
                    "day_of_month": day_of_month,
                    "notes": row.get("notes"),
                    "planned_date": planned_date,
                    "outcome": outcome,
                    "reasons": reasons,
                    "overlap_warnings": overlap_warnings,
                }
            )

        result = {
            "template_id": int(template_row["id"]),
            "channel_id": normalized_channel_id,
            "target_month": normalized_target_month,
            "summary": summary,
            "items": out_items,
        }
        result["preview_fingerprint"] = self._build_preview_fingerprint(
            template_id=int(template_row["id"]),
            template_updated_at=str(template_row["updated_at"]),
            channel_id=normalized_channel_id,
            target_month=normalized_target_month,
            items=out_items,
            summary=summary,
        )
        return result

    def execute_apply(
        self,
        template_id: Any,
        *,
        channel_id: Any,
        target_month: Any,
        preview_fingerprint: Any,
        request_id: str,
    ) -> dict[str, Any]:
        normalized_preview_fingerprint = str(preview_fingerprint or "").strip()
        normalized_request_id = str(request_id or "").strip() or "unknown"
        if not normalized_preview_fingerprint:
            raise MonthlyPlanningTemplateError(
                "MPT_PREVIEW_FINGERPRINT_REQUIRED",
                "preview_fingerprint is required for apply.",
            )

        preview = self.preview_apply(template_id, channel_id=channel_id, target_month=target_month)
        if normalized_preview_fingerprint != str(preview["preview_fingerprint"]):
            raise MonthlyPlanningTemplateError(
                "MPT_PREVIEW_STALE",
                "Preview is stale; rerun preview before apply.",
            )

        template_row = self._get_template_row_or_raise(template_id)
        if str(template_row["status"]) != self.STATUS_ACTIVE:
            raise MonthlyPlanningTemplateError("MPT_TEMPLATE_ARCHIVED", "Archived template cannot be previewed or applied.")

        now_iso = self._now_iso()
        apply_run_id: int | None = None
        summary = {
            "total_items": len(preview["items"]),
            "created": 0,
            "blocked_duplicates": 0,
            "blocked_invalid_dates": 0,
            "failed": 0,
            "overlap_warnings": int((preview.get("summary") or {}).get("overlap_warnings") or 0),
        }
        results: list[dict[str, Any]] = []
        channel = dbm.get_channel_by_id(self._conn, int(preview["channel_id"]))
        channel_slug = str(channel["slug"])
        template_content_type = template_row.get("content_type")

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            apply_run_id = int(
                self._conn.execute(
                    """
                    INSERT INTO monthly_planning_template_apply_runs(
                        template_id, channel_id, target_month, preview_fingerprint, started_at, completed_at,
                        status, request_id, created_count, blocked_duplicate_count, blocked_invalid_date_count, failed_count
                    ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 0, 0, 0, 0)
                    """,
                    (
                        int(preview["template_id"]),
                        int(preview["channel_id"]),
                        str(preview["target_month"]),
                        normalized_preview_fingerprint,
                        now_iso,
                        "STARTED",
                        normalized_request_id,
                    ),
                ).lastrowid
            )

            for item in sorted(preview["items"], key=lambda rec: int(rec["position"])):
                item_key = str(item["item_key"])
                slot_code = str(item["slot_code"])
                position = int(item["position"])
                reasons = list(item.get("reasons") or [])
                outcome = str(item["outcome"])
                planned_release_id: int | None = None
                reason_code: str | None = None
                reason_message: str | None = None
                if reasons:
                    reason_code = str(reasons[0].get("code")) if reasons[0].get("code") is not None else None
                    reason_message = str(reasons[0].get("message")) if reasons[0].get("message") is not None else None

                if outcome == "BLOCKED_INVALID_DATE":
                    outcome = "BLOCKED_INVALID_DATE"
                    summary["blocked_invalid_dates"] += 1
                elif outcome == "BLOCKED_DUPLICATE":
                    outcome = "BLOCKED_DUPLICATE"
                    summary["blocked_duplicates"] += 1
                elif outcome == "WOULD_CREATE":
                    try:
                        planned_release_id = int(
                            self._conn.execute(
                                """
                                INSERT INTO planned_releases(
                                    channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at,
                                    planning_slot_code, source_template_id, source_template_item_key, source_template_target_month, source_template_apply_run_id
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    channel_slug,
                                    template_content_type,
                                    str(item["title"]),
                                    item["planned_date"],
                                    item.get("notes"),
                                    "PLANNED",
                                    now_iso,
                                    now_iso,
                                    slot_code,
                                    int(preview["template_id"]),
                                    item_key,
                                    str(preview["target_month"]),
                                    apply_run_id,
                                ),
                            ).lastrowid
                        )
                        outcome = "CREATED"
                        summary["created"] += 1
                    except sqlite3.IntegrityError:
                        outcome = "BLOCKED_DUPLICATE"
                        summary["blocked_duplicates"] += 1
                        reason_code = "MPT_DUPLICATE_PLANNING_SLOT"
                        reason_message = "Planned release with same slot identity already exists in target context."
                    except Exception:
                        outcome = "FAILED_INTERNAL"
                        summary["failed"] += 1
                        reason_code = "MPT_APPLY_FAILED"
                        reason_message = "Apply failed due to internal error."
                else:
                    outcome = "FAILED_INTERNAL"
                    summary["failed"] += 1
                    reason_code = "MPT_APPLY_FAILED"
                    reason_message = "Apply failed due to internal error."

                self._conn.execute(
                    """
                    INSERT INTO monthly_planning_template_apply_run_items(
                        apply_run_id, template_item_key, slot_code, position, outcome, planned_release_id, reason_code, reason_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        apply_run_id,
                        item_key,
                        slot_code,
                        position,
                        outcome,
                        planned_release_id,
                        reason_code,
                        reason_message,
                    ),
                )
                result_reasons = []
                if reason_code or reason_message:
                    result_reasons.append({"code": reason_code, "message": reason_message})
                results.append(
                    {
                        "item_key": item_key,
                        "slot_code": slot_code,
                        "outcome": outcome,
                        "planned_release_id": planned_release_id,
                        "reasons": result_reasons,
                    }
                )

            completed_at = self._now_iso()
            self._conn.execute(
                """
                UPDATE monthly_planning_template_apply_runs
                SET completed_at = ?, status = ?, created_count = ?, blocked_duplicate_count = ?, blocked_invalid_date_count = ?, failed_count = ?
                WHERE id = ?
                """,
                (
                    completed_at,
                    "COMPLETED",
                    int(summary["created"]),
                    int(summary["blocked_duplicates"]),
                    int(summary["blocked_invalid_dates"]),
                    int(summary["failed"]),
                    apply_run_id,
                ),
            )
            self._update_usage_summary_from_apply_runs(template_id=int(preview["template_id"]))
            self._conn.commit()
        except MonthlyPlanningTemplateError:
            self._conn.rollback()
            raise
        except Exception as exc:
            self._conn.rollback()
            raise MonthlyPlanningTemplateError("MPT_APPLY_FAILED", "Apply failed due to internal error.") from exc

        return {
            "apply_run_id": int(apply_run_id),
            "template_id": int(preview["template_id"]),
            "channel_id": int(preview["channel_id"]),
            "target_month": str(preview["target_month"]),
            "summary": summary,
            "items": results,
        }

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
                raise MonthlyPlanningTemplateError("MPT_INVALID_ITEM_POSITION", "position must be an integer >= 1.")
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

    def _normalize_target_month(self, target_month: Any) -> str:
        value = str(target_month or "").strip()
        if _TARGET_MONTH_RE.fullmatch(value) is None:
            raise MonthlyPlanningTemplateError("MPT_INVALID_TARGET_MONTH", "target_month must be a valid YYYY-MM month.")
        try:
            datetime.strptime(value, "%Y-%m")
        except ValueError as exc:
            raise MonthlyPlanningTemplateError("MPT_INVALID_TARGET_MONTH", "target_month must be a valid YYYY-MM month.") from exc
        return value

    def _resolve_planned_date(self, *, target_month: str, day_of_month: int) -> str | None:
        year = int(target_month[0:4])
        month = int(target_month[5:7])
        last_day = calendar.monthrange(year, month)[1]
        if day_of_month < 1 or day_of_month > last_day:
            return None
        return date(year, month, day_of_month).isoformat()

    def _build_overlap_warnings(self, *, existing_rows: list[dict[str, Any]], planned_date: str, item_content_type: Any) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []
        day = planned_date[8:10]
        for row in existing_rows:
            publish_at = row.get("publish_at")
            if not isinstance(publish_at, str) or len(publish_at) < 10:
                continue
            if publish_at[8:10] != day:
                continue
            existing_content_type = row.get("content_type")
            if isinstance(item_content_type, str) and item_content_type.strip() and isinstance(existing_content_type, str):
                if item_content_type.strip() != existing_content_type.strip():
                    continue
            warnings.append(
                {
                    "code": "MPT_SOFT_OVERLAP",
                    "message": "Existing planned release matches the same planning day/content surface.",
                    "planned_release_id": int(row["id"]),
                }
            )
        return warnings

    def _build_preview_fingerprint(
        self,
        *,
        template_id: int,
        template_updated_at: str,
        channel_id: int,
        target_month: str,
        items: list[dict[str, Any]],
        summary: dict[str, int],
    ) -> str:
        canonical = {
            "template_id": template_id,
            "template_updated_at": template_updated_at,
            "channel_id": channel_id,
            "target_month": target_month,
            "summary": summary,
            "items": items,
        }
        encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

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

    def _update_usage_summary_from_apply_runs(self, *, template_id: int) -> None:
        row = self._conn.execute(
            """
            SELECT
                COUNT(1) AS apply_run_count,
                MAX(completed_at) AS last_applied_at
            FROM monthly_planning_template_apply_runs
            WHERE template_id = ? AND status = 'COMPLETED'
            """,
            (template_id,),
        ).fetchone()
        apply_run_count = int(row["apply_run_count"]) if row and row["apply_run_count"] is not None else 0
        last_applied_at = row["last_applied_at"] if row else None
        last_month_row = self._conn.execute(
            """
            SELECT target_month
            FROM monthly_planning_template_apply_runs
            WHERE template_id = ? AND status = 'COMPLETED'
            ORDER BY completed_at DESC, id DESC
            LIMIT 1
            """,
            (template_id,),
        ).fetchone()
        last_applied_target_month = last_month_row["target_month"] if last_month_row else None
        usage_summary_json = json.dumps(
            {
                "apply_run_count": apply_run_count,
                "last_applied_target_month": last_applied_target_month,
                "last_applied_at": last_applied_at,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        self._conn.execute(
            """
            UPDATE monthly_planning_templates
            SET usage_summary_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (usage_summary_json, self._now_iso(), template_id),
        )

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
