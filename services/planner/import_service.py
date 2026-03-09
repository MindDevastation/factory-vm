from __future__ import annotations

import csv
import io
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.planner.time_normalization import PublishAtValidationError, normalize_publish_at

MAX_IMPORT_ROWS = 5000
REQUIRED_FIELDS = ("channel_slug", "content_type", "title", "publish_at", "notes")


class PlannerImportError(Exception):
    """Base planner import error."""


class PlannerImportTooManyRowsError(PlannerImportError):
    """Raised when import contains too many rows."""


class PlannerImportParseError(PlannerImportError):
    """Raised when import file cannot be parsed."""


class PlannerImportPreviewNotConfirmableError(PlannerImportError):
    """Raised when preview flags do not allow selected confirm mode."""


class PlannerImportConfirmConflictError(PlannerImportError):
    """Raised when strict confirm detects conflicts."""


@dataclass
class PlannerImportRowResult:
    row_num: int
    normalized: dict[str, Any]
    conflict: bool
    existing_release_id: int | None
    errors: list[str]


class PlannerImportPreviewService:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def build_preview(self, *, filename: str, payload: bytes) -> dict[str, Any]:
        rows = self._parse_rows(filename=filename, payload=payload)

        known_conflict_keys: dict[tuple[str, str], int] = {}
        seen_keys: set[tuple[str, str]] = set()
        result_rows: list[PlannerImportRowResult] = []

        for idx, raw_row in enumerate(rows, start=1):
            normalized = {key: None for key in REQUIRED_FIELDS}
            errors: list[str] = []

            if not isinstance(raw_row, dict):
                errors.append("ROW_MUST_BE_OBJECT")
                result_rows.append(
                    PlannerImportRowResult(
                        row_num=idx,
                        normalized=normalized,
                        conflict=False,
                        existing_release_id=None,
                        errors=errors,
                    )
                )
                continue

            for field in REQUIRED_FIELDS:
                if field not in raw_row:
                    errors.append(f"MISSING_FIELD:{field}")
                    continue
                value = raw_row[field]
                if value is None:
                    normalized[field] = None
                    continue
                if not isinstance(value, str):
                    errors.append(f"INVALID_TYPE:{field}")
                    continue
                normalized[field] = value.strip()

            channel_slug = normalized["channel_slug"]
            content_type = normalized["content_type"]
            title = normalized["title"]
            publish_at = normalized["publish_at"]
            notes = normalized["notes"]

            if isinstance(channel_slug, str) and not channel_slug:
                errors.append("INVALID_VALUE:channel_slug")
            if isinstance(content_type, str) and not content_type:
                errors.append("INVALID_VALUE:content_type")

            if isinstance(title, str) and len(title) > 1000:
                errors.append("TITLE_TOO_LONG")
            if isinstance(notes, str) and len(notes) > 5000:
                errors.append("NOTES_TOO_LONG")

            if isinstance(channel_slug, str) and channel_slug and not self._channel_exists(channel_slug):
                errors.append("CHANNEL_NOT_FOUND")

            if isinstance(publish_at, str):
                if not publish_at:
                    normalized["publish_at"] = None
                    publish_at = None
                else:
                    try:
                        normalized["publish_at"] = normalize_publish_at(publish_at)
                        publish_at = normalized["publish_at"]
                    except PublishAtValidationError:
                        errors.append("INVALID_PUBLISH_AT")

            conflict = False
            existing_release_id: int | None = None
            if isinstance(channel_slug, str) and channel_slug and isinstance(publish_at, str) and publish_at:
                key = (channel_slug, publish_at)
                if key in seen_keys:
                    errors.append("DUPLICATE_IN_FILE")
                seen_keys.add(key)

                if key not in known_conflict_keys:
                    known_conflict_keys[key] = self._find_existing_release_id(channel_slug, publish_at)
                existing_release_id = known_conflict_keys[key]
                conflict = existing_release_id is not None

            result_rows.append(
                PlannerImportRowResult(
                    row_num=idx,
                    normalized=normalized,
                    conflict=conflict,
                    existing_release_id=existing_release_id,
                    errors=errors,
                )
            )

        error_rows = sum(1 for row in result_rows if row.errors)
        conflict_rows = sum(1 for row in result_rows if row.conflict)

        return {
            "summary": {
                "total_rows": len(result_rows),
                "error_rows": error_rows,
                "conflict_rows": conflict_rows,
            },
            "can_confirm_strict": error_rows == 0 and conflict_rows == 0,
            "can_confirm_replace": error_rows == 0,
            "rows": [
                {
                    "row_num": row.row_num,
                    "normalized": row.normalized,
                    "conflict": row.conflict,
                    "existing_release_id": row.existing_release_id,
                    "errors": row.errors,
                }
                for row in result_rows
            ],
        }

    def confirm_preview(self, *, preview: dict[str, Any], mode: str) -> dict[str, Any]:
        rows = [row for row in preview.get("rows", []) if not row.get("errors")]

        if mode == "strict":
            if not bool(preview.get("can_confirm_strict")):
                raise PlannerImportPreviewNotConfirmableError("strict mode is not confirmable")
            return self._confirm_strict(rows)
        if mode == "replace":
            if not bool(preview.get("can_confirm_replace")):
                raise PlannerImportPreviewNotConfirmableError("replace mode is not confirmable")
            return self._confirm_replace(rows)
        raise ValueError("mode must be strict or replace")

    def _confirm_strict(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        inserted = 0
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                normalized = row.get("normalized") or {}
                channel_slug = normalized.get("channel_slug")
                publish_at = normalized.get("publish_at")
                if isinstance(channel_slug, str) and channel_slug and isinstance(publish_at, str) and publish_at:
                    if self._find_existing_release_id(channel_slug, publish_at) is not None:
                        raise PlannerImportConfirmConflictError("conflict")
                self._insert_non_conflict_row(normalized)
                inserted += 1
            self._conn.execute("COMMIT")
            return {"inserted": inserted, "updated": 0}
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _confirm_replace(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        inserted = 0
        updated = 0
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                normalized = row.get("normalized") or {}
                channel_slug = normalized.get("channel_slug")
                publish_at = normalized.get("publish_at")
                if isinstance(channel_slug, str) and channel_slug and isinstance(publish_at, str) and publish_at:
                    cur = self._conn.execute(
                        """
                        UPDATE planned_releases
                        SET content_type = ?, title = ?, notes = ?, updated_at = ?
                        WHERE channel_slug = ? AND publish_at = ?
                        """,
                        (
                            normalized.get("content_type"),
                            normalized.get("title"),
                            normalized.get("notes"),
                            self._now_iso(),
                            channel_slug,
                            publish_at,
                        ),
                    )
                    if cur.rowcount == 1:
                        updated += 1
                        continue

                self._insert_non_conflict_row(normalized)
                inserted += 1
            self._conn.execute("COMMIT")
            return {"inserted": inserted, "updated": updated}
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _insert_non_conflict_row(self, normalized: dict[str, Any]) -> None:
        now_iso = self._now_iso()
        self._conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'PLANNED', ?, ?)
            """,
            (
                normalized.get("channel_slug"),
                normalized.get("content_type"),
                normalized.get("title"),
                normalized.get("publish_at"),
                normalized.get("notes"),
                now_iso,
                now_iso,
            ),
        )

    def _parse_rows(self, *, filename: str, payload: bytes) -> list[dict[str, Any]]:
        file_name_lower = (filename or "").strip().lower()
        text = self._decode_payload(payload)

        try:
            if file_name_lower.endswith(".csv"):
                rows = self._parse_csv(text)
            elif file_name_lower.endswith(".json"):
                rows = self._parse_json(text)
            else:
                raise PlannerImportParseError("unsupported file format")
        except PlannerImportParseError:
            raise
        except Exception as exc:
            raise PlannerImportParseError("failed to parse import file") from exc

        if len(rows) > MAX_IMPORT_ROWS:
            raise PlannerImportTooManyRowsError("too many rows")
        return rows

    @staticmethod
    def _decode_payload(payload: bytes) -> str:
        try:
            return payload.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise PlannerImportParseError("import file must be UTF-8") from exc

    def _parse_csv(self, text: str) -> list[dict[str, Any]]:
        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            raise PlannerImportParseError("csv header is required")

        normalized_headers: list[str | None] = [header.strip() if isinstance(header, str) else header for header in reader.fieldnames]
        if len(set(normalized_headers)) != len(normalized_headers):
            # Deterministic parse failure when multiple source headers normalize to the same key.
            raise PlannerImportParseError("csv header contains duplicate normalized fields")

        normalized_header_set = set(normalized_headers)
        if not set(REQUIRED_FIELDS).issubset(normalized_header_set):
            raise PlannerImportParseError("csv header missing required fields")

        rows: list[dict[str, Any]] = []
        for row in reader:
            row_norm = {(key.strip() if isinstance(key, str) else key): value for key, value in row.items()}
            mapped: dict[str, Any] = {}
            for field in REQUIRED_FIELDS:
                value = row_norm.get(field)
                mapped[field] = None if value is None else value
            rows.append(mapped)
        return rows

    @staticmethod
    def _parse_json(text: str) -> list[dict[str, Any]]:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise PlannerImportParseError("invalid json") from exc

        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            rows = payload["rows"]
        else:
            raise PlannerImportParseError("json must be an array of rows")

        return rows

    def _channel_exists(self, channel_slug: str) -> bool:
        row = self._conn.execute("SELECT 1 FROM channels WHERE slug = ? LIMIT 1", (channel_slug,)).fetchone()
        return bool(row)

    def _find_existing_release_id(self, channel_slug: str, publish_at: str) -> int | None:
        row = self._conn.execute(
            "SELECT id FROM planned_releases WHERE channel_slug = ? AND publish_at = ? LIMIT 1",
            (channel_slug, publish_at),
        ).fetchone()
        if not row:
            return None
        return int(row["id"])

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
