from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class BulkBindingsError(Exception):
    code: str
    message: str
    status_code: int
    details: dict[str, Any] | None = None


class InvalidInputError(BulkBindingsError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(code="CTA_INVALID_INPUT", message=message, status_code=400, details=details)


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_nonempty(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise InvalidInputError(f"{field} must be a string", {"field": field})
    out = value.strip()
    if not out:
        raise InvalidInputError(f"{field} must not be empty", {"field": field})
    return out


def _normalize_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise InvalidInputError(f"{field} must be a boolean", {"field": field})
    return value


def _normalize_item(item: dict[str, Any], *, index: int) -> dict[str, Any]:
    return {
        "tag_code": _normalize_nonempty(item.get("tag_code"), f"items[{index}].tag_code"),
        "channel_slug": _normalize_nonempty(item.get("channel_slug"), f"items[{index}].channel_slug"),
        "is_active": _normalize_bool(item.get("is_active"), f"items[{index}].is_active"),
    }


def preview_bulk_bindings(conn: sqlite3.Connection, items: list[dict[str, Any]]) -> dict[str, Any]:
    draft_results: list[dict[str, Any]] = []
    counts = {"CREATE": 0, "UPDATE": 0, "NOOP": 0, "INVALID": 0}

    for idx, raw in enumerate(items):
        normalized: dict[str, Any] | None = None
        resolved: dict[str, Any] = {"tag_id": None, "channel_slug": None}
        errors: list[dict[str, Any]] = []
        action = "INVALID"
        summary = "invalid item"
        try:
            normalized = _normalize_item(raw, index=idx)
            resolved["channel_slug"] = normalized["channel_slug"]

            tag_row = conn.execute(
                "SELECT id, category FROM custom_tags WHERE code = ? ORDER BY id ASC LIMIT 1",
                (normalized["tag_code"],),
            ).fetchone()
            if tag_row is None:
                errors.append({"code": "CTU_TAG_NOT_FOUND", "message": "custom tag not found for tag_code", "details": {"tag_code": normalized["tag_code"]}})
            else:
                resolved["tag_id"] = int(tag_row["id"])
                resolved["tag_category"] = str(tag_row["category"])
                if resolved["tag_category"] != "VISUAL":
                    errors.append(
                        {
                            "code": "CTU_BINDING_NOT_ALLOWED_FOR_CATEGORY",
                            "message": "bindings are only allowed for VISUAL tags",
                            "details": {"tag_code": normalized["tag_code"], "category": resolved["tag_category"]},
                        }
                    )

            if conn.execute("SELECT 1 FROM channels WHERE slug = ? LIMIT 1", (normalized["channel_slug"],)).fetchone() is None:
                errors.append({"code": "CTA_CHANNEL_NOT_FOUND", "message": "channel not found", "details": {"channel_slug": normalized["channel_slug"]}})

            if errors:
                action = "INVALID"
                summary = "; ".join(err["message"] for err in errors)
            else:
                binding = conn.execute(
                    "SELECT id FROM custom_tag_channel_bindings WHERE tag_id = ? AND channel_slug = ? LIMIT 1",
                    (resolved["tag_id"], normalized["channel_slug"]),
                ).fetchone()
                has_binding = binding is not None
                if (not has_binding) and normalized["is_active"]:
                    action = "CREATE"
                    summary = "will create binding"
                elif has_binding and (not normalized["is_active"]):
                    resolved["binding_id"] = int(binding["id"])
                    action = "UPDATE"
                    summary = "will remove binding"
                else:
                    if has_binding:
                        resolved["binding_id"] = int(binding["id"])
                    action = "NOOP"
                    summary = "no change required"

        except BulkBindingsError as err:
            errors.append({"code": err.code, "message": err.message, "details": err.details or {}})
            action = "INVALID"
            summary = err.message

        draft_results.append(
            {
                "index": idx,
                "normalized": normalized,
                "resolved": resolved,
                "action": action,
                "summary": summary,
                "errors": errors,
            }
        )

    key_to_indices: dict[tuple[int, str], list[int]] = {}
    for item in draft_results:
        resolved = item["resolved"]
        tag_id = resolved.get("tag_id")
        channel_slug = resolved.get("channel_slug")
        if isinstance(tag_id, int) and isinstance(channel_slug, str):
            key = (tag_id, channel_slug)
            key_to_indices.setdefault(key, []).append(int(item["index"]))

    duplicate_indices: set[int] = set()
    for indices in key_to_indices.values():
        if len(indices) > 1:
            duplicate_indices.update(indices)

    results: list[dict[str, Any]] = []
    for item in draft_results:
        if int(item["index"]) in duplicate_indices:
            normalized = item["normalized"]
            details: dict[str, Any] = {}
            if isinstance(normalized, dict):
                details = {
                    "tag_code": normalized.get("tag_code"),
                    "channel_slug": normalized.get("channel_slug"),
                }
            item["errors"].append(
                {
                    "code": "CTA_DUPLICATE_NATURAL_KEY",
                    "message": "duplicate binding item for natural key (tag_id, channel_slug)",
                    "details": details,
                }
            )
            item["action"] = "INVALID"
            item["summary"] = "duplicate natural key in payload"

        counts[item["action"]] += 1
        results.append(item)

    return {
        "can_confirm": counts["INVALID"] == 0,
        "summary": {
            "total": len(items),
            "create": counts["CREATE"],
            "update": counts["UPDATE"],
            "noop": counts["NOOP"],
            "invalid": counts["INVALID"],
        },
        "items": results,
    }


def confirm_bulk_bindings(conn: sqlite3.Connection, items: list[dict[str, Any]]) -> dict[str, Any]:
    preview = preview_bulk_bindings(conn, items)
    if not preview["can_confirm"]:
        return {"ok": False, "summary": preview["summary"], "results": [], "errors": ["preview contains invalid items"]}

    now_text = _now_text()
    created = 0
    updated = 0
    noop = 0
    results: list[dict[str, Any]] = []

    conn.execute("BEGIN IMMEDIATE")
    try:
        for item in preview["items"]:
            if item["action"] == "INVALID":
                raise InvalidInputError("invalid item reached confirm", {"index": item["index"]})

            normalized = item["normalized"]
            assert isinstance(normalized, dict)
            tag_code = str(normalized["tag_code"])
            channel_slug = str(normalized["channel_slug"])
            is_active = bool(normalized["is_active"])

            tag_row = conn.execute(
                "SELECT id, category FROM custom_tags WHERE code = ? ORDER BY id ASC LIMIT 1",
                (tag_code,),
            ).fetchone()
            if tag_row is None:
                raise InvalidInputError("custom tag not found for tag_code", {"tag_code": tag_code})
            if str(tag_row["category"]) != "VISUAL":
                raise InvalidInputError("bindings are only allowed for VISUAL tags", {"tag_code": tag_code, "category": str(tag_row["category"])})
            if conn.execute("SELECT 1 FROM channels WHERE slug = ? LIMIT 1", (channel_slug,)).fetchone() is None:
                raise InvalidInputError("channel not found", {"channel_slug": channel_slug})

            tag_id = int(tag_row["id"])
            existing = conn.execute(
                "SELECT id FROM custom_tag_channel_bindings WHERE tag_id = ? AND channel_slug = ? LIMIT 1",
                (tag_id, channel_slug),
            ).fetchone()
            has_binding = existing is not None
            if (not has_binding) and is_active:
                conn.execute(
                    "INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at) VALUES(?,?,?)",
                    (tag_id, channel_slug, now_text),
                )
                created += 1
                results.append({"index": item["index"], "action": "CREATE"})
                continue

            if has_binding and (not is_active):
                conn.execute("DELETE FROM custom_tag_channel_bindings WHERE id = ?", (int(existing["id"]),))
                updated += 1
                results.append({"index": item["index"], "action": "UPDATE"})
                continue

            noop += 1
            results.append({"index": item["index"], "action": "NOOP"})

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "ok": True,
        "summary": {"total": len(items), "created": created, "updated": updated, "noop": noop, "invalid": 0},
        "results": results,
    }
