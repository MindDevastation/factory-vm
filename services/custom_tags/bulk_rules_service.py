from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import sqlite3
from typing import Any

from services.custom_tags import rules_service


@dataclass
class BulkRulesError(Exception):
    code: str
    message: str
    status_code: int
    details: dict[str, Any] | None = None


class InvalidInputError(BulkRulesError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(code="CTA_INVALID_INPUT", message=message, status_code=400, details=details)


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rule_summary(item: dict[str, Any]) -> str:
    state = "active" if item["is_active"] else "inactive"
    return (
        f"{state}: {item['source_path']} {item['operator']} {item['value_json']} "
        f"(match={item['match_mode']}, priority={item['priority']})"
    )


def _normalize_item(conn: sqlite3.Connection, item: dict[str, Any], *, index: int) -> tuple[dict[str, Any], dict[str, Any]]:
    tag_code_raw = item.get("tag_code")
    if not isinstance(tag_code_raw, str):
        raise InvalidInputError("items[index].tag_code must be a string", {"field": f"items[{index}].tag_code"})
    tag_code = tag_code_raw.strip()
    if not tag_code:
        raise InvalidInputError("items[index].tag_code must not be empty", {"field": f"items[{index}].tag_code"})

    tag_row = conn.execute(
        "SELECT id FROM custom_tags WHERE code = ? ORDER BY id ASC LIMIT 1",
        (tag_code,),
    ).fetchone()
    if tag_row is None:
        raise InvalidInputError("custom tag not found for tag_code", {"field": f"items[{index}].tag_code", "tag_code": tag_code})

    payload = dict(item)
    payload["tag_id"] = int(tag_row["id"])
    payload.pop("tag_code", None)

    try:
        normalized = rules_service._normalize_rule_payload(payload, tag_id=int(tag_row["id"]))
    except rules_service.InvalidInputError as exc:
        raise InvalidInputError(exc.message, exc.details) from exc

    return normalized, {"tag_id": int(tag_row["id"]), "tag_code": tag_code}


def preview_bulk_rules(conn: sqlite3.Connection, items: list[dict[str, Any]]) -> dict[str, Any]:
    draft: list[dict[str, Any]] = []
    counts = {"CREATE": 0, "INVALID": 0}

    for idx, raw in enumerate(items):
        errors: list[dict[str, Any]] = []
        normalized: dict[str, Any] | None = None
        resolved: dict[str, Any] = {"tag_id": None, "tag_code": None}
        action = "INVALID"
        summary = "invalid item"

        try:
            normalized, resolved = _normalize_item(conn, raw, index=idx)
            action = "CREATE"
            summary = _rule_summary(normalized)
        except BulkRulesError as err:
            errors.append({"code": err.code, "message": err.message, "details": err.details or {}})
            summary = err.message

        draft.append(
            {
                "index": idx,
                "normalized": normalized,
                "resolved": resolved,
                "action": action,
                "summary": summary,
                "errors": errors,
            }
        )

    dedup_map: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in draft:
        if row["action"] != "CREATE":
            continue
        normalized = row["normalized"]
        assert isinstance(normalized, dict)
        key = (
            normalized["tag_id"],
            normalized["source_path"],
            normalized["operator"],
            normalized["value_json"],
            normalized["match_mode"],
        )
        dedup_map.setdefault(key, []).append(row)

    for group in dedup_map.values():
        if len(group) <= 1:
            continue
        baseline = group[0]["normalized"]
        assert isinstance(baseline, dict)
        for row in group[1:]:
            normalized = row["normalized"]
            assert isinstance(normalized, dict)
            conflicts = {}
            for field in ("priority", "weight", "required", "stop_after_match", "is_active"):
                if normalized[field] != baseline[field]:
                    conflicts[field] = [baseline[field], normalized[field]]

            if conflicts:
                code = "CTA_DUPLICATE_RULE_CONFLICT"
                message = "conflicting duplicate rule in payload"
                details = {
                    "tag_id": normalized["tag_id"],
                    "source_path": normalized["source_path"],
                    "operator": normalized["operator"],
                    "value_json": normalized["value_json"],
                    "match_mode": normalized["match_mode"],
                    "conflicts": conflicts,
                }
            else:
                code = "CTA_DUPLICATE_RULE"
                message = "duplicate rule in payload"
                details = {
                    "tag_id": normalized["tag_id"],
                    "source_path": normalized["source_path"],
                    "operator": normalized["operator"],
                    "value_json": normalized["value_json"],
                    "match_mode": normalized["match_mode"],
                }

            row["errors"].append({"code": code, "message": message, "details": details})
            row["action"] = "INVALID"
            row["summary"] = message

    results: list[dict[str, Any]] = []
    for row in draft:
        counts[row["action"]] += 1
        results.append(row)

    return {
        "can_confirm": counts["INVALID"] == 0,
        "summary": {"total": len(items), "create": counts["CREATE"], "invalid": counts["INVALID"]},
        "items": results,
    }


def confirm_bulk_rules(conn: sqlite3.Connection, items: list[dict[str, Any]]) -> dict[str, Any]:
    preview = preview_bulk_rules(conn, items)
    if not preview["can_confirm"]:
        return {"ok": False, "summary": preview["summary"], "results": [], "errors": ["preview contains invalid items"]}

    now_text = _now_text()
    created = 0
    results: list[dict[str, Any]] = []

    conn.execute("BEGIN IMMEDIATE")
    try:
        for item in preview["items"]:
            if item["action"] != "CREATE":
                raise InvalidInputError("invalid item reached confirm", {"index": item["index"]})
            normalized = item["normalized"]
            assert isinstance(normalized, dict)
            conn.execute(
                """
                INSERT INTO custom_tag_rules(
                    tag_id, source_path, operator, value_json, match_mode, priority, weight,
                    required, stop_after_match, is_active, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    normalized["tag_id"],
                    normalized["source_path"],
                    normalized["operator"],
                    normalized["value_json"],
                    normalized["match_mode"],
                    normalized["priority"],
                    normalized["weight"],
                    1 if normalized["required"] else 0,
                    1 if normalized["stop_after_match"] else 0,
                    1 if normalized["is_active"] else 0,
                    now_text,
                    now_text,
                ),
            )
            created += 1
            results.append({"index": item["index"], "action": "CREATE"})

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "ok": True,
        "summary": {"total": len(items), "created": created, "invalid": 0},
        "results": results,
    }
