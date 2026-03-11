from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


VALID_OPERATORS = {
    "equals",
    "not_equals",
    "gt",
    "gte",
    "lt",
    "lte",
    "contains",
    "in",
    "between",
}
VALID_MATCH_MODES = {"ALL", "ANY"}


@dataclass
class RulesError(Exception):
    code: str
    message: str
    status_code: int
    details: dict[str, Any] | None = None


class TagNotFoundError(RulesError):
    def __init__(self, tag_id: int):
        super().__init__(
            code="CTA_TAG_NOT_FOUND",
            message="custom tag not found",
            status_code=404,
            details={"tag_id": tag_id},
        )


class CtuTagNotFoundError(RulesError):
    def __init__(self, tag_id: int):
        super().__init__(
            code="CTU_TAG_NOT_FOUND",
            message="custom tag not found",
            status_code=404,
            details={"tag_id": tag_id},
        )


class CtuRuleNotFoundError(RulesError):
    def __init__(self, tag_id: int, rule_id: int):
        super().__init__(
            code="CTU_RULE_NOT_FOUND",
            message="custom tag rule not found",
            status_code=404,
            details={"tag_id": tag_id, "rule_id": rule_id},
        )


class CtuInvalidPayloadError(RulesError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(
            code="CTU_INVALID_PAYLOAD",
            message=message,
            status_code=400,
            details=details,
        )


class CtuBindingNotAllowedForCategoryError(RulesError):
    def __init__(self, tag_id: int, category: str):
        super().__init__(
            code="CTU_BINDING_NOT_ALLOWED_FOR_CATEGORY",
            message="bindings are only allowed for VISUAL tags",
            status_code=400,
            details={"tag_id": tag_id, "category": category},
        )


class CtuReplaceAllRulesFailedError(RulesError):
    def __init__(self, tag_id: int, reason: str):
        super().__init__(
            code="CTU_REPLACE_ALL_RULES_FAILED",
            message="replace all rules failed",
            status_code=400,
            details={"tag_id": tag_id, "reason": reason},
        )


class InvalidInputError(RulesError):
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


def _normalize_bool(value: Any, field: str) -> int:
    if not isinstance(value, bool):
        raise InvalidInputError(f"{field} must be a boolean", {"field": field})
    return 1 if value else 0


def _normalize_operator(value: Any) -> str:
    operator = _normalize_nonempty(value, "operator")
    if operator not in VALID_OPERATORS:
        raise InvalidInputError(
            "operator must be one of equals, not_equals, gt, gte, lt, lte, contains, in, between",
            {"field": "operator"},
        )
    return operator


def _normalize_match_mode(value: Any) -> str:
    if not isinstance(value, str):
        raise InvalidInputError("match_mode must be a string", {"field": "match_mode"})
    mode = value.strip().upper()
    if mode not in VALID_MATCH_MODES:
        raise InvalidInputError("match_mode must be ALL or ANY", {"field": "match_mode"})
    return mode


def _normalize_value_json(value: Any) -> str:
    if not isinstance(value, str):
        raise InvalidInputError("value_json must be a JSON string", {"field": "value_json"})
    try:
        json.loads(value)
    except json.JSONDecodeError as exc:
        raise InvalidInputError(f"value_json must be valid JSON: {exc.msg}", {"field": "value_json"}) from exc
    return value


def _normalize_priority(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidInputError("priority must be an integer", {"field": "priority"})
    return value


def _normalize_weight(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidInputError("weight must be a number or null", {"field": "weight"})
    return float(value)


def _require_tag(conn: sqlite3.Connection, tag_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT id, category FROM custom_tags WHERE id = ?",
        (tag_id,),
    ).fetchone()
    if row is None:
        raise TagNotFoundError(tag_id)
    return row


def _require_tag_ctu(conn: sqlite3.Connection, tag_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT id, category FROM custom_tags WHERE id = ?",
        (tag_id,),
    ).fetchone()
    if row is None:
        raise CtuTagNotFoundError(tag_id)
    return row


def _ctu_summary(row: dict[str, Any]) -> str:
    state = "active" if bool(row["is_active"]) else "inactive"
    return f"{state}: {row['source_path']} {row['operator']} {row['value_json']}"


def _rule_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "tag_id": int(row["tag_id"]),
        "source_path": str(row["source_path"]),
        "operator": str(row["operator"]),
        "value_json": str(row["value_json"]),
        "match_mode": str(row["match_mode"]),
        "priority": int(row["priority"]),
        "weight": None if row["weight"] is None else float(row["weight"]),
        "required": bool(row["required"]),
        "stop_after_match": bool(row["stop_after_match"]),
        "is_active": bool(row["is_active"]),
        "summary": _ctu_summary(row),
    }


def _normalize_rule_payload(payload: dict[str, Any], *, tag_id: int) -> dict[str, Any]:
    return {
        "tag_id": tag_id,
        "source_path": _normalize_nonempty(payload.get("source_path"), "source_path"),
        "operator": _normalize_operator(payload.get("operator")),
        "value_json": _normalize_value_json(payload.get("value_json")),
        "match_mode": _normalize_match_mode(payload.get("match_mode", "ALL")),
        "priority": _normalize_priority(payload.get("priority", 100)),
        "weight": _normalize_weight(payload.get("weight")),
        "required": bool(_normalize_bool(payload.get("required", False), "required")),
        "stop_after_match": bool(_normalize_bool(payload.get("stop_after_match", False), "stop_after_match")),
        "is_active": bool(_normalize_bool(payload.get("is_active", True), "is_active")),
    }


def list_rules(conn: sqlite3.Connection, tag_id: int) -> list[dict[str, Any]]:
    _require_tag(conn, tag_id)
    rows = conn.execute(
        """
        SELECT id, tag_id, source_path, operator, value_json, match_mode,
               priority, weight, required, stop_after_match, is_active
        FROM custom_tag_rules
        WHERE tag_id = ?
        ORDER BY priority DESC, id ASC
        """,
        (tag_id,),
    ).fetchall()
    return [_rule_row_to_dict(row) for row in rows]


def create_rule(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    tag_id = payload.get("tag_id")
    if not isinstance(tag_id, int) or isinstance(tag_id, bool):
        raise InvalidInputError("tag_id must be an integer", {"field": "tag_id"})
    _require_tag(conn, tag_id)

    source_path = _normalize_nonempty(payload.get("source_path"), "source_path")
    operator = _normalize_operator(payload.get("operator"))
    value_json = _normalize_value_json(payload.get("value_json"))
    match_mode = _normalize_match_mode(payload.get("match_mode", "ALL"))
    priority = _normalize_priority(payload.get("priority", 100))
    weight = _normalize_weight(payload.get("weight"))
    required = _normalize_bool(payload.get("required", False), "required")
    stop_after_match = _normalize_bool(payload.get("stop_after_match", False), "stop_after_match")
    is_active = _normalize_bool(payload.get("is_active", True), "is_active")

    now_text = _now_text()
    cur = conn.execute(
        """
        INSERT INTO custom_tag_rules(
            tag_id, source_path, operator, value_json, match_mode, priority, weight,
            required, stop_after_match, is_active, created_at, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            tag_id,
            source_path,
            operator,
            value_json,
            match_mode,
            priority,
            weight,
            required,
            stop_after_match,
            is_active,
            now_text,
            now_text,
        ),
    )
    row = conn.execute(
        """
        SELECT id, tag_id, source_path, operator, value_json, match_mode,
               priority, weight, required, stop_after_match, is_active
        FROM custom_tag_rules
        WHERE id = ?
        """,
        (int(cur.lastrowid),),
    ).fetchone()
    assert row is not None
    return _rule_row_to_dict(row)


def update_rule(conn: sqlite3.Connection, rule_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    if "tag_id" in payload:
        raise InvalidInputError("tag_id is not editable", {"field": "tag_id"})

    allowed = {
        "source_path",
        "operator",
        "value_json",
        "match_mode",
        "priority",
        "weight",
        "required",
        "stop_after_match",
        "is_active",
    }
    extra = sorted(set(payload.keys()) - allowed)
    if extra:
        raise InvalidInputError("unknown fields in patch", {"fields": extra})

    existing = conn.execute(
        """
        SELECT id, tag_id, source_path, operator, value_json, match_mode,
               priority, weight, required, stop_after_match, is_active
        FROM custom_tag_rules
        WHERE id = ?
        """,
        (rule_id,),
    ).fetchone()
    if existing is None:
        raise InvalidInputError("rule not found", {"rule_id": rule_id})

    source_path = _normalize_nonempty(payload["source_path"], "source_path") if "source_path" in payload else existing["source_path"]
    operator = _normalize_operator(payload["operator"]) if "operator" in payload else existing["operator"]
    value_json = _normalize_value_json(payload["value_json"]) if "value_json" in payload else existing["value_json"]
    match_mode = _normalize_match_mode(payload["match_mode"]) if "match_mode" in payload else existing["match_mode"]
    priority = _normalize_priority(payload["priority"]) if "priority" in payload else int(existing["priority"])
    weight = _normalize_weight(payload["weight"]) if "weight" in payload else existing["weight"]
    required = _normalize_bool(payload["required"], "required") if "required" in payload else int(existing["required"])
    stop_after_match = _normalize_bool(payload["stop_after_match"], "stop_after_match") if "stop_after_match" in payload else int(existing["stop_after_match"])
    is_active = _normalize_bool(payload["is_active"], "is_active") if "is_active" in payload else int(existing["is_active"])

    conn.execute(
        """
        UPDATE custom_tag_rules
        SET source_path = ?, operator = ?, value_json = ?, match_mode = ?,
            priority = ?, weight = ?, required = ?, stop_after_match = ?, is_active = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            source_path,
            operator,
            value_json,
            match_mode,
            priority,
            weight,
            required,
            stop_after_match,
            is_active,
            _now_text(),
            rule_id,
        ),
    )
    row = conn.execute(
        """
        SELECT id, tag_id, source_path, operator, value_json, match_mode,
               priority, weight, required, stop_after_match, is_active
        FROM custom_tag_rules
        WHERE id = ?
        """,
        (rule_id,),
    ).fetchone()
    assert row is not None
    return _rule_row_to_dict(row)


def delete_rule(conn: sqlite3.Connection, rule_id: int) -> None:
    cur = conn.execute("DELETE FROM custom_tag_rules WHERE id = ?", (rule_id,))
    if cur.rowcount == 0:
        raise InvalidInputError("rule not found", {"rule_id": rule_id})


def _binding_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "tag_id": int(row["tag_id"]),
        "channel_slug": str(row["channel_slug"]),
    }


def list_channel_bindings(conn: sqlite3.Connection, tag_id: int) -> list[dict[str, Any]]:
    _require_tag(conn, tag_id)
    rows = conn.execute(
        """
        SELECT id, tag_id, channel_slug
        FROM custom_tag_channel_bindings
        WHERE tag_id = ?
        ORDER BY id ASC
        """,
        (tag_id,),
    ).fetchall()
    return [_binding_row_to_dict(row) for row in rows]


def create_channel_binding(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    tag_id = payload.get("tag_id")
    if not isinstance(tag_id, int) or isinstance(tag_id, bool):
        raise InvalidInputError("tag_id must be an integer", {"field": "tag_id"})
    tag_row = _require_tag(conn, tag_id)
    if str(tag_row["category"]) != "VISUAL":
        raise InvalidInputError("channel bindings are only allowed for VISUAL tags", {"tag_id": tag_id})

    channel_slug = _normalize_nonempty(payload.get("channel_slug"), "channel_slug")
    channel_row = conn.execute("SELECT 1 FROM channels WHERE slug = ?", (channel_slug,)).fetchone()
    if channel_row is None:
        raise InvalidInputError("channel_slug not found", {"field": "channel_slug", "channel_slug": channel_slug})

    now_text = _now_text()
    try:
        cur = conn.execute(
            """
            INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at)
            VALUES(?,?,?)
            """,
            (tag_id, channel_slug, now_text),
        )
    except sqlite3.IntegrityError as exc:
        raise InvalidInputError("channel binding already exists", {"tag_id": tag_id, "channel_slug": channel_slug}) from exc

    row = conn.execute(
        "SELECT id, tag_id, channel_slug FROM custom_tag_channel_bindings WHERE id = ?",
        (int(cur.lastrowid),),
    ).fetchone()
    assert row is not None
    return _binding_row_to_dict(row)


def delete_channel_binding(conn: sqlite3.Connection, binding_id: int) -> None:
    cur = conn.execute("DELETE FROM custom_tag_channel_bindings WHERE id = ?", (binding_id,))
    if cur.rowcount == 0:
        raise InvalidInputError("channel binding not found", {"binding_id": binding_id})


def list_rules_for_modal(conn: sqlite3.Connection, tag_id: int) -> list[dict[str, Any]]:
    _require_tag_ctu(conn, tag_id)
    return list_rules(conn, tag_id)


def create_rule_for_modal(conn: sqlite3.Connection, tag_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    _require_tag_ctu(conn, tag_id)
    try:
        normalized = _normalize_rule_payload(payload, tag_id=tag_id)
    except InvalidInputError as exc:
        raise CtuInvalidPayloadError(exc.message, exc.details) from exc
    return create_rule(conn, normalized)


def update_rule_for_modal(conn: sqlite3.Connection, tag_id: int, rule_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    _require_tag_ctu(conn, tag_id)
    existing = conn.execute("SELECT id, tag_id FROM custom_tag_rules WHERE id = ?", (rule_id,)).fetchone()
    if existing is None or int(existing["tag_id"]) != tag_id:
        raise CtuRuleNotFoundError(tag_id, rule_id)
    try:
        return update_rule(conn, rule_id=rule_id, payload=payload)
    except InvalidInputError as exc:
        raise CtuInvalidPayloadError(exc.message, exc.details) from exc


def delete_rule_for_modal(conn: sqlite3.Connection, tag_id: int, rule_id: int) -> None:
    _require_tag_ctu(conn, tag_id)
    existing = conn.execute("SELECT id, tag_id FROM custom_tag_rules WHERE id = ?", (rule_id,)).fetchone()
    if existing is None or int(existing["tag_id"]) != tag_id:
        raise CtuRuleNotFoundError(tag_id, rule_id)
    conn.execute("DELETE FROM custom_tag_rules WHERE id = ?", (rule_id,))


def replace_all_rules_for_modal(conn: sqlite3.Connection, tag_id: int, rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    _require_tag_ctu(conn, tag_id)
    normalized_rules: list[dict[str, Any]] = []
    try:
        for item in rules:
            normalized_rules.append(_normalize_rule_payload(item, tag_id=tag_id))
    except InvalidInputError as exc:
        raise CtuInvalidPayloadError(exc.message, exc.details) from exc

    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM custom_tag_rules WHERE tag_id = ?", (tag_id,))
        for item in normalized_rules:
            create_rule(conn, item)
        conn.execute("COMMIT")
    except RulesError:
        conn.execute("ROLLBACK")
        raise
    except Exception as exc:
        conn.execute("ROLLBACK")
        raise CtuReplaceAllRulesFailedError(tag_id, str(exc)) from exc
    return list_rules(conn, tag_id)


def list_bindings_for_modal(conn: sqlite3.Connection, tag_id: int) -> list[dict[str, Any]]:
    _require_tag_ctu(conn, tag_id)
    return list_channel_bindings(conn, tag_id)


def replace_bindings_for_modal(conn: sqlite3.Connection, tag_id: int, channel_slugs: list[str]) -> list[dict[str, Any]]:
    tag = _require_tag_ctu(conn, tag_id)
    if str(tag["category"]) != "VISUAL":
        raise CtuBindingNotAllowedForCategoryError(tag_id, str(tag["category"]))

    normalized_slugs: list[str] = []
    seen: set[str] = set()
    for slug in channel_slugs:
        try:
            normalized = _normalize_nonempty(slug, "channel_slug")
        except InvalidInputError as exc:
            raise CtuInvalidPayloadError(exc.message, exc.details) from exc
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_slugs.append(normalized)

    missing = [
        slug
        for slug in normalized_slugs
        if conn.execute("SELECT 1 FROM channels WHERE slug = ?", (slug,)).fetchone() is None
    ]
    if missing:
        raise CtuInvalidPayloadError("channel_slug not found", {"channel_slugs": missing})

    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM custom_tag_channel_bindings WHERE tag_id = ?", (tag_id,))
        now_text = _now_text()
        for slug in normalized_slugs:
            conn.execute(
                "INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at) VALUES(?,?,?)",
                (tag_id, slug, now_text),
            )
        conn.execute("COMMIT")
    except Exception as exc:
        conn.execute("ROLLBACK")
        raise CtuInvalidPayloadError("failed to replace bindings", {"error": str(exc)}) from exc
    return list_channel_bindings(conn, tag_id)
