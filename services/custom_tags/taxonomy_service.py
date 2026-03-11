from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.custom_tags import rules_service


TAXONOMY_SCHEMA_VERSION = "custom_tags_taxonomy/1"


@dataclass
class TaxonomyError(Exception):
    code: str
    message: str
    status_code: int
    details: dict[str, Any] | None = None


class InvalidInputError(TaxonomyError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__("CTA_INVALID_INPUT", message, 400, details)


class NotFoundError(TaxonomyError):
    def __init__(self, message: str, details: dict[str, Any]):
        super().__init__("CTA_TAG_NOT_FOUND", message, 404, details)


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_ids(values: list[int], *, field: str) -> list[int]:
    if not isinstance(values, list) or not values:
        raise InvalidInputError(f"{field} must be a non-empty list", {"field": field})
    out: list[int] = []
    for value in values:
        if not isinstance(value, int) or isinstance(value, bool):
            raise InvalidInputError(f"{field} must contain integers", {"field": field})
        out.append(value)
    return out


def clone_tag(
    conn: sqlite3.Connection,
    *,
    source_tag_id: int,
    code: str,
    label: str,
    description: str | None = None,
    include_rules: bool = True,
    include_bindings: bool = True,
    is_active: bool = True,
) -> dict[str, Any]:
    source = conn.execute(
        "SELECT id, category FROM custom_tags WHERE id = ?",
        (source_tag_id,),
    ).fetchone()
    if source is None:
        raise NotFoundError("custom tag not found", {"tag_id": source_tag_id})

    now_text = _now_text()
    try:
        cur = conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (code.strip(), label.strip(), str(source["category"]), description, 1 if is_active else 0, now_text, now_text),
        )
    except sqlite3.IntegrityError as exc:
        raise InvalidInputError("tag with same category and code already exists", {"category": str(source["category"]), "code": code}) from exc

    cloned_tag_id = int(cur.lastrowid)
    cloned_rules = 0
    cloned_bindings = 0

    if include_rules:
        rules = conn.execute(
            """
            SELECT source_path, operator, value_json, match_mode, priority, weight, required, stop_after_match, is_active
            FROM custom_tag_rules WHERE tag_id = ?
            ORDER BY priority DESC, id ASC
            """,
            (source_tag_id,),
        ).fetchall()
        for row in rules:
            conn.execute(
                """
                INSERT INTO custom_tag_rules(tag_id, source_path, operator, value_json, match_mode, priority, weight, required, stop_after_match, is_active, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    cloned_tag_id,
                    row["source_path"],
                    row["operator"],
                    row["value_json"],
                    row["match_mode"],
                    row["priority"],
                    row["weight"],
                    row["required"],
                    row["stop_after_match"],
                    row["is_active"],
                    now_text,
                    now_text,
                ),
            )
            cloned_rules += 1

    if include_bindings and str(source["category"]) == "VISUAL":
        bindings = conn.execute(
            "SELECT channel_slug FROM custom_tag_channel_bindings WHERE tag_id = ? ORDER BY id ASC",
            (source_tag_id,),
        ).fetchall()
        for row in bindings:
            conn.execute(
                "INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at) VALUES(?,?,?)",
                (cloned_tag_id, row["channel_slug"], now_text),
            )
            cloned_bindings += 1

    out = conn.execute(
        "SELECT id, code, label, category, description, is_active FROM custom_tags WHERE id = ?",
        (cloned_tag_id,),
    ).fetchone()
    assert out is not None
    return {
        "tag": {
            "id": int(out["id"]),
            "code": str(out["code"]),
            "label": str(out["label"]),
            "category": str(out["category"]),
            "description": out["description"],
            "is_active": bool(out["is_active"]),
        },
        "cloned_rules": cloned_rules,
        "cloned_bindings": cloned_bindings,
    }


def clone_rules(conn: sqlite3.Connection, *, source_tag_id: int, target_tag_id: int, replace_all: bool = False) -> dict[str, Any]:
    source = conn.execute("SELECT id FROM custom_tags WHERE id = ?", (source_tag_id,)).fetchone()
    target = conn.execute("SELECT id FROM custom_tags WHERE id = ?", (target_tag_id,)).fetchone()
    if source is None:
        raise NotFoundError("custom tag not found", {"tag_id": source_tag_id})
    if target is None:
        raise NotFoundError("custom tag not found", {"tag_id": target_tag_id})

    rows = conn.execute(
        """
        SELECT source_path, operator, value_json, match_mode, priority, weight, required, stop_after_match, is_active
        FROM custom_tag_rules WHERE tag_id = ? ORDER BY priority DESC, id ASC
        """,
        (source_tag_id,),
    ).fetchall()
    now_text = _now_text()
    if replace_all:
        conn.execute("DELETE FROM custom_tag_rules WHERE tag_id = ?", (target_tag_id,))
    created = 0
    for row in rows:
        conn.execute(
            """
            INSERT INTO custom_tag_rules(tag_id, source_path, operator, value_json, match_mode, priority, weight, required, stop_after_match, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                target_tag_id,
                row["source_path"],
                row["operator"],
                row["value_json"],
                row["match_mode"],
                row["priority"],
                row["weight"],
                row["required"],
                row["stop_after_match"],
                row["is_active"],
                now_text,
                now_text,
            ),
        )
        created += 1
    return {"source_tag_id": source_tag_id, "target_tag_id": target_tag_id, "cloned_rules": created, "replace_all": replace_all}


def bulk_set_tags_active(conn: sqlite3.Connection, *, tag_ids: list[int], is_active: bool) -> dict[str, Any]:
    ids = _normalize_ids(tag_ids, field="tag_ids")
    placeholders = ",".join("?" for _ in ids)
    now_text = _now_text()
    cur = conn.execute(f"UPDATE custom_tags SET is_active = ?, updated_at = ? WHERE id IN ({placeholders})", (1 if is_active else 0, now_text, *ids))
    return {"updated": int(cur.rowcount), "is_active": is_active}


def bulk_set_rules_active(conn: sqlite3.Connection, *, rule_ids: list[int], is_active: bool) -> dict[str, Any]:
    ids = _normalize_ids(rule_ids, field="rule_ids")
    placeholders = ",".join("?" for _ in ids)
    now_text = _now_text()
    cur = conn.execute(f"UPDATE custom_tag_rules SET is_active = ?, updated_at = ? WHERE id IN ({placeholders})", (1 if is_active else 0, now_text, *ids))
    return {"updated": int(cur.rowcount), "is_active": is_active}


def bulk_set_bindings_enabled(conn: sqlite3.Connection, *, items: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(items, list) or not items:
        raise InvalidInputError("items must be a non-empty list", {"field": "items"})
    enabled = 0
    disabled = 0
    now_text = _now_text()
    for item in items:
        tag_id = item.get("tag_id")
        channel_slug = str(item.get("channel_slug", "")).strip()
        is_enabled = item.get("is_enabled")
        if not isinstance(tag_id, int) or not channel_slug or not isinstance(is_enabled, bool):
            raise InvalidInputError("each item must include tag_id:int, channel_slug:str, is_enabled:bool")
        row = conn.execute(
            "SELECT id FROM custom_tag_channel_bindings WHERE tag_id = ? AND channel_slug = ?",
            (tag_id, channel_slug),
        ).fetchone()
        if is_enabled:
            if row is None:
                conn.execute(
                    "INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at) VALUES(?,?,?)",
                    (tag_id, channel_slug, now_text),
                )
                enabled += 1
        else:
            if row is not None:
                conn.execute("DELETE FROM custom_tag_channel_bindings WHERE id = ?", (int(row["id"]),))
                disabled += 1
    return {"enabled": enabled, "disabled": disabled}


def export_taxonomy(conn: sqlite3.Connection) -> dict[str, Any]:
    tags = [dict(row) for row in conn.execute("SELECT id, code, label, category, description, is_active FROM custom_tags ORDER BY category, code, id").fetchall()]
    bindings = [dict(row) for row in conn.execute("SELECT tag_id, channel_slug FROM custom_tag_channel_bindings ORDER BY tag_id, channel_slug").fetchall()]
    rules = [dict(row) for row in conn.execute("SELECT tag_id, source_path, operator, value_json, match_mode, priority, weight, required, stop_after_match, is_active FROM custom_tag_rules ORDER BY tag_id, priority DESC, id ASC").fetchall()]
    for row in tags:
        row["id"] = int(row["id"])
        row["is_active"] = bool(row["is_active"])
    for row in rules:
        row["required"] = bool(row["required"])
        row["stop_after_match"] = bool(row["stop_after_match"])
        row["is_active"] = bool(row["is_active"])
    return {
        "schema_version": TAXONOMY_SCHEMA_VERSION,
        "exported_at": _now_text(),
        "tags": tags,
        "bindings": bindings,
        "rules": rules,
    }


def _validate_taxonomy_payload(payload: dict[str, Any]) -> None:
    if payload.get("schema_version") != TAXONOMY_SCHEMA_VERSION:
        raise InvalidInputError(f"schema_version must be {TAXONOMY_SCHEMA_VERSION}", {"field": "schema_version"})
    for field in ("tags", "bindings", "rules"):
        if not isinstance(payload.get(field), list):
            raise InvalidInputError(f"{field} must be an array", {"field": field})


def import_taxonomy_preview(conn: sqlite3.Connection, *, payload: dict[str, Any]) -> dict[str, Any]:
    _validate_taxonomy_payload(payload)
    tags = payload["tags"]
    bindings = payload["bindings"]
    rules = payload["rules"]

    tag_key_by_id: dict[int, tuple[str, str]] = {}
    for tag in tags:
        tag_id = tag.get("id")
        if isinstance(tag_id, int) and not isinstance(tag_id, bool):
            tag_key_by_id[tag_id] = (str(tag["category"]), str(tag["code"]))

    normalized_bindings: list[dict[str, Any]] = []
    for binding in bindings:
        if "tag_category" in binding and "tag_code" in binding:
            normalized_bindings.append(binding)
            continue
        tag_id = binding.get("tag_id")
        key = tag_key_by_id.get(tag_id) if isinstance(tag_id, int) and not isinstance(tag_id, bool) else None
        if key is None:
            raise InvalidInputError("binding must include tag_category+tag_code or a known tag_id", {"binding": binding})
        normalized_bindings.append(
            {
                "tag_category": key[0],
                "tag_code": key[1],
                "channel_slug": binding.get("channel_slug"),
            }
        )

    normalized_rules: list[dict[str, Any]] = []
    for rule in rules:
        if "tag_category" in rule and "tag_code" in rule:
            normalized_rules.append(rule)
            continue
        tag_id = rule.get("tag_id")
        key = tag_key_by_id.get(tag_id) if isinstance(tag_id, int) and not isinstance(tag_id, bool) else None
        if key is None:
            raise InvalidInputError("rule must include tag_category+tag_code or a known tag_id", {"rule": rule})
        normalized_rule = dict(rule)
        normalized_rule["tag_category"] = key[0]
        normalized_rule["tag_code"] = key[1]
        normalized_rules.append(normalized_rule)

    existing_tags = {
        (str(row["category"]), str(row["code"])): row
        for row in conn.execute("SELECT id, category, code, label, description, is_active FROM custom_tags").fetchall()
    }

    inserts = 0
    updates = 0
    unchanged = 0
    for tag in tags:
        key = (str(tag["category"]), str(tag["code"]))
        current = existing_tags.get(key)
        if current is None:
            inserts += 1
        elif (
            str(current["label"]) == str(tag["label"])
            and current["description"] == tag.get("description")
            and bool(current["is_active"]) == bool(tag.get("is_active", True))
        ):
            unchanged += 1
        else:
            updates += 1

    return {
        "can_confirm": True,
        "summary": {
            "tags_total": len(tags),
            "tag_inserts": inserts,
            "tag_updates": updates,
            "tag_unchanged": unchanged,
            "bindings_replace_count": len(normalized_bindings),
            "rules_replace_count": len(normalized_rules),
        },
    }


def import_taxonomy_confirm(conn: sqlite3.Connection, *, payload: dict[str, Any]) -> dict[str, Any]:
    preview = import_taxonomy_preview(conn, payload=payload)
    tags = payload["tags"]
    bindings = payload["bindings"]
    rules = payload["rules"]
    now_text = _now_text()

    tag_key_by_id: dict[int, tuple[str, str]] = {}
    for tag in tags:
        tag_id = tag.get("id")
        if isinstance(tag_id, int) and not isinstance(tag_id, bool):
            tag_key_by_id[tag_id] = (str(tag["category"]), str(tag["code"]))

    normalized_bindings: list[dict[str, Any]] = []
    for binding in bindings:
        if "tag_category" in binding and "tag_code" in binding:
            normalized_bindings.append(binding)
            continue
        tag_id = binding.get("tag_id")
        key = tag_key_by_id.get(tag_id) if isinstance(tag_id, int) and not isinstance(tag_id, bool) else None
        if key is None:
            raise InvalidInputError("binding must include tag_category+tag_code or a known tag_id", {"binding": binding})
        normalized_bindings.append(
            {
                "tag_category": key[0],
                "tag_code": key[1],
                "channel_slug": binding.get("channel_slug"),
            }
        )

    normalized_rules: list[dict[str, Any]] = []
    for rule in rules:
        if "tag_category" in rule and "tag_code" in rule:
            normalized_rules.append(rule)
            continue
        tag_id = rule.get("tag_id")
        key = tag_key_by_id.get(tag_id) if isinstance(tag_id, int) and not isinstance(tag_id, bool) else None
        if key is None:
            raise InvalidInputError("rule must include tag_category+tag_code or a known tag_id", {"rule": rule})
        normalized_rule = dict(rule)
        normalized_rule["tag_category"] = key[0]
        normalized_rule["tag_code"] = key[1]
        normalized_rules.append(normalized_rule)

    conn.execute("BEGIN IMMEDIATE")
    try:
        tag_id_by_key: dict[tuple[str, str], int] = {}
        for tag in tags:
            category = str(tag["category"])
            code = str(tag["code"])
            row = conn.execute("SELECT id FROM custom_tags WHERE category = ? AND code = ?", (category, code)).fetchone()
            if row is None:
                cur = conn.execute(
                    "INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
                    (code, str(tag["label"]), category, tag.get("description"), 1 if bool(tag.get("is_active", True)) else 0, now_text, now_text),
                )
                tag_id = int(cur.lastrowid)
            else:
                tag_id = int(row["id"])
                conn.execute(
                    "UPDATE custom_tags SET label = ?, description = ?, is_active = ?, updated_at = ? WHERE id = ?",
                    (str(tag["label"]), tag.get("description"), 1 if bool(tag.get("is_active", True)) else 0, now_text, tag_id),
                )
            tag_id_by_key[(category, code)] = tag_id

        included_tag_ids = tuple(tag_id_by_key.values())
        if included_tag_ids:
            placeholders = ",".join("?" for _ in included_tag_ids)
            conn.execute(f"DELETE FROM custom_tag_channel_bindings WHERE tag_id IN ({placeholders})", included_tag_ids)
            conn.execute(f"DELETE FROM custom_tag_rules WHERE tag_id IN ({placeholders})", included_tag_ids)

        for binding in normalized_bindings:
            category = str(binding["tag_category"])
            code = str(binding["tag_code"])
            tag_id = tag_id_by_key.get((category, code))
            if tag_id is None:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at) VALUES(?,?,?)",
                (tag_id, str(binding["channel_slug"]), now_text),
            )

        for rule in normalized_rules:
            category = str(rule["tag_category"])
            code = str(rule["tag_code"])
            tag_id = tag_id_by_key.get((category, code))
            if tag_id is None:
                continue
            normalized = rules_service._normalize_rule_payload(rule, tag_id=tag_id)
            conn.execute(
                """
                INSERT INTO custom_tag_rules(tag_id, source_path, operator, value_json, match_mode, priority, weight, required, stop_after_match, is_active, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    tag_id,
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

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {"can_confirm": True, **preview["summary"]}


def channel_dashboard(conn: sqlite3.Connection, *, channel_slug: str) -> dict[str, Any]:
    visual_rows = conn.execute(
        """
        SELECT t.id, t.code, t.label, t.description, t.is_active
        FROM custom_tags t
        JOIN custom_tag_channel_bindings b ON b.tag_id = t.id
        WHERE t.category = 'VISUAL' AND b.channel_slug = ?
        ORDER BY t.code ASC
        """,
        (channel_slug,),
    ).fetchall()

    rule_rows = conn.execute(
        """
        SELECT r.id, r.tag_id, t.code AS tag_code, t.category, r.source_path, r.operator, r.value_json, r.priority
        FROM custom_tag_rules r
        JOIN custom_tags t ON t.id = r.tag_id
        WHERE t.category IN ('MOOD', 'THEME') AND r.is_active = 1
        ORDER BY t.category ASC, t.code ASC, r.priority DESC, r.id ASC
        """
    ).fetchall()

    counts_rows = conn.execute(
        """
        SELECT t.id AS tag_id, t.code, t.category, COUNT(DISTINCT tr.id) AS tracks_count
        FROM custom_tags t
        LEFT JOIN track_custom_tag_assignments a ON a.tag_id = t.id AND a.state IN ('AUTO','MANUAL')
        LEFT JOIN tracks tr ON tr.id = a.track_pk AND tr.channel_slug = ?
        GROUP BY t.id, t.code, t.category
        ORDER BY t.category ASC, t.code ASC
        """,
        (channel_slug,),
    ).fetchall()
    counts_by_tag_id = {int(row["tag_id"]): int(row["tracks_count"]) for row in counts_rows}

    visual_tags = [
        {
            "id": int(row["id"]),
            "code": str(row["code"]),
            "label": str(row["label"]),
            "description": row["description"],
            "is_active": bool(row["is_active"]),
            "tracks_count": counts_by_tag_id.get(int(row["id"]), 0),
        }
        for row in visual_rows
    ]
    active_rules = [
        {
            "id": int(row["id"]),
            "tag_id": int(row["tag_id"]),
            "tag_code": str(row["tag_code"]),
            "tag_category": str(row["category"]),
            "source_path": str(row["source_path"]),
            "operator": str(row["operator"]),
            "value_json": str(row["value_json"]),
            "priority": int(row["priority"]),
            "tracks_count": counts_by_tag_id.get(int(row["tag_id"]), 0),
        }
        for row in rule_rows
    ]
    tag_usage = [
        {
            "tag_id": int(row["tag_id"]),
            "tag_code": str(row["code"]),
            "tag_category": str(row["category"]),
            "tracks_count": int(row["tracks_count"]),
        }
        for row in counts_rows
    ]
    return {"channel_slug": channel_slug, "visual_tags": visual_tags, "active_rules": active_rules, "tag_usage": tag_usage}
