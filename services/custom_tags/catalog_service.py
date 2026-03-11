from __future__ import annotations

import json
import re
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CATEGORY_TO_FILE = {
    "VISUAL": "visual_tags.json",
    "MOOD": "mood_tags.json",
    "THEME": "theme_tags.json",
}
VALID_CATEGORIES = set(CATEGORY_TO_FILE.keys())
SEED_SCHEMA_VERSION = "custom_tags_seed/1"
SLUG_RE = re.compile(r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$")


@dataclass
class CatalogError(Exception):
    code: str
    message: str
    status_code: int
    details: dict[str, Any] | None = None


class TagNotFoundError(CatalogError):
    def __init__(self, tag_id: int):
        super().__init__(
            code="CTA_TAG_NOT_FOUND",
            message="custom tag not found",
            status_code=404,
            details={"tag_id": tag_id},
        )


class InvalidInputError(CatalogError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(code="CTA_INVALID_INPUT", message=message, status_code=400, details=details)


class SeedInvalidJsonError(CatalogError):
    def __init__(self, file_name: str, message: str):
        super().__init__(
            code="CTA_SEED_INVALID_JSON",
            message=message,
            status_code=400,
            details={"file": file_name},
        )


class SeedValidationError(CatalogError):
    def __init__(self, file_name: str, message: str):
        super().__init__(
            code="CTA_SEED_VALIDATION_FAILED",
            message=message,
            status_code=400,
            details={"file": file_name},
        )


class SeedNotFoundError(CatalogError):
    def __init__(self, file_name: str):
        super().__init__(
            code="CTS_SEED_NOT_FOUND",
            message=f"required seed file is missing: {file_name}",
            status_code=400,
            details={"file": file_name},
        )


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_nonempty(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise InvalidInputError(f"{field} must be a string", {"field": field})
    out = value.strip()
    if not out:
        raise InvalidInputError(f"{field} must not be empty", {"field": field})
    return out


def _normalize_description(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidInputError("description must be a string or null", {"field": "description"})
    return value


def _normalize_bool(value: Any, field: str = "is_active") -> int:
    if not isinstance(value, bool):
        raise InvalidInputError(f"{field} must be a boolean", {"field": field})
    return 1 if value else 0


def _tag_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "code": str(row["code"]),
        "label": str(row["label"]),
        "category": str(row["category"]),
        "description": row.get("description"),
        "is_active": bool(row.get("is_active", 0)),
    }


def list_catalog(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, code, label, category, description, is_active
        FROM custom_tags
        ORDER BY category ASC, code ASC, id ASC
        """
    ).fetchall()
    return [_tag_row_to_dict(row) for row in rows]


def _format_rule_value(value_json: str) -> str:
    try:
        parsed = json.loads(value_json)
    except json.JSONDecodeError:
        return value_json
    if isinstance(parsed, str):
        return parsed
    if isinstance(parsed, bool):
        return "true" if parsed else "false"
    if parsed is None:
        return "null"
    return str(parsed)


def _format_rule_condition(row: dict[str, Any]) -> str:
    field = str(row["source_path"]).split(".")[-1]
    value = _format_rule_value(str(row["value_json"]))
    operator = str(row["operator"])
    if operator == "equals":
        return f"{field}={value}"
    if operator == "not_equals":
        return f"{field}!={value}"
    op_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
    if operator in op_map:
        return f"{field}{op_map[operator]}{value}"
    return f"{field} {operator} {value}"


def build_rules_summary(rules: list[dict[str, Any]]) -> str:
    count = len(rules)
    if count == 0:
        return "No rules"
    conditions = [_format_rule_condition(rule) for rule in rules[:3]]
    suffix = "" if count <= 3 else "; …"
    return f"{count} active rules: {'; '.join(conditions)}{suffix}"


def list_custom_tags_enriched(
    conn: sqlite3.Connection,
    *,
    category: str | None = None,
    tag_id: int | None = None,
    q: str | None = None,
    include_bindings: bool = True,
    include_rules_summary: bool = True,
    include_usage: bool = False,
) -> list[dict[str, Any]]:
    if category is not None and category not in VALID_CATEGORIES:
        raise InvalidInputError("category must be one of VISUAL, MOOD, THEME", {"field": "category"})

    where: list[str] = []
    params: list[Any] = []
    if category is not None:
        where.append("category = ?")
        params.append(category)
    if tag_id is not None:
        where.append("id = ?")
        params.append(tag_id)
    if q:
        needle = f"%{q.strip().lower()}%"
        where.append("(LOWER(code) LIKE ? OR LOWER(label) LIKE ?)")
        params.extend([needle, needle])

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    tags = conn.execute(
        f"""
        SELECT id, code, label, category, description, is_active
        FROM custom_tags
        {where_sql}
        ORDER BY category ASC, code ASC, id ASC
        """,
        tuple(params),
    ).fetchall()

    tag_ids = [int(row["id"]) for row in tags]
    bindings_by_tag: dict[int, list[dict[str, Any]]] = {}
    rules_by_tag: dict[int, list[dict[str, Any]]] = {}

    if include_bindings and tag_ids:
        placeholders = ",".join("?" for _ in tag_ids)
        rows = conn.execute(
            f"""
            SELECT id, tag_id, channel_slug
            FROM custom_tag_channel_bindings
            WHERE tag_id IN ({placeholders})
            ORDER BY id ASC
            """,
            tuple(tag_ids),
        ).fetchall()
        for row in rows:
            row_tag_id = int(row["tag_id"])
            bindings_by_tag.setdefault(row_tag_id, []).append(
                {
                    "id": int(row["id"]),
                    "tag_id": row_tag_id,
                    "channel_slug": str(row["channel_slug"]),
                }
            )

    if include_rules_summary and tag_ids:
        placeholders = ",".join("?" for _ in tag_ids)
        rows = conn.execute(
            f"""
            SELECT id, tag_id, source_path, operator, value_json, priority
            FROM custom_tag_rules
            WHERE tag_id IN ({placeholders}) AND is_active = 1
            ORDER BY tag_id ASC, priority DESC, id ASC
            """,
            tuple(tag_ids),
        ).fetchall()
        for row in rows:
            row_tag_id = int(row["tag_id"])
            rules_by_tag.setdefault(row_tag_id, []).append(dict(row))

    usage_by_tag: dict[int, dict[str, int]] = {}
    if include_usage and tag_ids:
        placeholders = ",".join("?" for _ in tag_ids)
        binding_rows = conn.execute(
            f"""
            SELECT tag_id, COUNT(DISTINCT channel_slug) AS channels_count
            FROM custom_tag_channel_bindings
            WHERE tag_id IN ({placeholders})
            GROUP BY tag_id
            """,
            tuple(tag_ids),
        ).fetchall()
        for row in binding_rows:
            usage_by_tag.setdefault(int(row["tag_id"]), {})["channels_count"] = int(row["channels_count"])

        rule_rows = conn.execute(
            f"""
            SELECT tag_id, COUNT(*) AS rules_count
            FROM custom_tag_rules
            WHERE tag_id IN ({placeholders}) AND is_active = 1
            GROUP BY tag_id
            """,
            tuple(tag_ids),
        ).fetchall()
        for row in rule_rows:
            usage_by_tag.setdefault(int(row["tag_id"]), {})["rules_count"] = int(row["rules_count"])

        track_rows = conn.execute(
            f"""
            SELECT tag_id, COUNT(DISTINCT track_pk) AS tracks_count
            FROM track_custom_tag_assignments
            WHERE tag_id IN ({placeholders}) AND state IN ('AUTO', 'MANUAL')
            GROUP BY tag_id
            """,
            tuple(tag_ids),
        ).fetchall()
        for row in track_rows:
            usage_by_tag.setdefault(int(row["tag_id"]), {})["tracks_count"] = int(row["tracks_count"])

    payload: list[dict[str, Any]] = []
    for row in tags:
        item = _tag_row_to_dict(dict(row))
        item["bindings"] = bindings_by_tag.get(item["id"], []) if item["category"] == "VISUAL" and include_bindings else []
        active_rules = rules_by_tag.get(item["id"], []) if include_rules_summary else []
        item["rules_count"] = len(active_rules)
        item["rules_summary"] = build_rules_summary(active_rules) if include_rules_summary else "No rules"
        if include_usage:
            item["usage"] = {
                "channels_count": int(usage_by_tag.get(item["id"], {}).get("channels_count", 0)),
                "rules_count": int(usage_by_tag.get(item["id"], {}).get("rules_count", 0)),
                "tracks_count": int(usage_by_tag.get(item["id"], {}).get("tracks_count", 0)),
            }
        payload.append(item)
    return payload


def create_tag(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    code = _normalize_nonempty(payload.get("code"), "code")
    label = _normalize_nonempty(payload.get("label"), "label")
    category = _normalize_nonempty(payload.get("category"), "category").upper()
    if category not in VALID_CATEGORIES:
        raise InvalidInputError("category must be one of VISUAL, MOOD, THEME", {"field": "category"})
    description = _normalize_description(payload.get("description"))
    is_active = _normalize_bool(payload.get("is_active", True))
    now_text = _now_text()

    try:
        cur = conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (code, label, category, description, is_active, now_text, now_text),
        )
    except sqlite3.IntegrityError as exc:
        raise InvalidInputError("tag with same category and code already exists", {"category": category, "code": code}) from exc

    row = conn.execute(
        "SELECT id, code, label, category, description, is_active FROM custom_tags WHERE id = ?",
        (int(cur.lastrowid),),
    ).fetchone()
    assert row is not None
    return _tag_row_to_dict(row)


def update_tag(conn: sqlite3.Connection, tag_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {"code", "label", "description", "is_active"}
    extra = sorted(set(payload.keys()) - allowed)
    if extra:
        raise InvalidInputError("unknown fields in patch", {"fields": extra})
    if "category" in payload:
        raise InvalidInputError("category is not editable", {"field": "category"})

    existing = conn.execute(
        "SELECT id, code, label, category, description, is_active FROM custom_tags WHERE id = ?",
        (tag_id,),
    ).fetchone()
    if existing is None:
        raise TagNotFoundError(tag_id)

    code = _normalize_nonempty(payload["code"], "code") if "code" in payload else existing["code"]
    label = _normalize_nonempty(payload["label"], "label") if "label" in payload else existing["label"]
    description = _normalize_description(payload["description"]) if "description" in payload else existing["description"]
    is_active = _normalize_bool(payload["is_active"]) if "is_active" in payload else int(existing["is_active"])

    try:
        conn.execute(
            """
            UPDATE custom_tags
            SET code = ?, label = ?, description = ?, is_active = ?, updated_at = ?
            WHERE id = ?
            """,
            (code, label, description, is_active, _now_text(), tag_id),
        )
    except sqlite3.IntegrityError as exc:
        raise InvalidInputError("tag with same category and code already exists", {"category": existing["category"], "code": code}) from exc

    updated = conn.execute(
        "SELECT id, code, label, category, description, is_active FROM custom_tags WHERE id = ?",
        (tag_id,),
    ).fetchone()
    assert updated is not None
    return _tag_row_to_dict(updated)


def _validate_seed_text(value: Any, *, field: str, max_len: int, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise InvalidInputError(f"{field} must be a string", {"field": field})
    out = value.strip()
    if not allow_empty and not out:
        raise InvalidInputError(f"{field} must not be empty", {"field": field})
    if len(out) > max_len:
        raise InvalidInputError(f"{field} must be at most {max_len} characters", {"field": field, "max_len": max_len})
    return out


def _validate_slug(value: Any, *, field: str) -> str:
    slug = _validate_seed_text(value, field=field, max_len=100)
    if not SLUG_RE.fullmatch(slug):
        raise InvalidInputError(
            f"{field} must match ^[a-z0-9]+(?:[-_][a-z0-9]+)*$",
            {"field": field, "pattern": SLUG_RE.pattern},
        )
    return slug


def _validate_seed_category(payload: dict[str, Any], *, category: str, file_name: str) -> None:
    if payload.get("schema_version") != SEED_SCHEMA_VERSION:
        raise SeedValidationError(file_name, f"schema_version must be {SEED_SCHEMA_VERSION}")
    if payload.get("category") != category:
        raise SeedValidationError(file_name, f"category must be {category}")


def _load_seed_file(path: Path, category: str) -> list[dict[str, Any]]:
    file_name = path.name
    if not path.is_file():
        raise SeedNotFoundError(file_name)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SeedInvalidJsonError(file_name, f"invalid JSON in {file_name}: {exc.msg}") from exc

    if not isinstance(payload, dict) or not isinstance(payload.get("tags"), list):
        raise SeedValidationError(file_name, "payload must be an object with a tags array")
    _validate_seed_category(payload, category=category, file_name=file_name)

    out: list[dict[str, Any]] = []
    for idx, item in enumerate(payload["tags"]):
        if not isinstance(item, dict):
            raise SeedValidationError(file_name, f"tags[{idx}] must be an object")
        try:
            code = _validate_slug(item.get("slug"), field=f"tags[{idx}].slug")
            label = _validate_seed_text(item.get("name"), field=f"tags[{idx}].name", max_len=100)
            description_raw = item.get("description")
            if description_raw is None:
                description = None
            else:
                description = _validate_seed_text(
                    description_raw,
                    field=f"tags[{idx}].description",
                    max_len=500,
                )
            is_active = _normalize_bool(item.get("is_active", True), field=f"tags[{idx}].is_active")
        except InvalidInputError as exc:
            raise SeedValidationError(file_name, exc.message) from exc
        out.append(
            {
                "category": category,
                "code": code,
                "label": label,
                "description": description,
                "is_active": is_active,
            }
        )
    return out


def import_catalog(conn: sqlite3.Connection, seed_dir: str) -> dict[str, Any]:
    seed_root = Path(seed_dir)
    imported = 0
    created = 0
    updated = 0
    now_text = _now_text()

    for category, filename in CATEGORY_TO_FILE.items():
        tags = _load_seed_file(seed_root / filename, category)
        for tag in tags:
            imported += 1
            row = conn.execute(
                "SELECT id FROM custom_tags WHERE category = ? AND code = ?",
                (tag["category"], tag["code"]),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (tag["code"], tag["label"], tag["category"], tag["description"], tag["is_active"], now_text, now_text),
                )
                created += 1
            else:
                conn.execute(
                    """
                    UPDATE custom_tags
                    SET label = ?, description = ?, is_active = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (tag["label"], tag["description"], tag["is_active"], now_text, int(row["id"])),
                )
                updated += 1

    return {"imported": imported, "created": created, "updated": updated}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=str(path.parent), delete=False) as fh:
        tmp_path = Path(fh.name)
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    tmp_path.replace(path)


def export_catalog(conn: sqlite3.Connection, seed_dir: str) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT code, label, category, description, is_active FROM custom_tags ORDER BY code ASC, id ASC"
    ).fetchall()
    by_category: dict[str, list[dict[str, Any]]] = {k: [] for k in CATEGORY_TO_FILE}
    for row in rows:
        category = str(row["category"])
        if category not in by_category:
            continue
        by_category[category].append(
            {
                "slug": str(row["code"]),
                "name": str(row["label"]),
                "description": row.get("description"),
                "is_active": bool(row.get("is_active", 0)),
            }
        )

    seed_root = Path(seed_dir)
    seed_root.mkdir(parents=True, exist_ok=True)
    exported_at = _now_text()
    staged: list[tuple[Path, dict[str, Any]]] = []
    for category, filename in CATEGORY_TO_FILE.items():
        staged.append(
            (
                seed_root / filename,
                {
                    "schema_version": SEED_SCHEMA_VERSION,
                    "category": category,
                    "exported_at": exported_at,
                    "tags": by_category[category],
                },
            )
        )
    for file_path, payload in staged:
        _write_json_atomic(file_path, payload)

    return {"exported": len(rows), "files": list(CATEGORY_TO_FILE.values())}


def _normalize_bulk_item(item: dict[str, Any], *, index: int) -> dict[str, Any]:
    category = _normalize_nonempty(item.get("category"), f"items[{index}].category").upper()
    if category not in VALID_CATEGORIES:
        raise InvalidInputError(
            "category must be one of VISUAL, MOOD, THEME",
            {"field": f"items[{index}].category"},
        )
    return {
        "category": category,
        "code": _validate_slug(item.get("slug"), field=f"items[{index}].slug"),
        "label": _validate_seed_text(item.get("name"), field=f"items[{index}].name", max_len=100),
        "description": (
            None
            if item.get("description") is None
            else _validate_seed_text(
                item.get("description"),
                field=f"items[{index}].description",
                max_len=500,
                allow_empty=True,
            )
        ),
        "is_active": _normalize_bool(item.get("is_active", True), field=f"items[{index}].is_active"),
    }


def preview_bulk_custom_tags(conn: sqlite3.Connection, items: list[dict[str, Any]]) -> dict[str, Any]:
    existing_rows = conn.execute(
        "SELECT category, code, label, description, is_active FROM custom_tags"
    ).fetchall()
    existing: dict[tuple[str, str], dict[str, Any]] = {
        (str(row["category"]), str(row["code"])): {
            "label": str(row["label"]),
            "description": row["description"],
            "is_active": int(row["is_active"]),
        }
        for row in existing_rows
    }

    seen: dict[tuple[str, str], tuple[int, dict[str, Any]]] = {}
    item_results: list[dict[str, Any]] = []
    valid_unique = 0
    errors = 0
    duplicates = 0
    upserts_against_db = 0

    for idx, raw in enumerate(items):
        item_result: dict[str, Any] = {
            "index": idx,
            "action": "error",
            "normalized": None,
            "warnings": [],
            "errors": [],
        }
        try:
            normalized = _normalize_bulk_item(raw, index=idx)
            key = (normalized["category"], normalized["code"])
            item_result["normalized"] = {
                "category": normalized["category"],
                "slug": normalized["code"],
                "name": normalized["label"],
                "description": normalized["description"],
                "is_active": bool(normalized["is_active"]),
            }

            prev = seen.get(key)
            if prev is not None:
                _prev_idx, prev_normalized = prev
                if prev_normalized != normalized:
                    errors += 1
                    item_result["errors"].append(
                        {
                            "code": "CONFLICTING_DUPLICATE_KEY",
                            "message": "conflicting payload values for category+slug",
                            "details": {
                                "category": normalized["category"],
                                "slug": normalized["code"],
                            },
                        }
                    )
                else:
                    duplicates += 1
                    item_result["action"] = "deduplicated"
                    item_result["warnings"].append(
                        {
                            "code": "DUPLICATE_IN_PAYLOAD",
                            "message": "duplicate category+slug with identical normalized payload; deduplicated",
                        }
                    )
                item_results.append(item_result)
                continue

            seen[key] = (idx, normalized)
            existing_row = existing.get(key)
            if existing_row is None:
                item_result["action"] = "insert"
            else:
                upserts_against_db += 1
                if (
                    existing_row["label"] == normalized["label"]
                    and existing_row["description"] == normalized["description"]
                    and int(existing_row["is_active"]) == int(normalized["is_active"])
                ):
                    item_result["action"] = "unchanged"
                else:
                    item_result["action"] = "update"
            valid_unique += 1
        except CatalogError as err:
            errors += 1
            item_result["errors"].append({"code": err.code, "message": err.message, "details": err.details or {}})
        item_results.append(item_result)

    return {
        "can_confirm": errors == 0,
        "summary": {
            "total": len(items),
            "valid": valid_unique,
            "errors": errors,
            "duplicates_in_payload": duplicates,
            "upserts_against_db": upserts_against_db,
        },
        "items": item_results,
    }


def confirm_bulk_custom_tags(conn: sqlite3.Connection, items: list[dict[str, Any]]) -> dict[str, Any]:
    preview = preview_bulk_custom_tags(conn, items)
    if not preview["can_confirm"]:
        return {
            "can_confirm": False,
            "summary": preview["summary"],
            "items": preview["items"],
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
        }

    inserted = 0
    updated = 0
    unchanged = 0
    now_text = _now_text()
    to_apply = [item for item in preview["items"] if item["action"] in {"insert", "update", "unchanged"}]

    conn.execute("BEGIN IMMEDIATE")
    try:
        for item in to_apply:
            normalized = item["normalized"]
            assert isinstance(normalized, dict)
            category = str(normalized["category"])
            code = str(normalized["slug"])
            label = str(normalized["name"])
            description = normalized["description"]
            is_active = _normalize_bool(normalized["is_active"])  # bool -> int

            row = conn.execute(
                "SELECT id, label, description, is_active FROM custom_tags WHERE category = ? AND code = ?",
                (category, code),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (code, label, category, description, is_active, now_text, now_text),
                )
                inserted += 1
                continue

            if (
                str(row["label"]) == label
                and row["description"] == description
                and int(row["is_active"]) == int(is_active)
            ):
                unchanged += 1
                continue

            conn.execute(
                """
                UPDATE custom_tags
                SET label = ?, description = ?, is_active = ?, updated_at = ?
                WHERE id = ?
                """,
                (label, description, is_active, now_text, int(row["id"])),
            )
            updated += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "can_confirm": True,
        "summary": preview["summary"],
        "items": preview["items"],
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
    }
