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
