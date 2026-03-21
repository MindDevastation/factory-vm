from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
from typing import Any, Dict

from services.common import db as dbm


class MetadataDefaultsError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


_SOURCE_SPECS = {
    "title_template": {
        "get_by_id": dbm.get_title_template_by_id,
        "name_key": "template_name",
        "field_key": "default_title_template_id",
    },
    "description_template": {
        "get_by_id": dbm.get_description_template_by_id,
        "name_key": "template_name",
        "field_key": "default_description_template_id",
    },
    "video_tag_preset": {
        "get_by_id": dbm.get_video_tag_preset_by_id,
        "name_key": "preset_name",
        "field_key": "default_video_tag_preset_id",
    },
}


def read_channel_defaults(conn: sqlite3.Connection, *, channel_slug: str) -> Dict[str, Any]:
    _ensure_channel_exists(conn, channel_slug=channel_slug)
    row = dbm.get_channel_metadata_defaults(conn, channel_slug=channel_slug) or {}
    defaults: Dict[str, Dict[str, Any] | None] = {}
    for source_type, spec in _SOURCE_SPECS.items():
        source_id = _int_or_none(row.get(spec["field_key"]))
        source_row = spec["get_by_id"](conn, source_id) if source_id is not None else None
        defaults[source_type] = _to_ref(source_row, name_key=spec["name_key"]) if source_row else None
    return {"channel_slug": channel_slug, "defaults": defaults}


def update_channel_defaults(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    default_title_template_id: int | None,
    default_description_template_id: int | None,
    default_video_tag_preset_id: int | None,
) -> Dict[str, Any]:
    _ensure_channel_exists(conn, channel_slug=channel_slug)
    ids_by_type = {
        "title_template": default_title_template_id,
        "description_template": default_description_template_id,
        "video_tag_preset": default_video_tag_preset_id,
    }
    validated_rows = {
        source_type: _validate_source(conn, channel_slug=channel_slug, source_type=source_type, source_id=source_id)
        for source_type, source_id in ids_by_type.items()
    }

    existing = dbm.get_channel_metadata_defaults(conn, channel_slug=channel_slug) or {}
    unchanged = all(_int_or_none(existing.get(_SOURCE_SPECS[source_type]["field_key"])) == source_id for source_type, source_id in ids_by_type.items())

    now_iso = _now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        dbm.upsert_channel_metadata_defaults(
            conn,
            channel_slug=channel_slug,
            default_title_template_id=default_title_template_id,
            default_description_template_id=default_description_template_id,
            default_video_tag_preset_id=default_video_tag_preset_id,
            updated_at=now_iso,
            created_at=now_iso,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "channel_slug": channel_slug,
        "defaults_updated": not unchanged,
        "defaults": {
            source_type: (_to_ref(row, name_key=_SOURCE_SPECS[source_type]["name_key"]) if row else None)
            for source_type, row in validated_rows.items()
        },
    }


def _validate_source(conn: sqlite3.Connection, *, channel_slug: str, source_type: str, source_id: int | None) -> Dict[str, Any] | None:
    if source_id is None:
        return None

    spec = _SOURCE_SPECS[source_type]
    row = spec["get_by_id"](conn, source_id)
    if not row:
        for other_type, other_spec in _SOURCE_SPECS.items():
            if other_type != source_type and other_spec["get_by_id"](conn, source_id):
                raise MetadataDefaultsError(code="MDO_DEFAULT_FIELD_TYPE_MISMATCH", message="Wrong source type for field")
        raise MetadataDefaultsError(code="MDO_DEFAULT_SOURCE_NOT_FOUND", message="Default source not found")

    if str(row.get("channel_slug") or "") != channel_slug:
        raise MetadataDefaultsError(code="MDO_DEFAULT_SOURCE_CHANNEL_MISMATCH", message="Default source must belong to the same channel")
    if str(row.get("status") or "") != "ACTIVE":
        raise MetadataDefaultsError(code="MDO_DEFAULT_SOURCE_NOT_ACTIVE", message="Default source must be ACTIVE")
    if row.get("validation_status") is not None and str(row.get("validation_status") or "") != "VALID":
        raise MetadataDefaultsError(code="MDO_DEFAULT_SOURCE_INVALID", message="Default source must be VALID")
    return row


def _ensure_channel_exists(conn: sqlite3.Connection, *, channel_slug: str) -> None:
    if not dbm.get_channel_by_slug(conn, channel_slug):
        raise MetadataDefaultsError(code="MDO_CHANNEL_NOT_FOUND", message="Channel not found")


def _to_ref(row: Dict[str, Any], *, name_key: str) -> Dict[str, Any]:
    return {"id": int(row["id"]), "name": str(row[name_key])}


def _int_or_none(value: Any) -> int | None:
    return int(value) if value is not None else None


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
