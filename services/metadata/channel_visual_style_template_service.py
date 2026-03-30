from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Dict, List

from services.common import db as dbm
from services.metadata.channel_visual_style_template_validator import validate_template_payload


class ChannelVisualStyleTemplateError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def create_channel_visual_style_template(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    template_name: str,
    template_payload: Any,
    make_default: bool,
) -> Dict[str, Any]:
    channel = dbm.get_channel_by_slug(conn, channel_slug)
    if not channel:
        raise ChannelVisualStyleTemplateError(code="CVST_CHANNEL_NOT_FOUND", message="Channel not found")
    if not str(template_name or "").strip():
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NAME_REQUIRED", message="template_name is required")

    validation = validate_template_payload(template_payload)
    if not validation.is_valid:
        first = validation.errors[0]
        raise ChannelVisualStyleTemplateError(code=first["code"], message=first["message"])

    now_iso = _now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        if make_default:
            dbm.unset_active_default_channel_visual_style_template(conn, channel_slug=channel_slug)
        template_id = dbm.create_channel_visual_style_template(
            conn,
            channel_slug=channel_slug,
            template_name=str(template_name).strip(),
            template_payload_json=dbm.json_dumps(validation.normalized_payload),
            status="ACTIVE",
            is_default=make_default,
            validation_status="VALID",
            validation_errors_json=None,
            last_validated_at=now_iso,
            created_at=now_iso,
            updated_at=now_iso,
            archived_at=None,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    row = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    assert row is not None
    return _serialize_template(row)


def list_channel_visual_style_templates(
    conn: sqlite3.Connection,
    *,
    channel_slug: str | None,
    status_filter: str,
    q: str | None,
) -> List[Dict[str, Any]]:
    status: str | None
    if status_filter == "active":
        status = "ACTIVE"
    elif status_filter == "archived":
        status = "ARCHIVED"
    else:
        status = None
    rows = dbm.list_channel_visual_style_templates(conn, channel_slug=channel_slug, status=status, q=(q or None))
    return [_serialize_template(row) for row in rows]


def get_channel_visual_style_template(conn: sqlite3.Connection, *, template_id: int) -> Dict[str, Any]:
    row = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    if not row:
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NOT_FOUND", message="Template not found")
    return _serialize_template(row)


def update_channel_visual_style_template(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    template_name: str | None,
    template_payload: Any | None,
) -> Dict[str, Any]:
    row = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    if not row:
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NOT_FOUND", message="Template not found")

    name_value = str(template_name if template_name is not None else row["template_name"]).strip()
    if not name_value:
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NAME_REQUIRED", message="template_name is required")

    payload_value: Any
    if template_payload is None:
        payload_value = _decode_payload_json(str(row["template_payload_json"]))
    else:
        payload_value = template_payload

    validation = validate_template_payload(payload_value)
    if not validation.is_valid:
        first = validation.errors[0]
        raise ChannelVisualStyleTemplateError(code=first["code"], message=first["message"])

    now_iso = _now_iso()
    ok = dbm.update_channel_visual_style_template_fields(
        conn,
        template_id=template_id,
        template_name=name_value,
        template_payload_json=dbm.json_dumps(validation.normalized_payload),
        validation_status="VALID",
        validation_errors_json=None,
        last_validated_at=now_iso,
        updated_at=now_iso,
    )
    if not ok:
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NOT_FOUND", message="Template not found")

    updated = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    assert updated is not None
    return _serialize_template(updated)


def archive_channel_visual_style_template(conn: sqlite3.Connection, *, template_id: int) -> Dict[str, Any]:
    row = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    if not row:
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NOT_FOUND", message="Template not found")
    if str(row["status"]) == "ARCHIVED":
        return _serialize_template(row)

    now_iso = _now_iso()
    dbm.archive_channel_visual_style_template(conn, template_id=template_id, updated_at=now_iso, archived_at=now_iso)
    updated = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    assert updated is not None
    return _serialize_template(updated)


def activate_channel_visual_style_template(conn: sqlite3.Connection, *, template_id: int) -> Dict[str, Any]:
    row = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    if not row:
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NOT_FOUND", message="Template not found")

    _ensure_template_payload_valid(conn, row=row)
    now_iso = _now_iso()
    dbm.activate_channel_visual_style_template(conn, template_id=template_id, updated_at=now_iso)
    dbm.set_channel_visual_style_template_default_flag(conn, template_id=template_id, is_default=False, updated_at=now_iso)

    updated = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    assert updated is not None
    return _serialize_template(updated)


def set_default_channel_visual_style_template(conn: sqlite3.Connection, *, template_id: int) -> Dict[str, Any]:
    row = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    if not row:
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NOT_FOUND", message="Template not found")
    if str(row["status"]) != "ACTIVE":
        raise ChannelVisualStyleTemplateError(
            code="CVST_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_DEFAULT",
            message="Archived template cannot be default",
        )

    _ensure_template_payload_valid(conn, row=row)

    now_iso = _now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        dbm.unset_active_default_channel_visual_style_template(conn, channel_slug=str(row["channel_slug"]))
        dbm.set_channel_visual_style_template_default_flag(conn, template_id=template_id, is_default=True, updated_at=now_iso)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    updated = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    assert updated is not None
    return _serialize_template(updated)


def set_release_visual_style_template_override(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    template_id: int,
) -> Dict[str, Any]:
    rows = dbm.resolve_release_visual_style_template_rows(conn, release_id=release_id)
    release_row = rows["release"]
    if not release_row:
        raise ChannelVisualStyleTemplateError(code="CVST_RELEASE_NOT_FOUND", message="Release not found")

    template_row = dbm.get_channel_visual_style_template_by_id(conn, template_id)
    if not template_row:
        raise ChannelVisualStyleTemplateError(code="CVST_TEMPLATE_NOT_FOUND", message="Template not found")
    if str(template_row["status"]) != "ACTIVE":
        raise ChannelVisualStyleTemplateError(
            code="CVST_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_OVERRIDE",
            message="Archived template cannot be used as release override",
        )

    if str(template_row["channel_slug"]) != str(release_row["channel_slug"]):
        raise ChannelVisualStyleTemplateError(
            code="CVST_TEMPLATE_CHANNEL_MISMATCH",
            message="Template channel does not match release channel",
        )

    now_iso = _now_iso()
    dbm.upsert_release_visual_style_template_override(
        conn,
        release_id=release_id,
        template_id=template_id,
        created_at=now_iso,
        updated_at=now_iso,
    )
    out = get_release_visual_style_template_override(conn, release_id=release_id)
    assert out is not None
    return out


def clear_release_visual_style_template_override(conn: sqlite3.Connection, *, release_id: int) -> Dict[str, Any]:
    rows = dbm.resolve_release_visual_style_template_rows(conn, release_id=release_id)
    release_row = rows["release"]
    if not release_row:
        raise ChannelVisualStyleTemplateError(code="CVST_RELEASE_NOT_FOUND", message="Release not found")
    cleared = dbm.clear_release_visual_style_template_override(conn, release_id=release_id)
    return {"release_id": int(release_id), "cleared": bool(cleared)}


def get_release_visual_style_template_override(conn: sqlite3.Connection, *, release_id: int) -> Dict[str, Any] | None:
    row = dbm.get_release_visual_style_template_override_by_release_id(conn, release_id=release_id)
    if not row:
        return None
    template = dbm.get_channel_visual_style_template_by_id(conn, int(row["template_id"]))
    if not template:
        return None
    return {
        "release_id": int(row["release_id"]),
        "template_id": int(row["template_id"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
        "template": _serialize_template(template),
    }


def resolve_effective_channel_visual_style_template_for_release(conn: sqlite3.Connection, *, release_id: int) -> Dict[str, Any]:
    rows = dbm.resolve_release_visual_style_template_rows(conn, release_id=release_id)
    release_row = rows["release"]
    if not release_row:
        raise ChannelVisualStyleTemplateError(code="CVST_RELEASE_NOT_FOUND", message="Release not found")

    override_meta = dbm.get_release_visual_style_template_override_by_release_id(conn, release_id=release_id)
    override_row = rows["override"]
    default_row = rows["default"]

    source = "none"
    effective_template: Dict[str, Any] | None = None
    is_override = False
    if override_row and str(override_row["status"]) == "ACTIVE" and str(override_row["channel_slug"]) == str(release_row["channel_slug"]):
        source = "release_override"
        is_override = True
        effective_template = _serialize_template(override_row)
    elif default_row:
        source = "channel_default"
        effective_template = _serialize_template(default_row)

    return {
        "release_id": int(release_row["release_id"]),
        "channel_slug": str(release_row["channel_slug"]),
        "source": source,
        "is_override": is_override,
        "has_override": bool(override_meta is not None),
        "effective_template": effective_template,
        "override_template_id": int(override_meta["template_id"]) if override_meta else None,
        "default_template_id": int(default_row["id"]) if default_row else None,
    }


def _ensure_template_payload_valid(conn: sqlite3.Connection, *, row: Dict[str, Any]) -> None:
    payload = _decode_payload_json(str(row["template_payload_json"]))
    validation = validate_template_payload(payload)
    now_iso = _now_iso()
    if not validation.is_valid:
        dbm.update_channel_visual_style_template_fields(
            conn,
            template_id=int(row["id"]),
            template_name=str(row["template_name"]),
            template_payload_json=str(row["template_payload_json"]),
            validation_status="INVALID",
            validation_errors_json=dbm.json_dumps(list(validation.errors)),
            last_validated_at=now_iso,
            updated_at=now_iso,
        )
        first = validation.errors[0]
        raise ChannelVisualStyleTemplateError(code=first["code"], message=first["message"])

    dbm.update_channel_visual_style_template_fields(
        conn,
        template_id=int(row["id"]),
        template_name=str(row["template_name"]),
        template_payload_json=dbm.json_dumps(validation.normalized_payload),
        validation_status="VALID",
        validation_errors_json=None,
        last_validated_at=now_iso,
        updated_at=now_iso,
    )


def _decode_payload_json(payload_json: str) -> Any:
    try:
        return json.loads(payload_json)
    except Exception as exc:
        raise ChannelVisualStyleTemplateError(code="CVST_PAYLOAD_TYPE", message="template_payload must be an object") from exc


def _serialize_template(row: Dict[str, Any]) -> Dict[str, Any]:
    validation_errors: List[Dict[str, str]] = []
    if row.get("validation_errors_json"):
        with _suppress_decode_errors():
            loaded = json.loads(str(row["validation_errors_json"]))
            if isinstance(loaded, list):
                validation_errors = [item for item in loaded if isinstance(item, dict)]

    return {
        "id": int(row["id"]),
        "channel_slug": str(row["channel_slug"]),
        "template_name": str(row["template_name"]),
        "template_payload": _decode_payload_json(str(row["template_payload_json"])),
        "status": str(row["status"]),
        "is_default": bool(int(row.get("is_default") or 0)),
        "validation_status": str(row["validation_status"]),
        "validation_errors": validation_errors,
        "last_validated_at": row.get("last_validated_at"),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
        "archived_at": row.get("archived_at"),
    }


class _suppress_decode_errors:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, _tb) -> bool:
        return exc is not None and issubclass(exc_type, (ValueError, TypeError))


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
