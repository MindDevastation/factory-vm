from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import sqlite3
import uuid
from typing import Any, Dict, List

from services.common import db as dbm
from services.metadata import description_template_service, descriptiongen_service, title_template_service, titlegen_service, video_tagsgen_service

ALL_FIELDS = ("title", "description", "tags")


class MetadataPreviewApplyError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class PreviewContextResult:
    release_id: int
    channel_slug: str
    current: Dict[str, Any]
    defaults: Dict[str, Any]
    active_sources: Dict[str, Any]


def load_preview_apply_context(conn: sqlite3.Connection, *, release_id: int) -> PreviewContextResult:
    release = _load_release(conn, release_id=release_id)
    tags_ctx = video_tagsgen_service.load_video_tags_context(conn, release_id=release_id)
    usable_title_templates = _list_usable_title_templates(conn, channel_slug=str(release["channel_slug"]))
    usable_description_templates = _list_usable_description_templates(conn, channel_slug=str(release["channel_slug"]))
    default_title_template = _find_default_source(usable_title_templates)
    default_description_template = _find_default_source(usable_description_templates)

    return PreviewContextResult(
        release_id=int(release["id"]),
        channel_slug=str(release["channel_slug"]),
        current={
            "title": str(release.get("title") or ""),
            "description": str(release.get("description") or ""),
            "tags_json": list(tags_ctx.current_tags_json),
        },
        defaults={
            "title_template": default_title_template,
            "description_template": default_description_template,
            "video_tag_preset": _template_source_item(tags_ctx.default_preset, name_key="preset_name"),
        },
        active_sources={
            "title_templates": usable_title_templates,
            "description_templates": usable_description_templates,
            "video_tag_presets": list(tags_ctx.active_presets),
        },
    )


def create_preview_session(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    requested_fields: List[str] | None,
    sources: Dict[str, Any],
    created_by: str | None,
    ttl_seconds: int,
) -> Dict[str, Any]:
    context = load_preview_apply_context(conn, release_id=release_id)
    fields = _normalize_requested_fields(requested_fields)

    session_id = uuid.uuid4().hex
    created_at = _now_iso()
    expires_at = _future_iso(ttl_seconds)

    field_records: Dict[str, Dict[str, Any]] = {}
    resolved_sources: Dict[str, Any] = {}
    dependency_fingerprints: Dict[str, Any] = {}
    field_statuses: Dict[str, str] = {}
    warnings: List[str] = []
    errors: List[str] = []

    title_rec = _build_not_requested_record(context.current["title"])
    description_rec = _build_not_requested_record(context.current["description"])
    tags_rec = _build_not_requested_record(list(context.current["tags_json"]))

    if "title" in fields:
        title_rec, title_source, title_fingerprint = _prepare_title_field(
            conn,
            release_id=release_id,
            channel_slug=context.channel_slug,
            current_value=str(context.current["title"]),
            template_id=sources.get("title_template_id"),
        )
        resolved_sources["title"] = title_source
        dependency_fingerprints["title"] = title_fingerprint
    if "description" in fields:
        description_rec, description_source, description_fingerprint = _prepare_description_field(
            conn,
            release_id=release_id,
            channel_slug=context.channel_slug,
            current_value=str(context.current["description"]),
            template_id=sources.get("description_template_id"),
        )
        resolved_sources["description"] = description_source
        dependency_fingerprints["description"] = description_fingerprint
    if "tags" in fields:
        tags_rec, tags_source, tags_fingerprint = _prepare_tags_field(
            conn,
            release_id=release_id,
            channel_slug=context.channel_slug,
            current_value=list(context.current["tags_json"]),
            preset_id=sources.get("video_tag_preset_id"),
        )
        resolved_sources["tags"] = tags_source
        dependency_fingerprints["tags"] = tags_fingerprint

    field_records["title"] = title_rec
    field_records["description"] = description_rec
    field_records["tags"] = tags_rec

    for field_name, rec in field_records.items():
        field_statuses[field_name] = str(rec["status"])
        warnings.extend(rec["warnings"])
        errors.extend(rec["errors"])

    requested_list = [f for f in ALL_FIELDS if f in fields]
    prepared_fields = [f for f in requested_list if field_statuses[f] in {"PROPOSED_READY", "NO_CHANGE", "OVERWRITE_READY", "CURRENT_ONLY"}]
    applyable_fields = [f for f in requested_list if field_statuses[f] in {"PROPOSED_READY", "OVERWRITE_READY"}]
    failed_fields = [f for f in requested_list if field_statuses[f] in {"GENERATION_FAILED", "CONFIGURATION_MISSING"}]

    payload = {
        "session_id": session_id,
        "release_id": context.release_id,
        "channel_slug": context.channel_slug,
        "session_status": "OPEN",
        "expires_at": expires_at,
        "current": context.current,
        "fields": {
            "title": title_rec,
            "description": description_rec,
            "tags": tags_rec,
        },
        "summary": {
            "requested_fields": requested_list,
            "prepared_fields": prepared_fields,
            "applyable_fields": applyable_fields,
            "failed_fields": failed_fields,
        },
    }

    fields_snapshot = {
        "title": title_rec,
        "description": description_rec,
        "tags": tags_rec,
    }

    dbm.insert_metadata_preview_session(
        conn,
        session_id=session_id,
        release_id=context.release_id,
        channel_slug=context.channel_slug,
        session_status="OPEN",
        requested_fields_json=dbm.json_dumps(requested_list),
        current_bundle_json=dbm.json_dumps(context.current),
        proposed_bundle_json=dbm.json_dumps({
            "title": title_rec.get("proposed_value"),
            "description": description_rec.get("proposed_value"),
            "tags_json": tags_rec.get("proposed_value"),
        }),
        sources_json=dbm.json_dumps(resolved_sources),
        field_statuses_json=dbm.json_dumps(field_statuses),
        dependency_fingerprints_json=dbm.json_dumps(dependency_fingerprints),
        warnings_json=dbm.json_dumps(warnings),
        errors_json=dbm.json_dumps(errors),
        fields_snapshot_json=dbm.json_dumps(fields_snapshot),
        created_by=created_by,
        created_at=created_at,
        expires_at=expires_at,
        applied_at=None,
    )
    conn.commit()
    return payload


def _prepare_title_field(conn: sqlite3.Connection, *, release_id: int, channel_slug: str, current_value: str, template_id: Any):
    resolved_template_id = _as_int_or_none(template_id)
    if template_id is None:
        default_row = _find_default_source(_list_usable_title_templates(conn, channel_slug=channel_slug))
        if default_row is None:
            return _configuration_missing_record(current_value), {"type": "default", "id": None}, None
        resolved_template_id = int(default_row["id"])
    try:
        result = titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=resolved_template_id)
        proposed = result.proposed_title
        normalized_current = title_template_service.normalize_whitespace(current_value)
        normalized_proposed = title_template_service.normalize_whitespace(proposed)
        status, changed, overwrite_required = _diff_status(normalized_current, normalized_proposed)
        return {
            "status": status,
            "current_value": current_value,
            "proposed_value": proposed,
            "changed": changed,
            "overwrite_required": overwrite_required,
            "source": result.used_template,
            "warnings": list(result.warnings),
            "errors": [],
        }, result.used_template, result.generation_fingerprint
    except titlegen_service.TitleGenError as exc:
        return _generation_failed_record(current_value, code=exc.code, message=exc.message), {"type": "explicit" if template_id is not None else "default", "id": template_id}, None


def _prepare_description_field(conn: sqlite3.Connection, *, release_id: int, channel_slug: str, current_value: str, template_id: Any):
    resolved_template_id = _as_int_or_none(template_id)
    if template_id is None:
        default_row = _find_default_source(_list_usable_description_templates(conn, channel_slug=channel_slug))
        if default_row is None:
            return _configuration_missing_record(current_value), {"type": "default", "id": None}, None
        resolved_template_id = int(default_row["id"])
    try:
        result = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=resolved_template_id)
        proposed = result.proposed_description
        normalized_current = description_template_service.normalize_multiline(current_value)
        normalized_proposed = description_template_service.normalize_multiline(proposed)
        status, changed, overwrite_required = _diff_status(normalized_current, normalized_proposed)
        return {
            "status": status,
            "current_value": current_value,
            "proposed_value": proposed,
            "changed": changed,
            "overwrite_required": overwrite_required,
            "source": result.used_template,
            "warnings": list(result.warnings),
            "errors": [],
        }, result.used_template, result.generation_fingerprint
    except descriptiongen_service.DescriptionGenError as exc:
        return _generation_failed_record(current_value, code=exc.code, message=exc.message), {"type": "explicit" if template_id is not None else "default", "id": template_id}, None


def _prepare_tags_field(conn: sqlite3.Connection, *, release_id: int, channel_slug: str, current_value: List[str], preset_id: Any):
    if preset_id is None:
        ctx = video_tagsgen_service.load_video_tags_context(conn, release_id=release_id)
        if ctx.default_preset is None:
            return _configuration_missing_record(current_value), {"type": "default", "id": None}, None
    try:
        result = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=_as_int_or_none(preset_id))
        proposed = list(result.proposed_tags_json)
        normalized_current = list(result.current_tags_json)
        normalized_proposed = list(result.proposed_tags_json)
        status, changed, overwrite_required = _diff_status(normalized_current, normalized_proposed)
        return {
            "status": status,
            "current_value": list(result.current_tags_json),
            "proposed_value": proposed,
            "changed": changed,
            "overwrite_required": overwrite_required,
            "source": result.used_preset,
            "warnings": list(result.warnings),
            "errors": [],
        }, result.used_preset, result.generation_fingerprint
    except video_tagsgen_service.VideoTagsGenError as exc:
        return _generation_failed_record(current_value, code=exc.code, message=exc.message), {"type": "explicit" if preset_id is not None else "default", "id": preset_id}, None


def _load_release(conn: sqlite3.Connection, *, release_id: int) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT r.id, r.title, r.description, r.tags_json, c.slug AS channel_slug
        FROM releases r
        JOIN channels c ON c.id = r.channel_id
        WHERE r.id = ?
        """,
        (release_id,),
    ).fetchone()
    if not row:
        raise MetadataPreviewApplyError(code="MPA_RELEASE_NOT_FOUND", message="Release not found")
    return dict(row)


def _normalize_requested_fields(fields: List[str] | None) -> set[str]:
    if fields is None:
        return set(ALL_FIELDS)
    normalized = {str(item).strip() for item in fields if str(item).strip()}
    invalid = normalized - set(ALL_FIELDS)
    if invalid:
        raise MetadataPreviewApplyError(code="MPA_FIELDS_INVALID", message=f"Unsupported fields requested: {', '.join(sorted(invalid))}")
    return normalized


def _build_not_requested_record(current_value: Any) -> Dict[str, Any]:
    return {
        "status": "NOT_REQUESTED",
        "current_value": current_value,
        "proposed_value": current_value,
        "changed": False,
        "overwrite_required": False,
        "source": None,
        "warnings": [],
        "errors": [],
    }


def _configuration_missing_record(current_value: Any) -> Dict[str, Any]:
    return {
        "status": "CONFIGURATION_MISSING",
        "current_value": current_value,
        "proposed_value": current_value,
        "changed": False,
        "overwrite_required": False,
        "source": None,
        "warnings": [],
        "errors": ["No active default source configured for requested field"],
    }


def _generation_failed_record(current_value: Any, *, code: str, message: str) -> Dict[str, Any]:
    return {
        "status": "GENERATION_FAILED",
        "current_value": current_value,
        "proposed_value": current_value,
        "changed": False,
        "overwrite_required": False,
        "source": None,
        "warnings": [],
        "errors": [f"{code}: {message}"],
    }


def _template_source_item(row: Dict[str, Any] | None, *, name_key: str) -> Dict[str, Any] | None:
    if not row:
        return None
    return {
        "id": int(row["id"]),
        name_key: str(row[name_key]),
        "status": str(row.get("status") or ""),
        "is_default": bool(int(row.get("is_default") or 0)),
    }


def _find_default_source(rows: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    for row in rows:
        if bool(int(row.get("is_default") or 0)):
            return row
    return None


def _list_usable_title_templates(conn: sqlite3.Connection, *, channel_slug: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, template_name, status, is_default
        FROM title_templates
        WHERE channel_slug = ? AND status = 'ACTIVE' AND validation_status = 'VALID'
        ORDER BY id DESC
        """,
        (channel_slug,),
    ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "template_name": str(row["template_name"]),
            "status": str(row["status"]),
            "is_default": bool(int(row.get("is_default") or 0)),
        }
        for row in rows
    ]


def _list_usable_description_templates(conn: sqlite3.Connection, *, channel_slug: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, template_name, status, is_default
        FROM description_templates
        WHERE channel_slug = ? AND status = 'ACTIVE' AND validation_status = 'VALID'
        ORDER BY id DESC
        """,
        (channel_slug,),
    ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "template_name": str(row["template_name"]),
            "status": str(row["status"]),
            "is_default": bool(int(row.get("is_default") or 0)),
        }
        for row in rows
    ]


def _diff_status(normalized_current: Any, normalized_proposed: Any) -> tuple[str, bool, bool]:
    if normalized_current == normalized_proposed:
        return "NO_CHANGE", False, False
    has_current = False
    if isinstance(normalized_current, list):
        has_current = bool(normalized_current)
    else:
        has_current = bool(str(normalized_current or "").strip())
    if has_current:
        return "OVERWRITE_READY", True, True
    return "PROPOSED_READY", True, False


def _as_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _future_iso(ttl_seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(ttl_seconds)))).isoformat()
