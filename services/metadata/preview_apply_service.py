from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
import uuid
from typing import Any, Dict, List, Set

from services.common import db as dbm
from services.metadata import description_template_service, descriptiongen_service, title_template_service, titlegen_service, video_tagsgen_service

ALL_FIELDS = ("title", "description", "tags")
APPLYABLE_FIELD_STATUSES = {"PROPOSED_READY", "OVERWRITE_READY", "NO_CHANGE"}


class MetadataPreviewApplyError(Exception):
    def __init__(self, *, code: str, message: str, details: Dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


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
    release = _load_release(conn, release_id=release_id)
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
        dependency_fingerprints["title"] = _build_field_dependency_fingerprint(
            field="title",
            release_row=release,
            source=title_source,
            generator_fingerprint=title_fingerprint,
        )
    if "description" in fields:
        description_rec, description_source, description_fingerprint = _prepare_description_field(
            conn,
            release_id=release_id,
            channel_slug=context.channel_slug,
            current_value=str(context.current["description"]),
            template_id=sources.get("description_template_id"),
        )
        resolved_sources["description"] = description_source
        dependency_fingerprints["description"] = _build_field_dependency_fingerprint(
            field="description",
            release_row=release,
            source=description_source,
            generator_fingerprint=description_fingerprint,
        )
    if "tags" in fields:
        tags_rec, tags_source, tags_fingerprint = _prepare_tags_field(
            conn,
            release_id=release_id,
            channel_slug=context.channel_slug,
            current_value=list(context.current["tags_json"]),
            preset_id=sources.get("video_tag_preset_id"),
        )
        resolved_sources["tags"] = tags_source
        dependency_fingerprints["tags"] = _build_field_dependency_fingerprint(
            field="tags",
            release_row=release,
            source=tags_source,
            generator_fingerprint=tags_fingerprint,
        )

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


def get_preview_session(conn: sqlite3.Connection, *, session_id: str) -> Dict[str, Any]:
    session = _load_session(conn, session_id=session_id)
    release = _load_release(conn, release_id=int(session["release_id"]))
    effective_status = _effective_session_status(conn, session=session)
    fields = _recalculate_field_staleness(session=session, release=release)
    return _build_session_payload(session=session, fields=fields, session_status=effective_status)


def apply_preview_session(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    selected_fields: List[str],
    overwrite_confirmed_fields: List[str] | None,
) -> Dict[str, Any]:
    session = _load_session(conn, session_id=session_id)
    release = _load_release(conn, release_id=int(session["release_id"]))
    status = _effective_session_status(conn, session=session)
    if status == "EXPIRED":
        raise MetadataPreviewApplyError(code="MPA_SESSION_EXPIRED", message="Preview session has expired")
    if status == "INVALIDATED":
        raise MetadataPreviewApplyError(code="MPA_SESSION_INVALIDATED", message="Preview session is invalidated")
    if status == "APPLIED":
        raise MetadataPreviewApplyError(code="MPA_APPLY_CONFLICT", message="Preview session has already been applied")
    if status != "OPEN":
        raise MetadataPreviewApplyError(code="MPA_APPLY_CONFLICT", message="Preview session is not open for apply")

    requested_fields = set(dbm.json_loads(str(session["requested_fields_json"]) or "[]"))
    selected = _normalize_selected_fields(selected_fields)
    if not selected:
        raise MetadataPreviewApplyError(code="MPA_SELECTED_FIELDS_EMPTY", message="selected_fields must not be empty")
    if not selected.issubset(requested_fields):
        raise MetadataPreviewApplyError(code="MPA_FIELD_NOT_PREPARED", message="Selected field is not prepared in this session")

    confirmed = _normalize_selected_fields(overwrite_confirmed_fields or [])
    fields = _recalculate_field_staleness(session=session, release=release)
    stale_selected = sorted([field for field in selected if str(fields[field]["status"]) == "STALE"])
    if stale_selected:
        raise MetadataPreviewApplyError(
            code="MPA_PREVIEW_STALE",
            message=f"Selected fields are stale: {', '.join(stale_selected)}",
            details={"stale_fields": stale_selected, "channel_slug": str(session.get("channel_slug") or "")},
        )

    for field in selected:
        status_value = str(fields[field]["status"])
        if status_value not in APPLYABLE_FIELD_STATUSES:
            raise MetadataPreviewApplyError(code="MPA_FIELD_NOT_SELECTEDABLE", message=f"Field '{field}' cannot be selected with status {status_value}")
        if status_value == "OVERWRITE_READY" and field not in confirmed:
            raise MetadataPreviewApplyError(
                code="MPA_OVERWRITE_CONFIRMATION_REQUIRED",
                message=f"overwrite confirmation is required for field '{field}'",
            )

    update_map: Dict[str, Any] = {}
    applied_fields: List[str] = []
    unchanged_fields: List[str] = []
    for field in ["title", "description", "tags"]:
        if field not in selected:
            continue
        if str(fields[field]["status"]) == "NO_CHANGE":
            unchanged_fields.append(field)
            continue
        proposed_value = fields[field].get("proposed_value")
        if field == "title":
            update_map["title"] = str(proposed_value or "")
        elif field == "description":
            update_map["description"] = str(proposed_value or "")
        elif field == "tags":
            update_map["tags_json"] = dbm.json_dumps(list(proposed_value or []))
        applied_fields.append(field)

    try:
        _apply_selected_fields_atomic(
            conn,
            release_id=int(session["release_id"]),
            selected_fields=selected,
            update_map=update_map,
            expected_release=release,
        )
        now_iso = _now_iso()
        _mark_session_applied_open_only(conn, session_id=session_id, applied_at=now_iso)
        conn.commit()
    except MetadataPreviewApplyError:
        conn.rollback()
        raise
    except sqlite3.OperationalError as exc:
        conn.rollback()
        raise MetadataPreviewApplyError(code="MPA_APPLY_CONFLICT", message=f"Failed to apply selected metadata fields: {exc}") from exc

    refreshed = _load_release(conn, release_id=int(session["release_id"]))
    return {
        "session_id": session_id,
        "release_id": int(session["release_id"]),
        "channel_slug": str(session.get("channel_slug") or ""),
        "applied_fields": applied_fields,
        "unchanged_fields": unchanged_fields,
        "result": "success",
        "release_metadata_after": {
            "title": str(refreshed.get("title") or ""),
            "description": str(refreshed.get("description") or ""),
            "tags_json": _normalize_tags_json_value(refreshed.get("tags_json")),
        },
        "stale_fields": [],
    }


def _apply_selected_fields_atomic(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    selected_fields: Set[str],
    update_map: Dict[str, Any],
    expected_release: Dict[str, Any],
) -> None:
    guarded_columns = _guarded_columns_for_selected_fields(selected_fields)
    where_parts = ["id = ?"]
    where_params: List[Any] = [release_id]
    for column in guarded_columns:
        value = expected_release.get(column)
        where_parts.append(f"(({column} IS NULL AND ? IS NULL) OR {column} = ?)")
        where_params.extend([value, value])

    assignments = ", ".join([f"{column} = ?" for column in update_map.keys()]) if update_map else "id = id"
    set_params = [update_map[column] for column in update_map.keys()] if update_map else []
    sql = f"UPDATE releases SET {assignments} WHERE {' AND '.join(where_parts)}"
    cur = conn.execute(sql, set_params + where_params)
    if int(cur.rowcount or 0) != 1:
        raise MetadataPreviewApplyError(code="MPA_APPLY_CONFLICT", message="Release changed during apply; regenerate preview and retry")


def _mark_session_applied_open_only(conn: sqlite3.Connection, *, session_id: str, applied_at: str) -> None:
    cur = conn.execute(
        "UPDATE metadata_preview_sessions SET session_status = 'APPLIED', applied_at = ? WHERE id = ? AND session_status = 'OPEN'",
        (applied_at, session_id),
    )
    if int(cur.rowcount or 0) != 1:
        raise MetadataPreviewApplyError(code="MPA_APPLY_CONFLICT", message="Preview session was already applied or no longer open")


def _guarded_columns_for_selected_fields(selected_fields: Set[str]) -> List[str]:
    guarded: List[str] = []
    for field in ["title", "description", "tags"]:
        if field not in selected_fields:
            continue
        if field == "title":
            guarded.extend(["title", "planned_at"])
        elif field == "description":
            guarded.extend(["description", "title", "planned_at"])
        elif field == "tags":
            guarded.extend(["tags_json", "title", "planned_at"])
    deduped: List[str] = []
    for column in guarded:
        if column not in deduped:
            deduped.append(column)
    return deduped


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
               , r.planned_at
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


def _normalize_selected_fields(fields: List[str] | None) -> Set[str]:
    normalized = {str(item).strip() for item in (fields or []) if str(item).strip()}
    invalid = normalized - set(ALL_FIELDS)
    if invalid:
        raise MetadataPreviewApplyError(code="MPA_FIELD_NOT_SELECTEDABLE", message=f"Unsupported selected fields: {', '.join(sorted(invalid))}")
    return normalized


def _load_session(conn: sqlite3.Connection, *, session_id: str) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM metadata_preview_sessions WHERE id = ?", (session_id,)).fetchone()
    if not row:
        raise MetadataPreviewApplyError(code="MPA_SESSION_NOT_FOUND", message="Preview session not found")
    return dict(row)


def _effective_session_status(conn: sqlite3.Connection, *, session: Dict[str, Any]) -> str:
    current_status = str(session.get("session_status") or "")
    if current_status != "OPEN":
        return current_status
    expires_at = datetime.fromisoformat(str(session["expires_at"]))
    if datetime.now(timezone.utc) > expires_at:
        conn.execute("UPDATE metadata_preview_sessions SET session_status = 'EXPIRED' WHERE id = ? AND session_status = 'OPEN'", (str(session["id"]),))
        conn.commit()
        return "EXPIRED"
    return "OPEN"


def _build_session_payload(*, session: Dict[str, Any], fields: Dict[str, Any], session_status: str) -> Dict[str, Any]:
    requested_fields = [f for f in ALL_FIELDS if f in set(dbm.json_loads(str(session["requested_fields_json"]) or "[]"))]
    statuses = {field: str(fields[field]["status"]) for field in ALL_FIELDS}
    prepared_fields = [f for f in requested_fields if statuses[f] in {"PROPOSED_READY", "NO_CHANGE", "OVERWRITE_READY", "CURRENT_ONLY"}]
    applyable_fields = [f for f in requested_fields if statuses[f] in APPLYABLE_FIELD_STATUSES]
    failed_fields = [f for f in requested_fields if statuses[f] in {"GENERATION_FAILED", "CONFIGURATION_MISSING", "STALE"}]
    return {
        "session_id": str(session["id"]),
        "release_id": int(session["release_id"]),
        "channel_slug": str(session["channel_slug"]),
        "session_status": session_status,
        "expires_at": str(session["expires_at"]),
        "current": dbm.json_loads(str(session["current_bundle_json"]) or "{}"),
        "fields": fields,
        "summary": {
            "requested_fields": requested_fields,
            "prepared_fields": prepared_fields,
            "applyable_fields": applyable_fields,
            "failed_fields": failed_fields,
        },
    }


def _recalculate_field_staleness(*, session: Dict[str, Any], release: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    fields = dbm.json_loads(str(session.get("fields_snapshot_json") or "{}"))
    dependency_fingerprints = dbm.json_loads(str(session.get("dependency_fingerprints_json") or "{}"))
    for field in ALL_FIELDS:
        record = fields.get(field) or _build_not_requested_record(_current_value_for_field(field=field, release=release))
        status = str(record.get("status") or "")
        if status in {"PROPOSED_READY", "OVERWRITE_READY", "NO_CHANGE"}:
            stored = dependency_fingerprints.get(field)
            current = _build_field_dependency_fingerprint(
                field=field,
                release_row=release,
                source=(record.get("source") or {}),
                generator_fingerprint=(stored or {}).get("generator_fingerprint") if isinstance(stored, dict) else stored,
            )
            stale = not _fingerprints_match(stored=stored, current=current)
            if stale:
                record["status"] = "STALE"
        fields[field] = record
    return fields


def _fingerprints_match(*, stored: Any, current: Dict[str, Any]) -> bool:
    if stored is None:
        return False
    if isinstance(stored, str):
        return stored == current.get("generator_fingerprint")
    if not isinstance(stored, dict):
        return False
    return (
        str(stored.get("target_field_fingerprint") or "") == str(current.get("target_field_fingerprint") or "")
        and str(stored.get("render_context_fingerprint") or "") == str(current.get("render_context_fingerprint") or "")
    )


def _build_field_dependency_fingerprint(
    *,
    field: str,
    release_row: Dict[str, Any],
    source: Dict[str, Any],
    generator_fingerprint: str | None,
) -> Dict[str, Any]:
    target_payload = _target_fingerprint_payload(field=field, release_row=release_row)
    context_payload = _context_fingerprint_payload(field=field, release_row=release_row)
    source_id = source.get("source_id")
    if source_id is None:
        source_id = source.get("id")
    return {
        "target_field_fingerprint": _hash_payload(target_payload),
        "render_context_fingerprint": _hash_payload(context_payload),
        "source_id": source_id,
        "source_updated_at": source.get("updated_at"),
        "generator_fingerprint": generator_fingerprint,
    }


def _target_fingerprint_payload(*, field: str, release_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "field": field,
        "value": _current_value_for_field(field=field, release=release_row),
    }


def _context_fingerprint_payload(*, field: str, release_row: Dict[str, Any]) -> Dict[str, Any]:
    planned_at = str(release_row.get("planned_at") or "")
    if field == "title":
        return {"planned_at": planned_at}
    if field in {"description", "tags"}:
        return {
            "title": str(release_row.get("title") or ""),
            "planned_at": planned_at,
        }
    return {}


def _current_value_for_field(*, field: str, release: Dict[str, Any]) -> Any:
    if field == "title":
        return str(release.get("title") or "")
    if field == "description":
        return str(release.get("description") or "")
    return _normalize_tags_json_value(release.get("tags_json"))


def _normalize_tags_json_value(value: Any) -> List[str]:
    if isinstance(value, list):
        raw = value
    else:
        try:
            raw = dbm.json_loads(str(value or "[]"))
        except Exception:
            raw = []
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw if isinstance(item, str)]


def _hash_payload(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
