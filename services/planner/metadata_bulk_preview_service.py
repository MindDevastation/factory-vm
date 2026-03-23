from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import sqlite3
import uuid
from typing import Any

from services.common import db as dbm
from services.metadata import preview_apply_service

ALL_FIELDS = ("title", "description", "tags")
RESOLVED_STATUSES = {"PROPOSED_READY", "NO_CHANGE", "OVERWRITE_READY"}
APPLYABLE_STATUSES = {"PROPOSED_READY", "OVERWRITE_READY", "NO_CHANGE"}
SOURCE_LOOKUPS = {
    "title_template": dbm.get_title_template_by_id,
    "description_template": dbm.get_description_template_by_id,
    "video_tag_preset": dbm.get_video_tag_preset_by_id,
}
logger = logging.getLogger(__name__)


class MetadataBulkPreviewError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def load_bulk_context(conn: sqlite3.Connection, *, planner_item_ids: list[int]) -> dict[str, Any]:
    mappings = _resolve_planner_items(conn, planner_item_ids=planner_item_ids)
    by_channel: dict[str, dict[str, Any]] = {}
    for rec in mappings:
        if rec["mapping_status"] != "RESOLVED_TO_RELEASE":
            continue
        channel_slug = str(rec["channel_slug"])
        if channel_slug in by_channel:
            by_channel[channel_slug]["item_count"] += 1
            continue
        context = preview_apply_service.load_preview_apply_context(conn, release_id=int(rec["release_id"]))
        by_channel[channel_slug] = {
            "channel_slug": channel_slug,
            "item_count": 1,
            "available_sources": context.active_sources,
        }
    return {
        "selected_item_count": len(planner_item_ids),
        "channel_groups": [by_channel[key] for key in sorted(by_channel.keys())],
    }


def create_bulk_preview_session(
    conn: sqlite3.Connection,
    *,
    planner_item_ids: list[int],
    fields: list[str] | None,
    overrides: dict[str, Any] | None,
    created_by: str | None,
    ttl_seconds: int,
) -> dict[str, Any]:
    normalized_fields = _normalize_fields(fields)
    mappings = _resolve_planner_items(conn, planner_item_ids=planner_item_ids)

    seen_release_ids: set[int] = set()
    item_states: list[dict[str, Any]] = []
    selected_channels: set[str] = set()

    for rec in mappings:
        state = {
            "planner_item_id": rec["planner_item_id"],
            "mapping_status": rec["mapping_status"],
            "release_id": rec.get("release_id"),
            "channel_slug": rec.get("channel_slug"),
            "fields": {field: {"status": "NOT_REQUESTED"} for field in ALL_FIELDS},
            "item_applyable": False,
            "item_errors": list(rec.get("item_errors") or []),
        }
        if rec["mapping_status"] != "RESOLVED_TO_RELEASE":
            item_states.append(state)
            continue

        release_id = int(rec["release_id"])
        if release_id in seen_release_ids:
            state["mapping_status"] = "DUPLICATE_TARGET"
            state["item_errors"].append({"code": "MBP_DUPLICATE_TARGET_DEDUPED", "message": "Duplicate release target in selection"})
            item_states.append(state)
            continue
        seen_release_ids.add(release_id)

        channel_slug = str(rec["channel_slug"])
        selected_channels.add(channel_slug)
        sources = _sources_for_item(
            conn,
            release_id=release_id,
            channel_slug=channel_slug,
            fields=normalized_fields,
            overrides=overrides or {},
        )
        prepared = preview_apply_service.prepare_preview_fields_for_release(
            conn,
            release_id=release_id,
            requested_fields=normalized_fields,
            sources=sources,
        )
        for field in ALL_FIELDS:
            state["fields"][field] = dict(prepared["field_records"][field])
            state["fields"][field]["effective_source_selection"] = prepared["effective_source_selection"].get(field)
            state["fields"][field]["effective_source_provenance"] = prepared["effective_source_provenance"].get(field)
            state["fields"][field]["dependency_fingerprint"] = prepared["dependency_fingerprints"].get(field)
        state["current"] = dict(prepared["context"].current)
        state["item_applyable"] = any(
            state["fields"][f]["status"] in RESOLVED_STATUSES for f in normalized_fields
        )
        item_states.append(state)

    summary = _aggregate_summary(item_states=item_states, selected_item_count=len(planner_item_ids), requested_fields=normalized_fields)
    now = _now_iso()
    expires_at = _future_iso(ttl_seconds)
    session_id = uuid.uuid4().hex
    planner_context = {
        "mapping_source": "planner_release_links",
        "selected_item_count": len(planner_item_ids),
        "resolved_target_count": summary["resolved_target_count"],
        "deduped_target_count": summary["deduped_target_count"],
    }

    dbm.insert_metadata_bulk_preview_session(
        conn,
        session_id=session_id,
        planner_context_json=dbm.json_dumps(planner_context),
        selected_item_ids_json=dbm.json_dumps(planner_item_ids),
        requested_fields_json=dbm.json_dumps(normalized_fields),
        selected_channels_json=dbm.json_dumps(sorted(selected_channels)),
        session_status="OPEN",
        aggregate_summary_json=dbm.json_dumps(summary),
        item_states_json=dbm.json_dumps(item_states),
        created_by=created_by,
        created_at=now,
        expires_at=expires_at,
        applied_at=None,
    )
    conn.commit()
    return {
        "session_id": session_id,
        "session_status": "OPEN",
        "expires_at": expires_at,
        "summary": summary,
        "items": item_states,
    }


def get_bulk_preview_session(conn: sqlite3.Connection, *, session_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM metadata_bulk_preview_sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        raise MetadataBulkPreviewError(code="MBP_SESSION_NOT_FOUND", message="Bulk preview session not found")
    body = dict(row)
    status = str(body["session_status"])
    if status == "OPEN" and datetime.now(timezone.utc) > datetime.fromisoformat(str(body["expires_at"])):
        conn.execute("UPDATE metadata_bulk_preview_sessions SET session_status = 'EXPIRED' WHERE id = ? AND session_status = 'OPEN'", (session_id,))
        conn.commit()
        status = "EXPIRED"
    return {
        "session_id": str(body["id"]),
        "session_status": status,
        "expires_at": str(body["expires_at"]),
        "summary": dbm.json_loads(str(body["aggregate_summary_json"])),
        "items": dbm.json_loads(str(body["item_states_json"])),
    }


def apply_bulk_preview_session(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    selected_items: list[int],
    selected_fields: list[str],
    overwrite_confirmed: dict[str, list[str]] | None,
) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM metadata_bulk_preview_sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        raise MetadataBulkPreviewError(code="MBP_SESSION_NOT_FOUND", message="Bulk preview session not found")
    body = dict(row)
    status = _effective_bulk_session_status(conn, session=body)
    if status == "EXPIRED":
        raise MetadataBulkPreviewError(code="MBP_SESSION_EXPIRED", message="Bulk preview session has expired")
    if status == "INVALIDATED":
        raise MetadataBulkPreviewError(code="MBP_SESSION_INVALIDATED", message="Bulk preview session is invalidated")
    if status != "OPEN":
        raise MetadataBulkPreviewError(code="MBP_SESSION_INVALIDATED", message="Bulk preview session is not open")

    normalized_items = _normalize_selected_items(selected_items)
    if not normalized_items:
        raise MetadataBulkPreviewError(code="MBP_SELECTED_ITEMS_EMPTY", message="selected_items must not be empty")
    normalized_fields = _normalize_selected_fields(selected_fields)
    if not normalized_fields:
        raise MetadataBulkPreviewError(code="MBP_SELECTED_FIELDS_EMPTY", message="selected_fields must not be empty")

    items = dbm.json_loads(str(body.get("item_states_json") or "[]"))
    requested_fields = set(dbm.json_loads(str(body.get("requested_fields_json") or "[]")))
    if not set(normalized_fields).issubset(requested_fields):
        raise MetadataBulkPreviewError(code="MBP_FIELD_NOT_PREPARED", message="Selected field is not prepared in this session")

    by_planner: dict[int, dict[str, Any]] = {}
    for item in items:
        planner_id = int(item["planner_item_id"])
        by_planner[planner_id] = item
    if not set(normalized_items).issubset(set(by_planner.keys())):
        raise MetadataBulkPreviewError(code="MBP_FIELD_NOT_PREPARED", message="selected_items must be a subset of session items")

    confirmed = _normalize_overwrite_confirmed(overwrite_confirmed)
    logger.info(
        "metadata.bulk_apply.started %s",
        {
            "session_id": session_id,
            "selected_item_count": len(normalized_items),
            "selected_apply_fields": list(normalized_fields),
        },
    )
    conn.execute("BEGIN")
    results: list[dict[str, Any]] = []
    success_count = 0
    failure_count = 0
    item_errors_all: list[str] = []
    stale_detected = 0
    for index, planner_item_id in enumerate(normalized_items):
        savepoint = f"mbp_item_{index}"
        conn.execute(f"SAVEPOINT {savepoint}")
        item = dict(by_planner[planner_item_id])
        release_id = item.get("release_id")
        item_result = {
            "planner_item_id": planner_item_id,
            "release_id": int(release_id) if release_id is not None else None,
            "result": "failure",
            "applied_fields": [],
            "unchanged_fields": [],
            "errors": [],
        }
        try:
            if str(item.get("mapping_status") or "") != "RESOLVED_TO_RELEASE" or release_id is None:
                raise MetadataBulkPreviewError(code="MBP_FIELD_NOT_SELECTEDABLE", message="Planner item is not resolved to a release target")
            release = preview_apply_service._load_release(conn, release_id=int(release_id))
            fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
            update_map: dict[str, Any] = {}
            selected_set = set(normalized_fields)
            for field in ALL_FIELDS:
                if field in selected_set:
                    continue
                rec = dict(fields.get(field) or {"status": "NOT_REQUESTED"})
                if str(rec.get("status") or "") in APPLYABLE_STATUSES:
                    rec["status"] = "NOT_SELECTED_FOR_APPLY"
                fields[field] = rec
            for field in normalized_fields:
                rec = dict(fields.get(field) or {"status": "NOT_REQUESTED"})
                stale = _is_field_stale(conn, item=item, field=field, record=rec, release=release)
                if stale:
                    stale_detected += 1
                    rec["status"] = "STALE"
                    fields[field] = rec
                    raise MetadataBulkPreviewError(code="MBP_PREVIEW_STALE", message=f"Prepared state for {field} is stale.")
                status_value = str(rec.get("status") or "")
                if status_value not in APPLYABLE_STATUSES:
                    raise MetadataBulkPreviewError(code="MBP_FIELD_NOT_SELECTEDABLE", message=f"Field '{field}' cannot be selected with status {status_value}")
                if status_value == "OVERWRITE_READY":
                    confirmed_for_item = confirmed.get(planner_item_id, set())
                    if field not in confirmed_for_item:
                        raise MetadataBulkPreviewError(
                            code="MBP_OVERWRITE_CONFIRMATION_REQUIRED",
                            message=f"overwrite confirmation is required for field '{field}'",
                        )
                if status_value == "NO_CHANGE":
                    item_result["unchanged_fields"].append(field)
                    continue
                proposed = rec.get("proposed_value")
                if field == "title":
                    update_map["title"] = str(proposed or "")
                elif field == "description":
                    update_map["description"] = str(proposed or "")
                elif field == "tags":
                    update_map["tags_json"] = dbm.json_dumps(list(proposed or []))
                item_result["applied_fields"].append(field)

            preview_apply_service._apply_selected_fields_atomic(
                conn,
                release_id=int(release_id),
                selected_fields=set(normalized_fields),
                update_map=update_map,
                expected_release=release,
            )
            item_result["result"] = "success"
            success_count += 1
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        except MetadataBulkPreviewError as exc:
            item_result["errors"].append({"code": exc.code, "message": exc.message})
            item_errors_all.append(exc.code)
            failure_count += 1
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        except preview_apply_service.MetadataPreviewApplyError:
            item_result["errors"].append({"code": "MBP_ITEM_APPLY_FAILED", "message": "Failed to apply selected metadata fields for item"})
            item_errors_all.append("MBP_ITEM_APPLY_FAILED")
            failure_count += 1
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        results.append(item_result)

    result_state = "success"
    if success_count and failure_count:
        result_state = "partial_success"
    elif failure_count and not success_count:
        result_state = "failed"
    now_iso = _now_iso()
    conn.execute(
        "UPDATE metadata_bulk_preview_sessions SET session_status = 'APPLIED', applied_at = ? WHERE id = ? AND session_status = 'OPEN'",
        (now_iso, session_id),
    )
    conn.commit()
    if stale_detected:
        logger.info("metadata.bulk_preview.stale_detected %s", {"session_id": session_id, "stale_count": stale_detected})
    if result_state == "partial_success":
        logger.info(
            "metadata.bulk_apply.partial_success %s",
            {
                "session_id": session_id,
                "selected_item_count": len(normalized_items),
                "selected_apply_fields": list(normalized_fields),
                "apply_success_count": success_count,
                "apply_failure_count": failure_count,
                "error_codes": sorted(set(item_errors_all)),
            },
        )
    elif result_state == "failed":
        logger.info(
            "metadata.bulk_apply.failed %s",
            {
                "session_id": session_id,
                "selected_item_count": len(normalized_items),
                "selected_apply_fields": list(normalized_fields),
                "apply_success_count": success_count,
                "apply_failure_count": failure_count,
                "error_codes": sorted(set(item_errors_all)),
            },
        )
    else:
        logger.info(
            "metadata.bulk_apply.completed %s",
            {
                "session_id": session_id,
                "selected_item_count": len(normalized_items),
                "selected_apply_fields": list(normalized_fields),
                "apply_success_count": success_count,
                "apply_failure_count": failure_count,
            },
        )
    return {
        "session_id": session_id,
        "result": result_state,
        "summary": {
            "selected_item_count": len(normalized_items),
            "items_applied_successfully": success_count,
            "items_failed": failure_count,
        },
        "items": results,
    }


def _resolve_planner_items(conn: sqlite3.Connection, *, planner_item_ids: list[int]) -> list[dict[str, Any]]:
    normalized_ids = [int(item) for item in planner_item_ids]
    if not normalized_ids:
        raise MetadataBulkPreviewError(code="MBP_SELECTED_ITEMS_EMPTY", message="planner_item_ids must not be empty")
    unique_ids = sorted(set(normalized_ids))

    placeholders = ",".join("?" for _ in unique_ids)
    rows = conn.execute(
        f"""
        SELECT pr.id AS planner_item_id,
               prl.release_id,
               c.slug AS channel_slug
        FROM planned_releases pr
        LEFT JOIN planner_release_links prl ON prl.planned_release_id = pr.id
        LEFT JOIN releases r ON r.id = prl.release_id
        LEFT JOIN channels c ON c.id = r.channel_id
        WHERE pr.id IN ({placeholders})
        """,
        tuple(unique_ids),
    ).fetchall()
    by_id = {int(row["planner_item_id"]): dict(row) for row in rows}

    output: list[dict[str, Any]] = []
    for item_id in normalized_ids:
        row = by_id.get(item_id)
        if row is None:
            output.append({
                "planner_item_id": item_id,
                "mapping_status": "INVALID_SELECTION",
                "release_id": None,
                "channel_slug": None,
                "item_errors": [{"code": "MBP_ITEM_INVALID_SELECTION", "message": "Planner item does not exist"}],
            })
            continue
        release_id = row.get("release_id")
        if release_id is None:
            output.append({
                "planner_item_id": item_id,
                "mapping_status": "UNRESOLVED_NO_TARGET",
                "release_id": None,
                "channel_slug": None,
                "item_errors": [{"code": "MBP_ITEM_TARGET_UNRESOLVED", "message": "Planner item has no canonical release binding"}],
            })
            continue
        output.append({
            "planner_item_id": item_id,
            "mapping_status": "RESOLVED_TO_RELEASE",
            "release_id": int(release_id),
            "channel_slug": str(row.get("channel_slug") or ""),
            "item_errors": [],
        })
    return output


def _normalize_fields(fields: list[str] | None) -> list[str]:
    requested = preview_apply_service._normalize_requested_fields(fields)  # canonical validation
    return [field for field in ALL_FIELDS if field in requested]


def _normalize_selected_fields(fields: list[str] | None) -> list[str]:
    selected = preview_apply_service._normalize_selected_fields(fields)
    return [field for field in ALL_FIELDS if field in selected]


def _normalize_selected_items(items: list[int] | None) -> list[int]:
    normalized: list[int] = []
    for item in list(items or []):
        normalized_item = int(item)
        if normalized_item not in normalized:
            normalized.append(normalized_item)
    return normalized


def _normalize_overwrite_confirmed(payload: dict[str, list[str]] | None) -> dict[int, set[str]]:
    output: dict[int, set[str]] = {}
    if not isinstance(payload, dict):
        return output
    for raw_item_id, fields in payload.items():
        item_id = int(raw_item_id)
        output[item_id] = preview_apply_service._normalize_selected_fields(fields)
    return output


def _effective_bulk_session_status(conn: sqlite3.Connection, *, session: dict[str, Any]) -> str:
    status = str(session.get("session_status") or "")
    if status != "OPEN":
        return status
    expires_at = datetime.fromisoformat(str(session["expires_at"]))
    if datetime.now(timezone.utc) > expires_at:
        conn.execute("UPDATE metadata_bulk_preview_sessions SET session_status = 'EXPIRED' WHERE id = ? AND session_status = 'OPEN'", (str(session["id"]),))
        conn.commit()
        return "EXPIRED"
    return "OPEN"


def _is_field_stale(
    conn: sqlite3.Connection,
    *,
    item: dict[str, Any],
    field: str,
    record: dict[str, Any],
    release: dict[str, Any],
) -> bool:
    status = str(record.get("status") or "")
    if status == "STALE":
        return True
    if status not in APPLYABLE_STATUSES:
        return False
    stored_current = record.get("current_value")
    live_current = preview_apply_service._current_value_for_field(field=field, release=release)
    if field in {"title", "description"}:
        if str(stored_current or "") != str(live_current or ""):
            return True
    elif list(stored_current or []) != list(live_current or []):
        return True
    stored_fingerprint = record.get("dependency_fingerprint")
    current_fingerprint = preview_apply_service._build_field_dependency_fingerprint(
        field=field,
        release_row=release,
        source=(record.get("source") or {}),
        generator_fingerprint=(stored_fingerprint or {}).get("generator_fingerprint") if isinstance(stored_fingerprint, dict) else None,
    )
    if not preview_apply_service._fingerprints_match(stored=stored_fingerprint, current=current_fingerprint):
        return True
    source = record.get("source")
    if isinstance(source, dict) and source.get("source_id") is not None:
        if not _is_source_active_valid(conn, source=source, channel_slug=str(item.get("channel_slug") or "")):
            return True
    return False


def _is_source_active_valid(conn: sqlite3.Connection, *, source: dict[str, Any], channel_slug: str) -> bool:
    source_type = str(source.get("source_type") or "")
    source_id = source.get("source_id")
    if source_type not in SOURCE_LOOKUPS or source_id is None:
        return True
    row = SOURCE_LOOKUPS[source_type](conn, int(source_id))
    if not row:
        return False
    if str(row.get("channel_slug") or "") != channel_slug:
        return False
    if str(row.get("status") or "") != "ACTIVE":
        return False
    if row.get("validation_status") is not None and str(row.get("validation_status") or "") != "VALID":
        return False
    return True


def _sources_for_item(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    channel_slug: str,
    fields: list[str],
    overrides: dict[str, Any],
) -> dict[str, Any]:
    context = preview_apply_service.load_preview_apply_context(conn, release_id=release_id)
    defaults = context.defaults
    result: dict[str, Any] = {}
    for field in fields:
        cfg = overrides.get(field) if isinstance(overrides.get(field), dict) else {"mode": "DEFAULT_ONLY"}
        mode = str(cfg.get("mode") or "DEFAULT_ONLY")
        if mode not in {"DEFAULT_ONLY", "CHANNEL_GROUP_OVERRIDE_IF_MATCHES"}:
            raise MetadataBulkPreviewError(code="MBP_OVERRIDE_MODE_UNSUPPORTED", message=f"Unsupported override mode: {mode}")
        override_id = None
        if mode == "CHANNEL_GROUP_OVERRIDE_IF_MATCHES":
            for row in list(cfg.get("overrides") or []):
                if str(row.get("channel_slug") or "") == channel_slug:
                    override_id = int(row.get("source_id"))
                    break
        if field == "title":
            result["title_template_id"] = override_id if override_id is not None else _source_id(defaults.get("title_template"))
        elif field == "description":
            result["description_template_id"] = override_id if override_id is not None else _source_id(defaults.get("description_template"))
        elif field == "tags":
            result["video_tag_preset_id"] = override_id if override_id is not None else _source_id(defaults.get("video_tag_preset"))
    return result


def _source_id(item: Any) -> int | None:
    if not isinstance(item, dict):
        return None
    value = item.get("id")
    if value is None:
        return None
    return int(value)


def _aggregate_summary(*, item_states: list[dict[str, Any]], selected_item_count: int, requested_fields: list[str]) -> dict[str, Any]:
    resolved_target_count = sum(1 for item in item_states if item["mapping_status"] == "RESOLVED_TO_RELEASE")
    deduped_target_count = sum(1 for item in item_states if item["mapping_status"] == "RESOLVED_TO_RELEASE" and not item["item_errors"])
    applyable_item_count = sum(1 for item in item_states if bool(item.get("item_applyable")))
    item_error_count = sum(1 for item in item_states if bool(item.get("item_errors")))
    overwrite_item_count = sum(1 for item in item_states if any(item["fields"][f].get("status") == "OVERWRITE_READY" for f in requested_fields))

    field_counts: dict[str, dict[str, int]] = {}
    for field in requested_fields:
        counts = {"ready": 0, "no_change": 0, "overwrite": 0, "source_missing": 0, "generation_failed": 0}
        for item in item_states:
            status = str(item["fields"][field].get("status") or "")
            if status == "PROPOSED_READY":
                counts["ready"] += 1
            elif status == "NO_CHANGE":
                counts["no_change"] += 1
            elif status == "OVERWRITE_READY":
                counts["overwrite"] += 1
            elif status in {"CONFIGURATION_MISSING", "INVALID_OVERRIDE", "INVALID_DEFAULT", "SOURCE_MISSING"}:
                counts["source_missing"] += 1
            elif status == "GENERATION_FAILED":
                counts["generation_failed"] += 1
        field_counts[field] = counts

    all_ready = 0
    some_ready = 0
    none_ready = 0
    for item in item_states:
        statuses = [str(item["fields"][f].get("status") or "") for f in requested_fields]
        ok = sum(1 for status in statuses if status in RESOLVED_STATUSES)
        if ok == len(requested_fields) and ok > 0:
            all_ready += 1
        elif ok > 0:
            some_ready += 1
        else:
            none_ready += 1

    return {
        "selected_item_count": selected_item_count,
        "resolved_target_count": resolved_target_count,
        "deduped_target_count": deduped_target_count,
        "applyable_item_count": applyable_item_count,
        "item_error_count": item_error_count,
        "overwrite_item_count": overwrite_item_count,
        "field_counts": field_counts,
        "items_all_requested_fields_ready": all_ready,
        "items_some_fields_ready": some_ready,
        "items_no_fields_ready": none_ready,
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _future_iso(ttl_seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(ttl_seconds)))).replace(microsecond=0).isoformat().replace("+00:00", "Z")
