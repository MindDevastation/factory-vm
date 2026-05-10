from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Callable

from services.common import db as dbm
from services.planner.visual_batch_service import create_visual_batch_preview_session

AdapterFn = Callable[[dict], dict]


class RuntimeAdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, AdapterFn] = {}

    def register(self, capability_code: str, adapter: AdapterFn) -> None:
        self._adapters[str(capability_code)] = adapter

    def get(self, capability_code: str) -> AdapterFn | None:
        return self._adapters.get(str(capability_code))


_SYNC_RESULT_CODE = {
    "CREATE_BULK_JSON_DRAFT": "BULK_JSON_DRAFT_TARGET_UPDATED",
    "CREATE_METADATA_REQUEST": "METADATA_TARGET_UPDATED",
    "CREATE_VISUAL_REQUEST": "VISUAL_TARGET_UPDATED",
    "CREATE_ANALYTICS_REQUEST": "ANALYTICS_TARGET_UPDATED",
}

_TARGET_KIND = {
    "CREATE_BULK_JSON_DRAFT": "bulk_json_draft",
    "CREATE_METADATA_REQUEST": "metadata_request",
    "CREATE_VISUAL_REQUEST": "visual_request",
    "CREATE_ANALYTICS_REQUEST": "analytics_request",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_id(prefix: str, payload: dict) -> str:
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return f"{prefix}-{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:20]}"


def _as_int(value, *, field_name: str) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Runtime adapter missing valid {field_name}.") from exc
    if out <= 0:
        raise RuntimeError(f"Runtime adapter missing valid {field_name}.")
    return out


def _require_text(payload: dict, field_name: str) -> str:
    value = str(payload.get(field_name) or "").strip()
    if not value:
        raise RuntimeError(f"Runtime adapter missing required {field_name}.")
    return value


def _json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _runtime_context(payload: dict) -> tuple[sqlite3.Connection, int, int, str, str | None, str | None]:
    if not isinstance(payload, dict):
        raise ValueError("Runtime payload must be an object.")
    conn = payload.get("_runtime_conn")
    execution_group_id = int(payload.get("_execution_group_id") or 0)
    execution_attempt_id = int(payload.get("_execution_attempt_id") or 0)
    capability_code = str(payload.get("_capability_code") or "").strip()
    if not isinstance(conn, sqlite3.Connection) or execution_group_id <= 0 or execution_attempt_id <= 0 or not capability_code:
        raise RuntimeError("Runtime adapter missing controlled execution context.")
    target_row = conn.execute("SELECT target_type,target_id FROM prompt_execution_groups WHERE id=?", (execution_group_id,)).fetchone()
    if target_row is None:
        raise RuntimeError("Runtime adapter missing execution group target.")
    return conn, execution_group_id, execution_attempt_id, capability_code, target_row[0], target_row[1]


def _record_usage_artifact(conn: sqlite3.Connection, *, execution_group_id: int, execution_attempt_id: int, artifact_ref: str, product_target: dict) -> None:
    cur = conn.execute(
        """
        UPDATE prompt_execution_usage
        SET artifact_ref=?, usage_payload_json=json_set(COALESCE(usage_payload_json,'{}'),'$.internal_product_target',json(?)), updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
        WHERE execution_group_id=? AND latest_attempt_id=?
        """,
        (artifact_ref, _json_dumps(product_target), execution_group_id, execution_attempt_id),
    )
    if cur.rowcount != 1:
        raise RuntimeError("Runtime adapter could not update execution usage bookkeeping.")


def _create_bulk_json_draft(payload: dict) -> dict:
    conn, execution_group_id, execution_attempt_id, capability_code, target_type, target_id = _runtime_context(payload)
    channel_id = payload.get("channel_id")
    if channel_id is None:
        channel_slug = str(payload.get("channel_slug") or (target_id if target_type == "channel" else "")).strip()
        if not channel_slug:
            raise RuntimeError("Runtime adapter missing channel target for bulk JSON draft.")
        channel = conn.execute("SELECT id FROM channels WHERE slug=?", (channel_slug,)).fetchone()
        if channel is None:
            raise RuntimeError("Runtime adapter channel target does not exist.")
        channel_id = channel[0]
    job_id = dbm.create_ui_job_draft(
        conn,
        channel_id=_as_int(channel_id, field_name="channel_id"),
        title=_require_text(payload, "title"),
        description=str(payload.get("description") or ""),
        tags_csv=str(payload.get("tags_csv") or ""),
        playlists_json=_json_dumps(payload.get("playlist_ids") or []),
        playlist_create_title=str(payload.get("playlist_create_title") or "").strip() or None,
        audience_is_for_kids=1 if bool(payload.get("audience_is_for_kids")) else 0,
        video_language=str(payload.get("video_language") or "en").strip() or "en",
        cover_name=str(payload.get("cover_name") or "").strip() or None,
        cover_ext=str(payload.get("cover_ext") or "").strip() or None,
        background_name=_require_text(payload, "background_name"),
        background_ext=_require_text(payload, "background_ext"),
        audio_ids_text=_require_text(payload, "audio_ids_text"),
        job_type="UI",
    )
    artifact_ref = f"ui_job_drafts:{job_id}"
    product_target = {"artifact_ref": artifact_ref, "target_kind": "bulk_json_draft", "job_id": job_id, "status": "CREATED"}
    _record_usage_artifact(conn, execution_group_id=execution_group_id, execution_attempt_id=execution_attempt_id, artifact_ref=artifact_ref, product_target=product_target)
    return {"result_code": _SYNC_RESULT_CODE[capability_code], "secret_safe_message": "Bulk JSON draft job created.", "artifact_ref": artifact_ref}


def _create_metadata_request(payload: dict) -> dict:
    conn, execution_group_id, execution_attempt_id, capability_code, target_type, target_id = _runtime_context(payload)
    selected_item_ids = payload.get("selected_item_ids") or ([target_id] if target_id else [])
    if not isinstance(selected_item_ids, list) or not selected_item_ids:
        raise RuntimeError("Runtime adapter missing selected metadata items.")
    requested_fields = payload.get("requested_fields") or ["title", "description", "tags"]
    selected_channels = payload.get("selected_channels") or ([] if target_type != "channel" or not target_id else [target_id])
    now = _utc_now()
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=15)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    session_payload = {
        "execution_group_id": execution_group_id,
        "execution_attempt_id": execution_attempt_id,
        "selected_item_ids": selected_item_ids,
        "requested_fields": requested_fields,
        "selected_channels": selected_channels,
    }
    session_id = _stable_id("runtime-metadata", session_payload)
    dbm.insert_metadata_bulk_preview_session(
        conn,
        session_id=session_id,
        planner_context_json=_json_dumps({"source": "prompt_runtime", "target_type": target_type, "target_id": target_id}),
        selected_item_ids_json=_json_dumps(selected_item_ids),
        requested_fields_json=_json_dumps(requested_fields),
        selected_channels_json=_json_dumps(selected_channels),
        session_status="OPEN",
        aggregate_summary_json=_json_dumps({"requested_total": len(selected_item_ids), "source": "prompt_runtime"}),
        item_states_json=_json_dumps([{"item_id": item, "status": "REQUESTED"} for item in selected_item_ids]),
        created_by=str(payload.get("created_by") or "prompt_runtime"),
        created_at=now,
        expires_at=expires_at,
        applied_at=None,
    )
    artifact_ref = f"metadata_bulk_preview_sessions:{session_id}"
    product_target = {"artifact_ref": artifact_ref, "target_kind": "metadata_request", "session_id": session_id, "status": "OPEN"}
    _record_usage_artifact(conn, execution_group_id=execution_group_id, execution_attempt_id=execution_attempt_id, artifact_ref=artifact_ref, product_target=product_target)
    return {"result_code": _SYNC_RESULT_CODE[capability_code], "secret_safe_message": "Metadata preview request created.", "artifact_ref": artifact_ref}


def _create_visual_request(payload: dict) -> dict:
    conn, execution_group_id, execution_attempt_id, capability_code, _target_type, target_id = _runtime_context(payload)
    selected_release_ids = payload.get("selected_release_ids") or ([target_id] if target_id else [])
    if not isinstance(selected_release_ids, list) or not selected_release_ids:
        raise RuntimeError("Runtime adapter missing selected releases for visual request.")
    action_type = str(payload.get("action_type") or "BULK_GENERATE_PREVIEWS").strip()
    result = create_visual_batch_preview_session(
        conn,
        action_type=action_type,
        selected_release_ids=[_as_int(item, field_name="selected_release_ids") for item in selected_release_ids],
        created_by=str(payload.get("created_by") or "prompt_runtime"),
        action_payload=payload.get("action_payload") if isinstance(payload.get("action_payload"), dict) else {},
    )
    session_id = str(result["preview_session_id"])
    artifact_ref = f"release_visual_batch_preview_sessions:{session_id}"
    product_target = {"artifact_ref": artifact_ref, "target_kind": "visual_request", "session_id": session_id, "status": "OPEN"}
    _record_usage_artifact(conn, execution_group_id=execution_group_id, execution_attempt_id=execution_attempt_id, artifact_ref=artifact_ref, product_target=product_target)
    return {"result_code": _SYNC_RESULT_CODE[capability_code], "secret_safe_message": "Visual preview request created.", "artifact_ref": artifact_ref}


def _create_analytics_request(payload: dict) -> dict:
    conn, execution_group_id, execution_attempt_id, capability_code, target_type, target_id = _runtime_context(payload)
    scope_type = str(payload.get("report_scope_type") or ("CHANNEL" if target_type == "channel" else "OVERVIEW")).strip().upper()
    if scope_type not in {"OVERVIEW", "CHANNEL", "RELEASE", "BATCH_MONTH"}:
        raise RuntimeError("Runtime adapter invalid analytics report scope.")
    artifact_type = str(payload.get("artifact_type") or "API_REPORT").strip().upper()
    if artifact_type not in {"XLSX", "STRUCTURED_REPORT", "API_REPORT"}:
        raise RuntimeError("Runtime adapter invalid analytics artifact type.")
    scope_ref = payload.get("report_scope_ref")
    if scope_ref is None and scope_type != "OVERVIEW":
        scope_ref = target_id
    now = dbm.now_ts()
    cur = conn.execute(
        """
        INSERT INTO analytics_report_records(report_scope_type,report_scope_ref,report_family,filter_payload_json,artifact_type,artifact_ref,generation_status,created_at,created_by)
        VALUES(?,?,?,?,?,NULL,'PENDING',?,?)
        """,
        (scope_type, scope_ref, str(payload.get("report_family") or "PROMPT_RUNTIME"), _json_dumps(payload.get("filter_payload") if isinstance(payload.get("filter_payload"), dict) else {}), artifact_type, now, str(payload.get("created_by") or "prompt_runtime")),
    )
    report_id = int(cur.lastrowid)
    artifact_ref = f"analytics_report_records:{report_id}"
    product_target = {"artifact_ref": artifact_ref, "target_kind": "analytics_request", "report_record_id": report_id, "status": "PENDING"}
    _record_usage_artifact(conn, execution_group_id=execution_group_id, execution_attempt_id=execution_attempt_id, artifact_ref=artifact_ref, product_target=product_target)
    return {"result_code": _SYNC_RESULT_CODE[capability_code], "secret_safe_message": "Analytics report request created.", "artifact_ref": artifact_ref}


_SYNC_ADAPTERS: dict[str, AdapterFn] = {
    "CREATE_BULK_JSON_DRAFT": _create_bulk_json_draft,
    "CREATE_METADATA_REQUEST": _create_metadata_request,
    "CREATE_VISUAL_REQUEST": _create_visual_request,
    "CREATE_ANALYTICS_REQUEST": _create_analytics_request,
}


def build_default_runtime_adapter_registry() -> RuntimeAdapterRegistry:
    registry = RuntimeAdapterRegistry()
    for capability_code, adapter in _SYNC_ADAPTERS.items():
        registry.register(capability_code, adapter)
    return registry
