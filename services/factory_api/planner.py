from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.planner_common import (
    log_planner_event,
    planner_auth_username,
    planner_error,
    planner_request_id,
)
from services.planner.planned_release_service import (
    PlannedReleaseConflictError,
    PlannedReleaseListParams,
    PlannedReleaseLockedError,
    PlannedReleaseNotFoundError,
    PlannedReleaseService,
)
from services.planner.import_service import (
    PlannerImportConfirmConflictError,
    PlannerImportParseError,
    PlannerImportPreviewNotConfirmableError,
    PlannerImportPreviewService,
    PlannerImportTooManyRowsError,
)
from services.planner.preview_store import (
    PreviewAlreadyUsedError,
    PreviewExpiredError,
    PreviewNotFoundError,
    PreviewStore,
    PreviewUsernameMismatchError,
)
from services.planner.materialization_service import (
    PlannerMaterializationError,
    PlannerMaterializationService,
)
from services.planner.materialization_foundation import (
    derive_binding_diagnostics_inputs,
    derive_materialization_state_summary_inputs,
    get_planned_release_by_id,
    validate_binding_invariants,
)
from services.planner.release_job_creation_foundation import (
    ReleaseJobCreationFoundationError,
    derive_job_creation_state_summary_inputs,
    derive_open_job_diagnostics_inputs,
    get_release_by_id,
    validate_open_job_invariants,
)
from services.planner.release_job_creation_service import ReleaseJobCreationError, ReleaseJobCreationService
from services.planner.metadata_bulk_preview_service import (
    MetadataBulkPreviewError,
    apply_bulk_preview_session,
    create_bulk_preview_session,
    get_bulk_preview_session,
    load_bulk_context,
)
from services.planner.mass_actions_preview_service import (
    PlannerMassActionPreviewError,
    create_mass_action_preview_session,
    get_mass_action_preview_session,
)
from services.planner.planned_release_readiness_service import (
    PlannedReleaseReadinessNotFoundError,
    PlannedReleaseReadinessService,
)
from services.planner.series import BulkSeriesInput, BulkSeriesValidationError, build_bulk_publish_ats
from services.planner.time_normalization import PublishAtValidationError, normalize_publish_at

logger = logging.getLogger(__name__)
_preview_store = PreviewStore()


def _require_planner_auth(env: Env):
    async def _dep(request: Request) -> str:
        return planner_auth_username(request, env)

    return _dep


def _release_dto(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "channel_slug": row["channel_slug"],
        "content_type": row["content_type"],
        "title": row["title"],
        "publish_at": row["publish_at"],
        "notes": row["notes"],
        "status": row["status"],
        "materialized_release_id": row.get("materialized_release_id"),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _readiness_summary(readiness: dict[str, Any]) -> dict[str, Any]:
    summary = readiness.get("summary") or {}
    primary_reason = readiness.get("primary_reason") or {}
    return {
        "aggregate_status": readiness.get("aggregate_status"),
        "blocked_domains_count": int(summary.get("blocked_domains") or 0),
        "not_ready_domains_count": int(summary.get("not_ready_domains") or 0),
        "computed_at": readiness.get("computed_at"),
        "primary_reason": primary_reason.get("message"),
        "primary_remediation_hint": readiness.get("primary_remediation_hint"),
    }


def _readiness_unavailable_payload() -> dict[str, Any]:
    return {
        "aggregate_status": None,
        "error": {
            "code": "PRS_READINESS_UNAVAILABLE",
            "message": "Readiness could not be computed for this item.",
        },
    }


def _parse_readiness_status_filter(readiness_status: str | None) -> set[str]:
    text = (readiness_status or "").strip()
    if not text:
        return set()
    allowed = {"NOT_READY", "BLOCKED", "READY_FOR_MATERIALIZATION"}
    out: set[str] = set()
    for part in text.split(","):
        value = part.strip()
        if not value:
            continue
        if value not in allowed:
            raise ValueError("readiness_status contains invalid value")
        out.add(value)
    if not out:
        raise ValueError("readiness_status contains invalid value")
    return out


def _parse_readiness_problem_filter(readiness_problem: str | None) -> set[str]:
    text = (readiness_problem or "").strip()
    if not text:
        return set()
    mapping = {
        "attention_required": {"NOT_READY", "BLOCKED"},
        "blocked_only": {"BLOCKED"},
        "ready_only": {"READY_FOR_MATERIALIZATION"},
    }
    if text not in mapping:
        raise ValueError("readiness_problem contains invalid value")
    return mapping[text]


def _parse_materialized_state_filter(materialized_state: str | None) -> set[str]:
    text = (materialized_state or "").strip()
    if not text:
        return set()
    allowed = {"materialized", "not_materialized", "binding_inconsistent"}
    out: set[str] = set()
    for part in text.split(","):
        value = part.strip()
        if not value:
            continue
        if value not in allowed:
            raise ValueError("materialized_state contains invalid value")
        out.add(value)
    if not out:
        raise ValueError("materialized_state contains invalid value")
    return out


def _parse_job_creation_state_filter(job_creation_state: str | None) -> set[str]:
    text = (job_creation_state or "").strip()
    if not text:
        return set()
    allowed = {"has_open_job", "no_open_job", "inconsistent_open_job_state"}
    out: set[str] = set()
    for part in text.split(","):
        value = part.strip()
        if not value:
            continue
        if value not in allowed:
            raise ValueError("job_creation_state contains invalid value")
        out.add(value)
    if not out:
        raise ValueError("job_creation_state contains invalid value")
    return out


def _build_job_creation_surface_payload(
    conn: sqlite3.Connection,
    *,
    planned_release: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding_result = validate_binding_invariants(conn, planned_release=planned_release)
    release_id_raw = planned_release.get("materialized_release_id")
    if binding_result.invariant_status != "OK" or release_id_raw is None:
        action_reason = "Materialized release is required"
        if binding_result.invariant_status != "OK":
            action_reason = "Binding inconsistent"
        summary = {
            "job_creation_state": "ACTION_DISABLED",
            "job_id": None,
            "action_reason": action_reason,
        }
        diagnostics = {
            "release_id": release_id_raw,
            "current_open_job_id": None,
            "linked_job_exists": False,
            "open_jobs_count": 0,
            "invariant_status": "ACTION_DISABLED",
            "invariant_reason": action_reason,
        }
        return summary, diagnostics

    release = get_release_by_id(conn, release_id=int(release_id_raw))
    if release is None:
        return (
            {
                "job_creation_state": "ACTION_DISABLED",
                "job_id": None,
                "action_reason": "Materialized release missing",
            },
            {
                "release_id": int(release_id_raw),
                "current_open_job_id": None,
                "linked_job_exists": False,
                "open_jobs_count": 0,
                "invariant_status": "ACTION_DISABLED",
                "invariant_reason": "Materialized release missing",
            },
        )

    try:
        diagnostics_obj = validate_open_job_invariants(conn, release=release)
    except ReleaseJobCreationFoundationError as exc:
        state = "CURRENT_POINTER_INCONSISTENT"
        if exc.code == "PRJ_MULTIPLE_OPEN_JOBS":
            state = "MULTIPLE_OPEN_INCONSISTENT"
        return (
            {
                "job_creation_state": state,
                "job_id": exc.details.get("open_job_id") or exc.details.get("current_open_job_id"),
                "action_reason": exc.message,
            },
            {
                "release_id": int(release_id_raw),
                "current_open_job_id": exc.details.get("current_open_job_id"),
                "linked_job_exists": bool(exc.details.get("open_job_id") or exc.details.get("current_open_job_id")),
                "open_jobs_count": int(exc.details.get("open_jobs_count") or 0),
                "invariant_status": state,
                "invariant_reason": exc.message,
            },
        )
    action_enabled = bool(str(release.get("origin_meta_file_id") or "").strip())
    summary_raw = derive_job_creation_state_summary_inputs(
        release=release,
        diagnostics=diagnostics_obj,
        action_enabled=action_enabled,
    )
    summary = {
        "job_creation_state": summary_raw.get("job_creation_state"),
        "job_id": diagnostics_obj.current_open_job_id if diagnostics_obj.invariant_status == "HAS_OPEN_JOB" else None,
        "action_reason": None if bool(summary_raw.get("action_enabled")) else "Release is not currently eligible for job creation.",
    }
    if summary["job_creation_state"] == "ACTION_DISABLED" and not summary["action_reason"]:
        summary["action_reason"] = "Job creation action is currently unavailable"
    diagnostics = derive_open_job_diagnostics_inputs(diagnostics=diagnostics_obj)
    return summary, diagnostics


def _matches_job_creation_state_filter(summary: dict[str, Any], filters: set[str]) -> bool:
    if not filters:
        return True
    state = str(summary.get("job_creation_state") or "")
    options: set[str] = set()
    if state == "HAS_OPEN_JOB":
        options.add("has_open_job")
    if state == "NO_OPEN_JOB":
        options.add("no_open_job")
    if state in {"MULTIPLE_OPEN_INCONSISTENT", "CURRENT_POINTER_INCONSISTENT"}:
        options.add("inconsistent_open_job_state")
    return bool(options & filters)


def _materialization_action_reason(*, state_value: str, readiness_status: str | None) -> str | None:
    if state_value == "BINDING_INCONSISTENT":
        return "Binding inconsistent"
    if state_value == "ALREADY_MATERIALIZED":
        return "Already materialized"
    if readiness_status == "BLOCKED":
        return "Blocked"
    if readiness_status == "NOT_READY":
        return "Not ready for materialization"
    if state_value == "ACTION_DISABLED":
        return "Materialization action is currently unavailable"
    return None


def _normalize_materialization_state_for_surface(summary: dict[str, Any]) -> dict[str, Any]:
    out = dict(summary)
    if out.get("materialized_release_id") is not None and out.get("invariant_status") == "OK":
        out["materialization_state"] = "ALREADY_MATERIALIZED"
    return out


def _matches_materialized_state_filter(summary: dict[str, Any], filters: set[str]) -> bool:
    if not filters:
        return True
    state_value = str(summary.get("materialization_state") or "")
    materialized_release_id = summary.get("materialized_release_id")
    options: set[str] = set()
    if state_value == "ALREADY_MATERIALIZED" or (
        materialized_release_id is not None and str(summary.get("invariant_status") or "") == "OK"
    ):
        options.add("materialized")
    if state_value == "BINDING_INCONSISTENT":
        options.add("binding_inconsistent")
    if state_value in {"NOT_MATERIALIZED", "ACTION_DISABLED"} and materialized_release_id is None:
        options.add("not_materialized")
    return bool(options & filters)


def _readiness_rank(status: str, *, readiness_priority: str) -> int:
    if readiness_priority == "attention_first":
        return {"BLOCKED": 0, "NOT_READY": 1, "READY_FOR_MATERIALIZATION": 2}.get(status, 99)
    return {"READY_FOR_MATERIALIZATION": 0, "NOT_READY": 1, "BLOCKED": 2}.get(status, 99)


def _iso_sort_key_asc_nulls_last(value: str | None) -> float:
    text = str(value or "").strip()
    if not text:
        return float("inf")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return float("inf")


def _evaluate_readiness_tolerant(
    readiness_svc: PlannedReleaseReadinessService,
    planned_release_ids: list[int],
) -> tuple[dict[int, dict[str, Any]], set[int]]:
    if not planned_release_ids:
        return {}, set()
    try:
        return readiness_svc.evaluate_many(planned_release_ids=planned_release_ids), set()
    except Exception:
        if len(planned_release_ids) == 1:
            return {}, {int(planned_release_ids[0])}
        mid = len(planned_release_ids) // 2
        left_map, left_failed = _evaluate_readiness_tolerant(readiness_svc, planned_release_ids[:mid])
        right_map, right_failed = _evaluate_readiness_tolerant(readiness_svc, planned_release_ids[mid:])
        left_map.update(right_map)
        return left_map, left_failed | right_failed


def _build_readiness_summary(scope_ids: list[int], readiness_map: dict[int, dict[str, Any]], unavailable_ids: set[int]) -> dict[str, Any]:
    scoped_unavailable_ids = {release_id for release_id in scope_ids if release_id in unavailable_ids}
    summary = {
        "scope_total": len(scope_ids),
        "ready_for_materialization": 0,
        "not_ready": 0,
        "blocked": 0,
        "unavailable": len(scoped_unavailable_ids),
        "attention_count": 0,
        "computed_at": None,
    }
    computed_at_values: list[str] = []
    for release_id in scope_ids:
        if release_id in unavailable_ids:
            continue
        aggregate_status = str((readiness_map.get(release_id) or {}).get("aggregate_status") or "")
        if aggregate_status == "READY_FOR_MATERIALIZATION":
            summary["ready_for_materialization"] += 1
        elif aggregate_status == "NOT_READY":
            summary["not_ready"] += 1
        elif aggregate_status == "BLOCKED":
            summary["blocked"] += 1
        computed_at = str((readiness_map.get(release_id) or {}).get("computed_at") or "").strip()
        if computed_at:
            computed_at_values.append(computed_at)
    summary["attention_count"] = summary["not_ready"] + summary["blocked"]
    if computed_at_values:
        summary["computed_at"] = max(computed_at_values)
    return summary


def _created_at_sort_key_desc(value: str) -> float:
    text = (value or "").strip()
    if not text:
        return float("inf")
    try:
        return -datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return float("inf")


def _channel_exists(conn: sqlite3.Connection, channel_slug: str) -> bool:
    row = conn.execute("SELECT 1 FROM channels WHERE slug = ? LIMIT 1", (channel_slug,)).fetchone()
    return bool(row)


def _extract_multipart_file(request: Request) -> tuple[str, bytes]:
    content_type = str(request.headers.get("content-type") or "")
    prefix = "boundary="
    if "multipart/form-data" not in content_type.lower() or prefix not in content_type:
        raise ValueError("multipart/form-data with boundary is required")

    boundary = content_type.split(prefix, 1)[1].strip()
    boundary = boundary.strip('"')
    if not boundary:
        raise ValueError("multipart boundary is required")

    payload = request.scope.get("_body")
    if payload is None:
        raise ValueError("multipart body is missing")

    marker = f"--{boundary}".encode("utf-8")
    sections = payload.split(marker)
    for section in sections:
        part = section.strip()
        if not part or part == b"--":
            continue
        if b"\r\n\r\n" not in part:
            continue
        headers_raw, body = part.split(b"\r\n\r\n", 1)
        if body.endswith(b"\r\n"):
            body = body[:-2]
        if body.endswith(b"--"):
            body = body[:-2]

        header_lines = headers_raw.decode("utf-8", errors="ignore").split("\r\n")
        disposition = next((line for line in header_lines if line.lower().startswith("content-disposition:")), "")
        if 'name="file"' not in disposition:
            continue

        filename = "upload"
        filename_key = 'filename="'
        if filename_key in disposition:
            filename = disposition.split(filename_key, 1)[1].split('"', 1)[0] or "upload"
        return filename, body

    raise ValueError("file part is required")


def _parse_planner_item_ids_query(value: str) -> list[int]:
    parts = [item.strip() for item in value.split(",") if item.strip()]
    if not parts:
        raise ValueError("planner_item_ids must include at least one integer id")
    out: list[int] = []
    for item in parts:
        try:
            out.append(int(item))
        except Exception as exc:
            raise ValueError("planner_item_ids must be a comma-separated integer list") from exc
    return out


def _parse_bulk_preview_payload(payload: Any) -> tuple[list[int], list[str] | None, dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("request body must be a JSON object")

    raw_item_ids = payload.get("planner_item_ids")
    if not isinstance(raw_item_ids, list):
        raise ValueError("planner_item_ids must be an integer array")
    planner_item_ids: list[int] = []
    for item in raw_item_ids:
        if isinstance(item, bool) or not isinstance(item, int):
            raise ValueError("planner_item_ids must be an integer array")
        planner_item_ids.append(item)

    raw_fields = payload.get("fields")
    fields: list[str] | None = None
    if raw_fields is not None:
        if not isinstance(raw_fields, list):
            raise ValueError("fields must be a string array")
        fields = []
        for item in raw_fields:
            if not isinstance(item, str):
                raise ValueError("fields must be a string array")
            fields.append(item)

    raw_overrides = payload.get("overrides")
    if raw_overrides is None:
        overrides = {}
    elif isinstance(raw_overrides, dict):
        overrides = raw_overrides
    else:
        raise ValueError("overrides must be an object")
    return planner_item_ids, fields, overrides


def _parse_bulk_apply_payload(payload: Any) -> tuple[list[int], list[str], dict[str, list[str]]]:
    if not isinstance(payload, dict):
        raise ValueError("request body must be a JSON object")
    raw_items = payload.get("selected_items")
    if not isinstance(raw_items, list):
        raise ValueError("selected_items must be an integer array")
    selected_items: list[int] = []
    for item in raw_items:
        if isinstance(item, bool) or not isinstance(item, int):
            raise ValueError("selected_items must be an integer array")
        selected_items.append(item)
    raw_fields = payload.get("selected_fields")
    if not isinstance(raw_fields, list):
        raise ValueError("selected_fields must be a string array")
    selected_fields: list[str] = []
    for field in raw_fields:
        if not isinstance(field, str):
            raise ValueError("selected_fields must be a string array")
        selected_fields.append(field)
    raw_confirmed = payload.get("overwrite_confirmed")
    if raw_confirmed is None:
        confirmed: dict[str, list[str]] = {}
    elif isinstance(raw_confirmed, dict):
        confirmed = {}
        for key, value in raw_confirmed.items():
            if not isinstance(key, str):
                raise ValueError("overwrite_confirmed keys must be planner_item_id strings")
            if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
                raise ValueError("overwrite_confirmed values must be string arrays")
            confirmed[key] = list(value)
    else:
        raise ValueError("overwrite_confirmed must be an object")
    return selected_items, selected_fields, confirmed


def _parse_mass_action_preview_payload(payload: Any) -> tuple[str, list[int]]:
    if not isinstance(payload, dict):
        raise ValueError("body must be object")
    action_type = str(payload.get("action_type") or "").strip()
    selected_item_ids = payload.get("selected_item_ids")
    if not isinstance(selected_item_ids, list):
        raise ValueError("selected_item_ids must be an integer array")
    normalized_ids: list[int] = []
    for item in selected_item_ids:
        if not isinstance(item, int):
            raise ValueError("selected_item_ids must be an integer array")
        normalized_ids.append(item)
    return action_type, normalized_ids


def create_planner_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/planner", tags=["planner"])

    @router.get("/metadata-bulk/context")
    async def planner_metadata_bulk_context(
        planner_item_ids: str,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"
        selected_count = 0
        if not username:
            result_status = "error"
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)
        try:
            item_ids = _parse_planner_item_ids_query(planner_item_ids)
            selected_count = len(item_ids)
        except ValueError as exc:
            result_status = "error"
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", str(exc), status_code=status_code, request_id=request_id)
        conn = dbm.connect(env)
        try:
            return load_bulk_context(conn, planner_item_ids=item_ids)
        except MetadataBulkPreviewError as exc:
            result_status = "error"
            status_code = 422
            return planner_error(exc.code, exc.message, status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_metadata_bulk_context",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={"selected_item_count": selected_count, "result_status": result_status},
            )

    @router.post("/metadata-bulk/preview")
    async def planner_metadata_bulk_preview(
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"
        selected_count = 0
        selected_fields: list[str] = []
        if not username:
            result_status = "error"
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)
        try:
            payload = await request.json()
        except Exception:
            result_status = "error"
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "invalid JSON body", status_code=status_code, request_id=request_id)
        try:
            planner_item_ids, fields, overrides = _parse_bulk_preview_payload(payload)
            selected_count = len(planner_item_ids)
            selected_fields = list(fields or [])
        except ValueError as exc:
            result_status = "error"
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", str(exc), status_code=status_code, request_id=request_id)
        conn = dbm.connect(env)
        try:
            return create_bulk_preview_session(
                conn,
                planner_item_ids=planner_item_ids,
                fields=fields,
                overrides=overrides,
                created_by=username,
                ttl_seconds=env.metadata_bulk_preview_ttl_sec,
            )
        except MetadataBulkPreviewError as exc:
            result_status = "error"
            status_code = 422
            return planner_error(exc.code, exc.message, status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_metadata_bulk_preview",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "selected_item_count": selected_count,
                    "selected_fields": selected_fields,
                    "result_status": result_status,
                },
            )

    @router.get("/metadata-bulk/sessions/{session_id}")
    async def planner_metadata_bulk_session(
        session_id: str,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"
        if not username:
            result_status = "error"
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)
        conn = dbm.connect(env)
        try:
            return get_bulk_preview_session(conn, session_id=session_id)
        except MetadataBulkPreviewError as exc:
            result_status = "error"
            code = 404 if exc.code == "MBP_SESSION_NOT_FOUND" else 422
            status_code = code
            return planner_error(exc.code, exc.message, status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_metadata_bulk_session",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={"session_id": session_id, "result_status": result_status},
            )

    @router.post("/metadata-bulk/sessions/{session_id}/apply")
    async def planner_metadata_bulk_apply(
        session_id: str,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"
        selected_count = 0
        selected_fields: list[str] = []
        if not username:
            result_status = "error"
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)
        try:
            payload = await request.json()
        except Exception:
            result_status = "error"
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "invalid JSON body", status_code=status_code, request_id=request_id)
        try:
            selected_items, selected_fields, overwrite_confirmed = _parse_bulk_apply_payload(payload)
            selected_count = len(selected_items)
        except ValueError as exc:
            result_status = "error"
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", str(exc), status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            return apply_bulk_preview_session(
                conn,
                session_id=session_id,
                selected_items=selected_items,
                selected_fields=selected_fields,
                overwrite_confirmed=overwrite_confirmed,
            )
        except MetadataBulkPreviewError as exc:
            result_status = "error"
            status_code = 404 if exc.code == "MBP_SESSION_NOT_FOUND" else 422
            return planner_error(exc.code, exc.message, status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_metadata_bulk_apply",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "session_id": session_id,
                    "selected_item_count": selected_count,
                    "selected_fields": selected_fields,
                    "result_status": result_status,
                },
            )

    @router.post("/mass-actions/preview")
    async def planner_mass_actions_preview(
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"
        selected_count = 0
        action_type = ""
        if not username:
            result_status = "error"
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)
        try:
            payload = await request.json()
        except Exception:
            result_status = "error"
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "invalid JSON body", status_code=status_code, request_id=request_id)
        try:
            action_type, selected_item_ids = _parse_mass_action_preview_payload(payload)
            selected_count = len(selected_item_ids)
        except ValueError as exc:
            result_status = "error"
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", str(exc), status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            return create_mass_action_preview_session(
                conn,
                action_type=action_type,
                selected_item_ids=selected_item_ids,
                created_by=username,
                ttl_seconds=env.planner_mass_action_preview_ttl_sec,
            )
        except PlannerMassActionPreviewError as exc:
            result_status = "error"
            status_code = 404 if exc.code == "PMA_SESSION_NOT_FOUND" else 422
            log_planner_event(
                logger,
                event_name="planner.mass_action.preview_failed",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "session_id": None,
                    "action_type": action_type or None,
                    "selected_count": selected_count,
                    "created_new_count": 0,
                    "returned_existing_count": 0,
                    "skipped_count": 0,
                    "failed_count": 0,
                    "stale_session_flag": False,
                    "error_codes": [exc.code],
                },
            )
            return planner_error(exc.code, exc.message, status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner.mass_action.preview_created" if result_status == "ok" else "planner_mass_actions_preview",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "action_type": action_type or None,
                    "selected_count": selected_count,
                    "result_status": result_status,
                },
            )

    @router.get("/mass-actions/{session_id}")
    async def planner_mass_actions_preview_session(
        session_id: str,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"
        if not username:
            status_code = 401
            result_status = "error"
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)
        conn = dbm.connect(env)
        try:
            return get_mass_action_preview_session(conn, session_id=session_id)
        except PlannerMassActionPreviewError as exc:
            status_code = 404 if exc.code == "PMA_SESSION_NOT_FOUND" else 422
            result_status = "error"
            return planner_error(exc.code, exc.message, status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_mass_actions_session_get",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={"session_id": session_id, "result_status": result_status},
            )

    @router.get("/releases")
    def planner_list_releases(
        request: Request,
        channel_slug: str | None = None,
        content_type: str | None = None,
        status: str | None = None,
        q: str = "",
        sort_by: str = "created_at",
        sort_dir: str = "desc",
        include_readiness: bool = False,
        include_readiness_summary: bool = False,
        readiness_status: str | None = None,
        readiness_problem: str | None = None,
        materialized_state: str | None = None,
        job_creation_state: str | None = None,
        readiness_priority: str | None = None,
        page: int = 1,
        page_size: int = 50,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200

        if not username:
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        sort_by_value = sort_by.strip() or "created_at"
        sort_dir_value = sort_dir.strip().lower() or "desc"
        if page < 1 or page_size < 1:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "page and page_size must be >= 1",
                status_code=status_code,
                request_id=request_id,
            )
        try:
            readiness_status_values = _parse_readiness_status_filter(readiness_status)
        except ValueError:
            status_code = 400
            return planner_error(
                "PRS_INVALID_READINESS_FILTER",
                "readiness_status contains invalid value",
                status_code=status_code,
                request_id=request_id,
            )
        try:
            readiness_problem_values = _parse_readiness_problem_filter(readiness_problem)
        except ValueError:
            status_code = 400
            return planner_error(
                "PRS_INVALID_READINESS_FILTER",
                "readiness_problem contains invalid value",
                status_code=status_code,
                request_id=request_id,
            )
        try:
            materialized_state_values = _parse_materialized_state_filter(materialized_state)
        except ValueError:
            status_code = 400
            return planner_error(
                "PRS_INVALID_MATERIALIZED_STATE_FILTER",
                "materialized_state contains invalid value",
                status_code=status_code,
                request_id=request_id,
            )
        try:
            job_creation_state_values = _parse_job_creation_state_filter(job_creation_state)
        except ValueError:
            status_code = 400
            return planner_error(
                "PRS_INVALID_JOB_CREATION_STATE_FILTER",
                "job_creation_state contains invalid value",
                status_code=status_code,
                request_id=request_id,
            )

        effective_readiness_filter = readiness_status_values or readiness_problem_values
        readiness_status_value = ",".join(sorted(readiness_status_values)) if readiness_status_values else None
        readiness_problem_value = readiness_problem.strip() if readiness_problem and readiness_problem.strip() else None

        readiness_sort_requested = sort_by_value == "readiness_priority"
        readiness_priority_value = (readiness_priority or "").strip()
        if sort_by_value not in PlannedReleaseService.SORT_ALLOWLIST and not readiness_sort_requested:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "sort_by is not allowed",
                status_code=status_code,
                request_id=request_id,
            )
        if readiness_sort_requested and readiness_priority_value not in {"attention_first", "ready_first"}:
            status_code = 400
            return planner_error(
                "PRS_INVALID_READINESS_SORT",
                "readiness_priority contains invalid value",
                status_code=status_code,
                request_id=request_id,
            )
        if sort_dir_value not in {"asc", "desc"}:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "sort_dir must be asc or desc",
                status_code=status_code,
                request_id=request_id,
            )

        conn = dbm.connect(env)
        try:
            svc = PlannedReleaseService(conn)
            base_params = PlannedReleaseListParams(
                channel_slug=(channel_slug.strip() or None) if channel_slug else None,
                content_type=(content_type.strip() or None) if content_type else None,
                status=(status.strip() or None) if status else None,
                search=(q.strip() or None),
                sort_by=sort_by_value if not readiness_sort_requested else "created_at",
                sort_dir=sort_dir_value,
                limit=page_size,
                offset=(page - 1) * page_size,
            )

            include_readiness_flag = bool(include_readiness) or bool(include_readiness_summary)
            readiness_requested = include_readiness_flag or bool(effective_readiness_filter)
            readiness_summary_payload: dict[str, Any] | None = None
            unavailable_ids: set[int] = set()
            if readiness_requested or readiness_sort_requested:
                candidates = svc.list_candidate_ids(base_params)
                candidate_ids = [item["id"] for item in candidates]
                readiness_svc = PlannedReleaseReadinessService(conn)
                readiness_map, unavailable_ids = _evaluate_readiness_tolerant(readiness_svc, candidate_ids)
                if materialized_state_values:
                    candidate_rows = svc.list_by_ids(candidate_ids)
                    row_by_id = {int(row["id"]): row for row in candidate_rows}
                    filtered_ids: list[int] = []
                    for release_id in candidate_ids:
                        row = row_by_id.get(int(release_id))
                        if row is None:
                            continue
                        row_dict = dict(row)
                        invariant_result = validate_binding_invariants(conn, planned_release=row_dict)
                        readiness_status_value = str((readiness_map.get(int(release_id)) or {}).get("aggregate_status") or "")
                        action_enabled = readiness_status_value == "READY_FOR_MATERIALIZATION"
                        summary = derive_materialization_state_summary_inputs(
                            planned_release=row_dict,
                            invariant_result=invariant_result,
                            action_enabled=action_enabled,
                        )
                        normalized_summary = _normalize_materialization_state_for_surface(summary)
                        if _matches_materialized_state_filter(normalized_summary, materialized_state_values):
                            filtered_ids.append(int(release_id))
                    candidate_ids = filtered_ids
                if unavailable_ids:
                    log_planner_event(
                        logger,
                        event_name="planner.readiness_surface.readiness_unavailable",
                        username=username,
                        started_at=started_at,
                        status_code=status_code,
                        request_id=request_id,
                        extra_fields={
                            "unavailable_count": len(unavailable_ids),
                        },
                    )
                if effective_readiness_filter:
                    candidate_ids = [
                        release_id
                        for release_id in candidate_ids
                        if (readiness_map.get(release_id) or {}).get("aggregate_status") in effective_readiness_filter
                    ]
                    log_planner_event(
                        logger,
                        event_name="planner.readiness_surface.filter_applied",
                        username=username,
                        started_at=started_at,
                        status_code=status_code,
                        request_id=request_id,
                        extra_fields={
                            "planner_scope_fingerprint": f"{channel_slug}|{content_type}|{status}|{q.strip()}",
                            "include_readiness": int(bool(include_readiness_flag)),
                            "readiness_status_filter": readiness_status_value,
                            "readiness_problem_filter": readiness_problem_value,
                            "sort_by": sort_by_value,
                            "readiness_priority": readiness_priority_value or None,
                            "scope_total": len(candidate_ids),
                            "computed_at": max(
                                [
                                    str((readiness_map.get(release_id) or {}).get("computed_at") or "")
                                    for release_id in candidate_ids
                                    if str((readiness_map.get(release_id) or {}).get("computed_at") or "")
                                ],
                                default=None,
                            ),
                        },
                    )
                if job_creation_state_values:
                    candidate_rows = svc.list_by_ids(candidate_ids)
                    row_by_id = {int(row["id"]): row for row in candidate_rows}
                    filtered_ids: list[int] = []
                    for release_id in candidate_ids:
                        row = row_by_id.get(int(release_id))
                        if row is None:
                            continue
                        row_dict = dict(row)
                        job_creation_state_summary, _ = _build_job_creation_surface_payload(
                            conn,
                            planned_release=row_dict,
                        )
                        if _matches_job_creation_state_filter(job_creation_state_summary, job_creation_state_values):
                            filtered_ids.append(int(release_id))
                    candidate_ids = filtered_ids
                if readiness_sort_requested:
                    publish_rows = svc.list_by_ids(candidate_ids)
                    publish_at_by_id = {int(row["id"]): str(row["publish_at"] or "") for row in publish_rows}
                    candidate_ids = sorted(
                        candidate_ids,
                        key=lambda rid: (
                            _readiness_rank(
                                str((readiness_map.get(rid) or {}).get("aggregate_status") or ""),
                                readiness_priority=readiness_priority_value,
                            ),
                            -int((readiness_map.get(rid) or {}).get("summary", {}).get("blocked_domains") or 0)
                            if readiness_priority_value == "attention_first"
                            else 0,
                            -int((readiness_map.get(rid) or {}).get("summary", {}).get("not_ready_domains") or 0)
                            if readiness_priority_value == "attention_first"
                            else 0,
                            _iso_sort_key_asc_nulls_last(publish_at_by_id.get(rid)),
                            int(rid),
                        ),
                    )
                    log_planner_event(
                        logger,
                        event_name="planner.readiness_surface.sort_applied",
                        username=username,
                        started_at=started_at,
                        status_code=status_code,
                        request_id=request_id,
                        extra_fields={
                            "planner_scope_fingerprint": f"{channel_slug}|{content_type}|{status}|{q.strip()}",
                            "include_readiness": int(bool(include_readiness_flag)),
                            "readiness_status_filter": readiness_status_value,
                            "readiness_problem_filter": readiness_problem_value,
                            "sort_by": sort_by_value,
                            "readiness_priority": readiness_priority_value,
                            "computed_at": max(
                                [
                                    str((readiness_map.get(release_id) or {}).get("computed_at") or "")
                                    for release_id in candidate_ids
                                    if str((readiness_map.get(release_id) or {}).get("computed_at") or "")
                                ],
                                default=None,
                            ),
                        },
                    )

                if include_readiness_flag:
                    readiness_summary_payload = _build_readiness_summary(candidate_ids, readiness_map, unavailable_ids)
                total = len(candidate_ids)
                start = (page - 1) * page_size
                stop = start + page_size
                page_ids = candidate_ids[start:stop]
                page_rows = svc.list_by_ids(page_ids)
                row_by_id = {int(row["id"]): row for row in page_rows}
                ordered_rows = [row_by_id[rid] for rid in page_ids if rid in row_by_id]
                items = []
                for row in ordered_rows:
                    item = _release_dto(row)
                    item_id = int(item["id"])
                    readiness_payload = readiness_map.get(item_id) or {}
                    if include_readiness_flag:
                        if item_id in unavailable_ids:
                            item["readiness"] = _readiness_unavailable_payload()
                        else:
                            item["readiness"] = _readiness_summary(readiness_payload)
                    row_dict = dict(row)
                    invariant_result = validate_binding_invariants(conn, planned_release=row_dict)
                    readiness_status_value = str(readiness_payload.get("aggregate_status") or "")
                    action_enabled = readiness_status_value == "READY_FOR_MATERIALIZATION"
                    materialization_state_summary = derive_materialization_state_summary_inputs(
                        planned_release=row_dict,
                        invariant_result=invariant_result,
                        action_enabled=action_enabled,
                    )
                    materialization_state_summary = _normalize_materialization_state_for_surface(materialization_state_summary)
                    materialization_state_summary["release_id"] = row_dict.get("materialized_release_id")
                    materialization_state_summary["action_reason"] = _materialization_action_reason(
                        state_value=str(materialization_state_summary.get("materialization_state") or ""),
                        readiness_status=readiness_status_value or None,
                    )
                    item["materialization_state_summary"] = materialization_state_summary
                    item["binding_diagnostics"] = derive_binding_diagnostics_inputs(
                        planned_release=row_dict,
                        invariant_result=invariant_result,
                    )
                    job_creation_state_summary, open_job_diagnostics = _build_job_creation_surface_payload(
                        conn,
                        planned_release=row_dict,
                    )
                    item["job_creation_state_summary"] = job_creation_state_summary
                    item["open_job_diagnostics"] = open_job_diagnostics
                    items.append(item)
                result_limit = page_size
            else:
                source_rows = []
                if materialized_state_values or job_creation_state_values:
                    candidate_ids = [item["id"] for item in svc.list_candidate_ids(base_params)]
                    page_candidate_rows = svc.list_by_ids(candidate_ids)
                    row_by_id = {int(row["id"]): row for row in page_candidate_rows}
                    source_rows = [row_by_id[rid] for rid in candidate_ids if rid in row_by_id]
                else:
                    result = svc.list(base_params)
                    source_rows = list(result["items"])
                    total = int(result["total"])
                    result_limit = int(result["limit"])
                items = []
                for row in source_rows:
                    item = _release_dto(row)
                    row_dict = dict(row)
                    invariant_result = validate_binding_invariants(conn, planned_release=row_dict)
                    materialization_state_summary = derive_materialization_state_summary_inputs(
                        planned_release=row_dict,
                        invariant_result=invariant_result,
                        action_enabled=False,
                    )
                    materialization_state_summary = _normalize_materialization_state_for_surface(materialization_state_summary)
                    materialization_state_summary["release_id"] = row_dict.get("materialized_release_id")
                    materialization_state_summary["action_reason"] = _materialization_action_reason(
                        state_value=str(materialization_state_summary.get("materialization_state") or ""),
                        readiness_status=None,
                    )
                    if not _matches_materialized_state_filter(materialization_state_summary, materialized_state_values):
                        continue
                    item["materialization_state_summary"] = materialization_state_summary
                    item["binding_diagnostics"] = derive_binding_diagnostics_inputs(
                        planned_release=row_dict,
                        invariant_result=invariant_result,
                    )
                    job_creation_state_summary, open_job_diagnostics = _build_job_creation_surface_payload(
                        conn,
                        planned_release=row_dict,
                    )
                    if not _matches_job_creation_state_filter(job_creation_state_summary, job_creation_state_values):
                        continue
                    item["job_creation_state_summary"] = job_creation_state_summary
                    item["open_job_diagnostics"] = open_job_diagnostics
                    items.append(item)
                if materialized_state_values or job_creation_state_values:
                    total = len(items)
                    start = (page - 1) * page_size
                    stop = start + page_size
                    items = items[start:stop]
                    result_limit = page_size
            response: dict[str, Any] = {
                "items": items,
                "pagination": {
                    "page": page,
                    "page_size": result_limit,
                    "total": total,
                },
            }
            if readiness_summary_payload is not None:
                response["readiness_summary"] = readiness_summary_payload
                log_planner_event(
                    logger,
                    event_name="planner.readiness_surface.list_loaded",
                    username=username,
                    started_at=started_at,
                    status_code=status_code,
                    request_id=request_id,
                    extra_fields={
                        "planner_scope_fingerprint": f"{channel_slug}|{content_type}|{status}|{q.strip()}",
                        "include_readiness": int(bool(include_readiness_flag)),
                        "readiness_status_filter": readiness_status_value,
                        "readiness_problem_filter": readiness_problem_value,
                        "sort_by": sort_by_value,
                        "readiness_priority": readiness_priority_value or None,
                        "scope_total": readiness_summary_payload["scope_total"],
                        "ready_count": readiness_summary_payload["ready_for_materialization"],
                        "not_ready_count": readiness_summary_payload["not_ready"],
                        "blocked_count": readiness_summary_payload["blocked"],
                        "unavailable_count": readiness_summary_payload["unavailable"],
                        "computed_at": readiness_summary_payload["computed_at"],
                    },
                )
            return response
        except Exception as exc:
            logger.exception("planner_list_releases_failed request_id=%s", request_id)
            status_code = 500
            return planner_error("PLR_INTERNAL", "planner internal error", status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_list_releases",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "channel_slug": channel_slug,
                    "content_type": content_type,
                    "status": status,
                    "q_len": len(q),
                    "sort_by": sort_by_value,
                    "sort_dir": sort_dir_value,
                    "include_readiness": int(bool(include_readiness)),
                    "include_readiness_summary": int(bool(include_readiness_summary)),
                    "readiness_status": readiness_status_value,
                    "readiness_problem": readiness_problem_value,
                    "materialized_state": ",".join(sorted(materialized_state_values)) if materialized_state_values else None,
                    "job_creation_state": ",".join(sorted(job_creation_state_values)) if job_creation_state_values else None,
                    "readiness_priority": readiness_priority_value or None,
                    "page": page,
                    "page_size": page_size,
                },
            )


    @router.post("/releases/bulk-create", status_code=201)
    async def planner_bulk_create_releases(
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 201

        if not username:
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        try:
            payload = await request.json()
        except Exception:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "body must be valid JSON object",
                status_code=status_code,
                request_id=request_id,
            )

        if not isinstance(payload, dict):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "body must be object", status_code=status_code, request_id=request_id)

        channel_slug = str(payload.get("channel_slug") or "").strip()
        content_type = str(payload.get("content_type") or "").strip()
        if not channel_slug or not content_type:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "channel_slug and content_type are required",
                status_code=status_code,
                request_id=request_id,
            )

        count_raw = payload.get("count", 1)
        mode = str(payload.get("mode") or "strict").strip().lower()
        if mode not in {"strict", "replace"}:
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "mode must be strict or replace", status_code=status_code, request_id=request_id)

        if not isinstance(count_raw, int):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "count must be integer", status_code=status_code, request_id=request_id)

        start_publish_at = payload.get("start_publish_at")
        if start_publish_at is not None:
            start_publish_at = str(start_publish_at)

        step = payload.get("step")
        if step is not None:
            step = str(step)

        conn = dbm.connect(env)
        try:
            if not _channel_exists(conn, channel_slug):
                status_code = 404
                return planner_error(
                    "PLR_CHANNEL_NOT_FOUND",
                    "channel not found",
                    status_code=status_code,
                    request_id=request_id,
                )

            publish_ats = build_bulk_publish_ats(BulkSeriesInput(count=count_raw, start_publish_at=start_publish_at, step=step))

            svc = PlannedReleaseService(conn)
            result = svc.bulk_create_or_replace(
                channel_slug=channel_slug,
                content_type=content_type,
                title=payload.get("title"),
                notes=payload.get("notes"),
                publish_ats=publish_ats,
                mode=mode,
            )
            return result
        except BulkSeriesValidationError as exc:
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", str(exc), status_code=status_code, request_id=request_id)
        except PlannedReleaseConflictError:
            status_code = 409
            return planner_error("PLR_CONFLICT", "conflict", status_code=status_code, request_id=request_id)
        except sqlite3.IntegrityError:
            status_code = 409
            return planner_error("PLR_CONFLICT", "conflict", status_code=status_code, request_id=request_id)
        except Exception:
            logger.exception("planner_bulk_create_releases_failed request_id=%s", request_id)
            status_code = 500
            return planner_error("PLR_INTERNAL", "planner internal error", status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_bulk_create_releases",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "channel_slug": channel_slug if isinstance(payload, dict) else None,
                    "count": payload.get("count") if isinstance(payload, dict) else None,
                    "mode": payload.get("mode") if isinstance(payload, dict) else None,
                },
            )

    @router.post("/releases/bulk-delete")
    async def planner_bulk_delete_releases(
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        payload: Any = None
        ids: list[int] = []

        if not username:
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        try:
            payload = await request.json()
        except Exception:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "body must be valid JSON object",
                status_code=status_code,
                request_id=request_id,
            )

        if not isinstance(payload, dict):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "body must be object", status_code=status_code, request_id=request_id)

        ids_raw = payload.get("ids")
        if not isinstance(ids_raw, list) or any(not isinstance(item, int) for item in ids_raw):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "ids must be integer array", status_code=status_code, request_id=request_id)

        ids = list(ids_raw)
        conn = dbm.connect(env)
        try:
            svc = PlannedReleaseService(conn)
            deleted_count = svc.bulk_delete(ids)
            return {"deleted_count": deleted_count}
        except PlannedReleaseNotFoundError:
            status_code = 404
            return planner_error("PLR_NOT_FOUND", "release not found", status_code=status_code, request_id=request_id)
        except PlannedReleaseLockedError:
            status_code = 409
            return planner_error("PLR_RELEASE_LOCKED", "release is locked", status_code=status_code, request_id=request_id)
        except Exception:
            logger.exception("planner_bulk_delete_releases_failed request_id=%s", request_id)
            status_code = 500
            return planner_error("PLR_INTERNAL", "planner internal error", status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_bulk_delete_releases",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "ids_count": len(ids),
                },
            )

    @router.post("/import/preview")
    async def planner_import_preview(
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200

        if not username:
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            request.scope["_body"] = await request.body()
            filename, payload = _extract_multipart_file(request)
            svc = PlannerImportPreviewService(conn)
            preview = svc.build_preview(filename=filename, payload=payload)
            preview_id = _preview_store.put(username, preview)
            return {
                "preview_id": preview_id,
                "summary": preview["summary"],
                "can_confirm_strict": preview["can_confirm_strict"],
                "can_confirm_replace": preview["can_confirm_replace"],
                "rows": preview["rows"],
            }
        except ValueError:
            status_code = 422
            return planner_error("PLR_PARSE_ERROR", "parse error", status_code=status_code, request_id=request_id)
        except PlannerImportTooManyRowsError:
            status_code = 413
            return planner_error("PLR_TOO_MANY_ROWS", "too many rows", status_code=status_code, request_id=request_id)
        except PlannerImportParseError:
            status_code = 422
            return planner_error("PLR_PARSE_ERROR", "parse error", status_code=status_code, request_id=request_id)
        except Exception:
            logger.exception("planner_import_preview_failed request_id=%s", request_id)
            status_code = 500
            return planner_error("PLR_INTERNAL", "planner internal error", status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_import_preview",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={},
            )

    @router.post("/import/confirm")
    async def planner_import_confirm(
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200

        if not username:
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        try:
            payload = await request.json()
        except Exception:
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "body must be object", status_code=status_code, request_id=request_id)

        if not isinstance(payload, dict):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "body must be object", status_code=status_code, request_id=request_id)

        preview_id = payload.get("preview_id")
        mode = payload.get("mode")
        if not isinstance(preview_id, str) or not preview_id.strip():
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "preview_id is required", status_code=status_code, request_id=request_id)
        if mode not in {"strict", "replace"}:
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "mode must be strict|replace", status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            try:
                preview = _preview_store.reserve(username, preview_id)
            except PreviewExpiredError:
                status_code = 404
                return planner_error("PLR_PREVIEW_EXPIRED", "preview expired", status_code=status_code, request_id=request_id)
            except PreviewAlreadyUsedError:
                status_code = 409
                return planner_error(
                    "PLR_PREVIEW_ALREADY_USED",
                    "preview already used",
                    status_code=status_code,
                    request_id=request_id,
                )
            except (PreviewNotFoundError, PreviewUsernameMismatchError):
                status_code = 404
                return planner_error("PLR_PREVIEW_NOT_FOUND", "preview not found", status_code=status_code, request_id=request_id)

            svc = PlannerImportPreviewService(conn)
            result = svc.confirm_preview(preview=preview, mode=mode)
            return {"ok": True, "mode": mode, **result}
        except PlannerImportPreviewNotConfirmableError:
            _preview_store.release(preview_id)
            status_code = 409
            return planner_error(
                "PLR_PREVIEW_NOT_CONFIRMABLE",
                "preview not confirmable",
                status_code=status_code,
                request_id=request_id,
            )
        except (PlannerImportConfirmConflictError, sqlite3.IntegrityError):
            _preview_store.release(preview_id)
            status_code = 409
            return planner_error("PLR_CONFLICT", "conflict", status_code=status_code, request_id=request_id)
        except Exception:
            _preview_store.release(preview_id)
            logger.exception("planner_import_confirm_failed request_id=%s", request_id)
            status_code = 500
            return planner_error("PLR_INTERNAL", "planner internal error", status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_import_confirm",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={"mode": mode if isinstance(mode, str) else None},
            )

    @router.patch("/releases/{release_id}")
    async def planner_patch_release(
        release_id: int,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200

        if not username:
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        try:
            payload = await request.json()
        except Exception:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "body must be valid JSON object",
                status_code=status_code,
                request_id=request_id,
            )
        if not isinstance(payload, dict):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "body must be object", status_code=status_code, request_id=request_id)
        if "status" in payload:
            status_code = 400
            return planner_error(
                "PLR_FIELD_NOT_EDITABLE",
                "status is not editable",
                status_code=status_code,
                request_id=request_id,
                details={"field": "status"},
            )

        updates: dict[str, Any] = {}
        for field in PlannedReleaseService.EDITABLE_FIELDS:
            if field in payload:
                updates[field] = payload[field]

        if "publish_at" in updates and updates["publish_at"] is not None:
            try:
                updates["publish_at"] = normalize_publish_at(str(updates["publish_at"]))
            except PublishAtValidationError:
                status_code = 400
                return planner_error(
                    "PLR_INVALID_INPUT",
                    "publish_at must be ISO8601 datetime",
                    status_code=status_code,
                    request_id=request_id,
                )

        conn = dbm.connect(env)
        try:
            if "channel_slug" in updates:
                channel_slug = updates["channel_slug"]
                if isinstance(channel_slug, str) and not channel_slug.strip():
                    status_code = 400
                    return planner_error(
                        "PLR_INVALID_INPUT",
                        "channel_slug must not be empty",
                        status_code=status_code,
                        request_id=request_id,
                    )
                if channel_slug and not _channel_exists(conn, str(channel_slug)):
                    status_code = 404
                    return planner_error(
                        "PLR_CHANNEL_NOT_FOUND",
                        "channel not found",
                        status_code=status_code,
                        request_id=request_id,
                    )

            svc = PlannedReleaseService(conn)
            row = svc.update(release_id, updates)
            return _release_dto(row)
        except PlannedReleaseNotFoundError:
            status_code = 404
            return planner_error("PLR_NOT_FOUND", "release not found", status_code=status_code, request_id=request_id)
        except PlannedReleaseLockedError:
            status_code = 409
            return planner_error("PLR_RELEASE_LOCKED", "release is locked", status_code=status_code, request_id=request_id)
        except sqlite3.IntegrityError:
            status_code = 409
            return planner_error("PLR_CONFLICT", "conflict", status_code=status_code, request_id=request_id)
        except Exception:
            logger.exception("planner_patch_release_failed request_id=%s release_id=%s", request_id, release_id)
            status_code = 500
            return planner_error("PLR_INTERNAL", "planner internal error", status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_patch_release",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "release_id": release_id,
                    "updated_fields": sorted(updates.keys()),
                    "title_len": len(str(updates.get("title") or "")) if "title" in updates else None,
                    "notes_len": len(str(updates.get("notes") or "")) if "notes" in updates else None,
                },
            )

    @router.post("/items/{planner_item_id}/materialize")
    async def planner_materialize_item(
        planner_item_id: int,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"

        if not username:
            status_code = 401
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            svc = PlannerMaterializationService(conn)
            out = svc.materialize_or_get(planner_item_id=planner_item_id, created_by=username)
            return {
                "planner_item_id": out.planner_item_id,
                "release_id": out.release_id,
                "planner_status": out.planner_status,
                "materialization_status": out.materialization_status,
            }
        except PlannerMaterializationError as exc:
            result_status = "error"
            status_map = {
                "PLM_NOT_FOUND": 404,
                "PLM_INVALID_STATUS": 409,
                "PLM_INCONSISTENT_STATE": 409,
                "PLM_BINDING_CONFLICT": 409,
                "PLM_INTERNAL": 500,
            }
            status_code = status_map.get(exc.code, 500)
            return planner_error(exc.code, exc.message, status_code=status_code, request_id=request_id)
        except Exception:
            result_status = "error"
            logger.exception("planner_materialize_item_failed request_id=%s planner_item_id=%s", request_id, planner_item_id)
            status_code = 500
            return planner_error("PLM_INTERNAL", "materialization failed", status_code=status_code, request_id=request_id)
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_materialize_item",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "planner_item_id": planner_item_id,
                    "result_status": result_status,
                },
            )

    @router.post("/planned-releases/{planned_release_id}/materialize")
    async def planner_materialize_planned_release(
        planned_release_id: int,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"

        if not username:
            status_code = 401
            result_status = "error"
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            svc = PlannerMaterializationService(conn)
            out = svc.materialize_planned_release(planned_release_id=planned_release_id, created_by=username)
            return {
                "planned_release_id": out.planned_release_id,
                "result": out.result,
                "release": {
                    "id": out.release_id,
                    "channel_slug": out.release_channel_slug,
                },
                "materialized_binding": out.materialized_binding,
                "materialization_state_summary": out.materialization_state_summary,
                "binding_diagnostics": out.binding_diagnostics,
            }
        except PlannerMaterializationError as exc:
            result_status = "error"
            status_map = {
                "PRM_NOT_FOUND": 404,
                "PRM_NOT_READY": 409,
                "PRM_BLOCKED": 409,
                "PRM_BINDING_INCONSISTENT": 409,
                "PRM_INVALID_PLANNED_RELEASE_STATE": 409,
                "PRM_RELEASE_CREATE_FAILED": 500,
                "PRM_CONCURRENCY_CONFLICT": 409,
            }
            status_code = status_map.get(exc.code, 500)
            return JSONResponse(status_code=status_code, content={
                "planned_release_id": exc.planned_release_id or planned_release_id,
                "result": "FAILED",
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                },
                "materialization_state_summary": exc.materialization_state_summary,
                "binding_diagnostics": exc.binding_diagnostics,
            })
        except Exception:
            result_status = "error"
            logger.exception(
                "planner_materialize_planned_release_failed request_id=%s planned_release_id=%s",
                request_id,
                planned_release_id,
            )
            status_code = 500
            return JSONResponse(status_code=status_code, content={
                "planned_release_id": planned_release_id,
                "result": "FAILED",
                "error": {
                    "code": "PRM_RELEASE_CREATE_FAILED",
                    "message": "Materialization failed.",
                    "details": {},
                },
                "materialization_state_summary": None,
                "binding_diagnostics": None,
            })
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_materialize_planned_release",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "planned_release_id": planned_release_id,
                    "result_status": result_status,
                },
            )

    @router.post("/planned-releases/{planned_release_id}/create-job")
    async def planner_create_job_for_planned_release(
        planned_release_id: int,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"

        if not username:
            status_code = 401
            result_status = "error"
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            planned_release = get_planned_release_by_id(conn, planned_release_id=planned_release_id)
            if planned_release is None:
                status_code = 404
                result_status = "error"
                return JSONResponse(status_code=status_code, content={
                    "planned_release_id": planned_release_id,
                    "result": "FAILED",
                    "error": {
                        "code": "PRM_NOT_FOUND",
                        "message": "Planned release was not found.",
                    },
                })
            binding_result = validate_binding_invariants(conn, planned_release=planned_release)
            if binding_result.invariant_status != "OK":
                status_code = 409
                result_status = "error"
                return JSONResponse(status_code=status_code, content={
                    "planned_release_id": planned_release_id,
                    "result": "FAILED",
                    "error": {
                        "code": "PRM_BINDING_INCONSISTENT",
                        "message": "Materialization binding is inconsistent.",
                    },
                    "binding_diagnostics": derive_binding_diagnostics_inputs(
                        planned_release=planned_release,
                        invariant_result=binding_result,
                    ),
                })
            release_id = planned_release.get("materialized_release_id")
            if release_id is None:
                status_code = 409
                result_status = "error"
                return JSONResponse(status_code=status_code, content={
                    "planned_release_id": planned_release_id,
                    "result": "FAILED",
                    "error": {
                        "code": "PRJ_PLANNED_RELEASE_NOT_MATERIALIZED",
                        "message": "Planned release is not materialized.",
                    },
                })
            svc = ReleaseJobCreationService(conn)
            out = svc.create_or_select(release_id=int(release_id))
            return {
                "planned_release_id": planned_release_id,
                "release_id": out.release_id,
                "result": out.result,
                "job": out.job,
                "current_open_relation": out.current_open_relation,
                "job_creation_state_summary": out.job_creation_state_summary,
                "open_job_diagnostics": out.open_job_diagnostics,
            }
        except ReleaseJobCreationError as exc:
            result_status = "error"
            status_map = {
                "PRJ_RELEASE_NOT_FOUND": 404,
                "PRJ_CONCURRENCY_CONFLICT": 409,
                "PRJ_JOB_CREATE_FAILED": 500,
            }
            status_code = status_map.get(exc.code, 422)
            return JSONResponse(status_code=status_code, content={
                "planned_release_id": planned_release_id,
                "result": "FAILED",
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                },
            })
        except Exception:
            status_code = 500
            result_status = "error"
            logger.exception("planner_create_job_for_planned_release_failed request_id=%s planned_release_id=%s", request_id, planned_release_id)
            return JSONResponse(status_code=status_code, content={
                "planned_release_id": planned_release_id,
                "result": "FAILED",
                "error": {
                    "code": "PRJ_JOB_CREATE_FAILED",
                    "message": "Job creation failed.",
                },
            })
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_create_job_for_planned_release",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "planned_release_id": planned_release_id,
                    "result_status": result_status,
                },
            )

    @router.get("/planned-releases/{planned_release_id}/readiness")
    def planner_planned_release_readiness(
        planned_release_id: int,
        request: Request,
        username: str = Depends(_require_planner_auth(env)),
    ):
        started_at = time.perf_counter()
        request_id = planner_request_id(request)
        status_code = 200
        result_status = "ok"

        if not username:
            status_code = 401
            result_status = "error"
            return planner_error("PLR_INVALID_INPUT", "Unauthorized", status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            svc = PlannedReleaseReadinessService(conn)
            payload = svc.evaluate(planned_release_id=planned_release_id)
            log_planner_event(
                logger,
                event_name="planner.readiness_surface.detail_loaded",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "planner_scope_fingerprint": None,
                    "planned_release_id": planned_release_id,
                    "computed_at": payload.get("computed_at"),
                    "aggregate_status": payload.get("aggregate_status"),
                },
            )
            return payload
        except PlannedReleaseReadinessNotFoundError:
            status_code = 404
            result_status = "error"
            return planner_error("PRS_PLANNED_RELEASE_NOT_FOUND", "planned release not found", status_code=status_code, request_id=request_id)
        except Exception:
            status_code = 500
            result_status = "error"
            logger.exception("planner_planned_release_readiness_failed request_id=%s planned_release_id=%s", request_id, planned_release_id)
            return planner_error(
                "PRS_READINESS_EVALUATION_FAILED",
                "planned release readiness evaluation failed",
                status_code=status_code,
                request_id=request_id,
            )
        finally:
            conn.close()
            log_planner_event(
                logger,
                event_name="planner_planned_release_readiness",
                username=username,
                started_at=started_at,
                status_code=status_code,
                request_id=request_id,
                extra_fields={
                    "planned_release_id": planned_release_id,
                    "result_status": result_status,
                },
            )

    return router
