from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request

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
from services.planner.metadata_bulk_preview_service import (
    MetadataBulkPreviewError,
    apply_bulk_preview_session,
    create_bulk_preview_session,
    get_bulk_preview_session,
    load_bulk_context,
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
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _readiness_summary(readiness: dict[str, Any]) -> dict[str, Any]:
    summary = readiness.get("summary") or {}
    primary_reason = readiness.get("primary_reason") or {}
    return {
        "aggregate_status": readiness.get("aggregate_status"),
        "blocked_domains": int(summary.get("blocked_domains") or 0),
        "not_ready_domains": int(summary.get("not_ready_domains") or 0),
        "primary_reason": primary_reason.get("message"),
        "primary_remediation_hint": readiness.get("primary_remediation_hint"),
    }


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

    @router.get("/releases")
    def planner_list_releases(
        request: Request,
        channel_slug: str | None = None,
        content_type: str | None = None,
        status: str | None = None,
        q: str = "",
        sort_by: str = "created_at",
        sort_dir: str = "desc",
        include_readiness_summary: bool = False,
        readiness_status: str | None = None,
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
        readiness_status_value = (readiness_status or "").strip() or None
        if readiness_status_value and readiness_status_value not in {"NOT_READY", "BLOCKED", "READY_FOR_MATERIALIZATION"}:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "readiness_status is not allowed",
                status_code=status_code,
                request_id=request_id,
            )

        readiness_sort_requested = sort_by_value == "readiness_severity"
        if sort_by_value not in PlannedReleaseService.SORT_ALLOWLIST and not readiness_sort_requested:
            status_code = 400
            return planner_error(
                "PLR_INVALID_INPUT",
                "sort_by is not allowed",
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

            readiness_filter_or_sort = bool(readiness_status_value) or readiness_sort_requested
            if readiness_filter_or_sort:
                candidates = svc.list_candidate_ids(base_params)
                candidate_ids = [item["id"] for item in candidates]
                readiness_svc = PlannedReleaseReadinessService(conn)
                readiness_map = readiness_svc.evaluate_many(planned_release_ids=candidate_ids) if candidate_ids else {}
                if readiness_status_value:
                    candidate_ids = [
                        release_id
                        for release_id in candidate_ids
                        if (readiness_map.get(release_id) or {}).get("aggregate_status") == readiness_status_value
                    ]
                if readiness_sort_requested:
                    severity_rank = {"BLOCKED": 0, "NOT_READY": 1, "READY_FOR_MATERIALIZATION": 2}
                    created_at_by_id = {item["id"]: item["created_at"] for item in candidates}
                    candidate_ids = sorted(
                        candidate_ids,
                        key=lambda rid: (
                            severity_rank.get(str((readiness_map.get(rid) or {}).get("aggregate_status") or ""), 99),
                            _created_at_sort_key_desc(str(created_at_by_id.get(rid) or "")),
                            int(rid),
                        ),
                    )

                total = len(candidate_ids)
                start = (page - 1) * page_size
                stop = start + page_size
                page_ids = candidate_ids[start:stop]
                page_rows = svc.list_by_ids(page_ids)
                row_by_id = {int(row["id"]): row for row in page_rows}
                ordered_rows = [row_by_id[rid] for rid in page_ids if rid in row_by_id]
                items = [_release_dto(row) for row in ordered_rows]
                if include_readiness_summary:
                    for item in items:
                        item["readiness"] = _readiness_summary(readiness_map.get(int(item["id"])) or {})
                result_limit = page_size
            else:
                result = svc.list(base_params)
                items = [_release_dto(row) for row in result["items"]]
                total = int(result["total"])
                result_limit = int(result["limit"])
                if include_readiness_summary:
                    page_ids = [int(item["id"]) for item in items]
                    readiness_map = PlannedReleaseReadinessService(conn).evaluate_many(planned_release_ids=page_ids) if page_ids else {}
                    for item in items:
                        item["readiness"] = _readiness_summary(readiness_map.get(int(item["id"])) or {})
            return {
                "items": items,
                "pagination": {
                    "page": page,
                    "page_size": result_limit,
                    "total": total,
                },
            }
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
                    "include_readiness_summary": int(bool(include_readiness_summary)),
                    "readiness_status": readiness_status_value,
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
            return svc.evaluate(planned_release_id=planned_release_id)
        except PlannedReleaseReadinessNotFoundError:
            status_code = 404
            result_status = "error"
            return planner_error("PRR_NOT_FOUND", "planned release not found", status_code=status_code, request_id=request_id)
        except Exception:
            status_code = 500
            result_status = "error"
            logger.exception("planner_planned_release_readiness_failed request_id=%s planned_release_id=%s", request_id, planned_release_id)
            return planner_error(
                "PRR_INTERNAL_EVALUATION_ERROR",
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
