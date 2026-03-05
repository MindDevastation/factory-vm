from __future__ import annotations

import logging
import sqlite3
import time
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


def create_planner_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/planner", tags=["planner"])

    @router.get("/releases")
    def planner_list_releases(
        request: Request,
        channel_slug: str | None = None,
        content_type: str | None = None,
        q: str = "",
        sort_by: str = "created_at",
        sort_dir: str = "desc",
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
        if sort_by_value not in PlannedReleaseService.SORT_ALLOWLIST:
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
            result = svc.list(
                PlannedReleaseListParams(
                    channel_slug=(channel_slug.strip() or None) if channel_slug else None,
                    content_type=(content_type.strip() or None) if content_type else None,
                    search=(q.strip() or None),
                    sort_by=sort_by_value,
                    sort_dir=sort_dir_value,
                    limit=page_size,
                    offset=(page - 1) * page_size,
                )
            )
            items = [_release_dto(row) for row in result["items"]]
            return {
                "items": items,
                "pagination": {
                    "page": page,
                    "page_size": result["limit"],
                    "total": result["total"],
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
                    "q_len": len(q),
                    "sort_by": sort_by_value,
                    "sort_dir": sort_dir_value,
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

    return router
