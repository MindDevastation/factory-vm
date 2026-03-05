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
from services.planner.duration import DurationValidationError
from services.planner.planned_release_service import (
    PlannedReleaseConflictError,
    PlannedReleaseListParams,
    PlannedReleaseLockedError,
    PlannedReleaseNotFoundError,
    PlannedReleaseService,
)
from services.planner.series import generate_bulk_publish_ats
from services.planner.time_normalization import PublishAtValidationError, normalize_publish_at

logger = logging.getLogger(__name__)


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
        payload: dict[str, Any] = {}

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

        channel_slug = payload.get("channel_slug")
        content_type = payload.get("content_type")
        count = payload.get("count")
        start_publish_at = payload.get("start_publish_at")
        step = payload.get("step")
        mode = str(payload.get("mode", "strict")).strip().lower() or "strict"
        title = payload.get("title")
        notes = payload.get("notes")

        if not isinstance(channel_slug, str) or not channel_slug.strip():
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "channel_slug must not be empty", status_code=status_code, request_id=request_id)
        if not isinstance(content_type, str) or not content_type.strip():
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "content_type must not be empty", status_code=status_code, request_id=request_id)
        if not isinstance(count, int):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "count must be integer", status_code=status_code, request_id=request_id)
        if start_publish_at is not None and not isinstance(start_publish_at, str):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "start_publish_at must be string or null", status_code=status_code, request_id=request_id)
        if step is not None and not isinstance(step, str):
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "step must be string", status_code=status_code, request_id=request_id)
        if mode not in {"strict", "replace"}:
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "mode must be strict or replace", status_code=status_code, request_id=request_id)

        try:
            publish_ats = generate_bulk_publish_ats(
                count=count,
                start_publish_at=start_publish_at,
                step=step,
            )
        except DurationValidationError:
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "step must be ISO8601 duration (PnD, PTnH, PTnM, PTnHnM)", status_code=status_code, request_id=request_id)
        except PublishAtValidationError:
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", "start_publish_at must be ISO8601 datetime", status_code=status_code, request_id=request_id)
        except ValueError as exc:
            status_code = 400
            return planner_error("PLR_INVALID_INPUT", str(exc), status_code=status_code, request_id=request_id)

        conn = dbm.connect(env)
        try:
            if not _channel_exists(conn, channel_slug.strip()):
                status_code = 404
                return planner_error(
                    "PLR_CHANNEL_NOT_FOUND",
                    "channel not found",
                    status_code=status_code,
                    request_id=request_id,
                )

            svc = PlannedReleaseService(conn)
            result = svc.bulk_create(
                channel_slug=channel_slug.strip(),
                content_type=content_type.strip(),
                title=title,
                notes=notes,
                publish_ats=publish_ats,
                mode=mode,
            )
            return result
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
                    "channel_slug": channel_slug,
                    "content_type": content_type,
                    "count": count,
                    "mode": mode,
                    "has_start_publish_at": start_publish_at is not None,
                },
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
