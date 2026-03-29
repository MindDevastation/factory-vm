from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.security import require_basic_auth

ALLOWED_AUDIT_STATUSES: tuple[str, ...] = (
    "unknown",
    "pending",
    "approved",
    "rejected",
    "manual-only",
    "suspended",
)


class AuditStatusMutationPayload(BaseModel):
    confirm: bool
    reason: str
    request_id: str
    status: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _actor_identity_from_request(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return "unknown"
    try:
        raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        username, _ = raw.split(":", 1)
    except Exception:
        return "unknown"
    return username.strip() or "unknown"


def validate_audit_status(status: str) -> str:
    normalized = str(status or "")
    if normalized not in ALLOWED_AUDIT_STATUSES:
        raise ValueError("invalid audit status")
    return normalized


def resolve_effective_audit_status(conn: Any, *, channel_slug: str | None) -> dict[str, Any]:
    project_default_row = conn.execute(
        "SELECT status FROM publish_audit_status_project_defaults WHERE singleton_key = 1"
    ).fetchone()
    project_default_status = str(project_default_row["status"]) if project_default_row else "unknown"

    channel_override_status: str | None = None
    if channel_slug:
        row = conn.execute(
            "SELECT status FROM publish_audit_status_channel_overrides WHERE channel_slug = ?",
            (channel_slug,),
        ).fetchone()
        if row:
            channel_override_status = str(row["status"])

    effective_status = channel_override_status or project_default_status or "unknown"
    return {
        "channel_slug": channel_slug,
        "project_default_status": project_default_status,
        "channel_override_status": channel_override_status,
        "effective_status": effective_status,
    }


def _mutation_error(*, code: str, message: str, request_id: str, status_code: int = 422) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "request_id": request_id}},
    )


def _validate_mutation_payload(payload: AuditStatusMutationPayload) -> str | JSONResponse:
    request_id = str(payload.request_id or "").strip()
    if not request_id:
        return _mutation_error(code="PAS_REQUEST_ID_REQUIRED", message="request_id is required", request_id="", status_code=422)
    if payload.confirm is not True:
        return _mutation_error(
            code="PAS_CONFIRM_REQUIRED",
            message="confirm must be true",
            request_id=request_id,
            status_code=422,
        )
    if not str(payload.reason or "").strip():
        return _mutation_error(
            code="PAS_REASON_REQUIRED",
            message="reason is required",
            request_id=request_id,
            status_code=422,
        )
    try:
        return validate_audit_status(payload.status)
    except ValueError:
        return _mutation_error(
            code="PAS_INVALID_STATUS",
            message="status is invalid",
            request_id=request_id,
            status_code=422,
        )


def _append_history(
    conn: Any,
    *,
    scope_type: str,
    channel_slug: str | None,
    previous_status: str | None,
    status: str,
    reason: str,
    request_id: str,
    actor_identity: str,
) -> None:
    conn.execute(
        """
        INSERT INTO publish_audit_status_history(
            scope_type, channel_slug, previous_status, status, reason, request_id, actor_identity, created_at
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            scope_type,
            channel_slug,
            previous_status,
            status,
            reason,
            request_id,
            actor_identity,
            _now_iso(),
        ),
    )


def create_publish_audit_status_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/publish/audit-status", tags=["publish-audit-status"])

    @router.get("/effective")
    def get_effective(channel_slug: str | None = None, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            if channel_slug:
                channel = dbm.get_channel_by_slug(conn, channel_slug)
                if not channel:
                    return JSONResponse(status_code=404, content={"error": {"code": "PAS_CHANNEL_NOT_FOUND", "message": "channel not found"}})
            resolved = resolve_effective_audit_status(conn, channel_slug=channel_slug)
            return resolved
        finally:
            conn.close()

    @router.put("/project-default")
    def put_project_default(payload: AuditStatusMutationPayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        validated = _validate_mutation_payload(payload)
        if isinstance(validated, JSONResponse):
            return validated
        normalized_status = validated
        request_id = str(payload.request_id).strip()
        reason = str(payload.reason).strip()
        actor = _actor_identity_from_request(request)

        conn = dbm.connect(env)
        try:
            current_row = conn.execute(
                "SELECT status FROM publish_audit_status_project_defaults WHERE singleton_key = 1"
            ).fetchone()
            previous_status = str(current_row["status"]) if current_row else None

            now_iso = _now_iso()
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO publish_audit_status_project_defaults(
                        singleton_key, status, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(1,?,?,?,?,?,?)
                    ON CONFLICT(singleton_key) DO UPDATE SET
                        status = excluded.status,
                        updated_at = excluded.updated_at,
                        updated_by = excluded.updated_by,
                        last_reason = excluded.last_reason,
                        last_request_id = excluded.last_request_id
                    """,
                    (normalized_status, now_iso, now_iso, actor, reason, request_id),
                )
                _append_history(
                    conn,
                    scope_type="project_default",
                    channel_slug=None,
                    previous_status=previous_status,
                    status=normalized_status,
                    reason=reason,
                    request_id=request_id,
                    actor_identity=actor,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            return {
                "ok": True,
                "request_id": request_id,
                "scope_type": "project_default",
                "status": normalized_status,
                "previous_status": previous_status,
                "actor_identity": actor,
            }
        finally:
            conn.close()

    @router.put("/channels/{channel_slug}")
    def put_channel_override(
        channel_slug: str,
        payload: AuditStatusMutationPayload,
        request: Request,
        _: bool = Depends(require_basic_auth(env)),
    ):
        validated = _validate_mutation_payload(payload)
        if isinstance(validated, JSONResponse):
            return validated
        normalized_status = validated
        request_id = str(payload.request_id).strip()
        reason = str(payload.reason).strip()
        actor = _actor_identity_from_request(request)

        conn = dbm.connect(env)
        try:
            channel = dbm.get_channel_by_slug(conn, channel_slug)
            if not channel:
                return JSONResponse(
                    status_code=404,
                    content={
                        "error": {
                            "code": "PAS_CHANNEL_NOT_FOUND",
                            "message": "channel not found",
                            "request_id": request_id,
                        }
                    },
                )

            current_row = conn.execute(
                "SELECT status FROM publish_audit_status_channel_overrides WHERE channel_slug = ?",
                (channel_slug,),
            ).fetchone()
            previous_status = str(current_row["status"]) if current_row else None

            now_iso = _now_iso()
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO publish_audit_status_channel_overrides(
                        channel_slug, status, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(channel_slug) DO UPDATE SET
                        status = excluded.status,
                        updated_at = excluded.updated_at,
                        updated_by = excluded.updated_by,
                        last_reason = excluded.last_reason,
                        last_request_id = excluded.last_request_id
                    """,
                    (channel_slug, normalized_status, now_iso, now_iso, actor, reason, request_id),
                )
                _append_history(
                    conn,
                    scope_type="channel_override",
                    channel_slug=channel_slug,
                    previous_status=previous_status,
                    status=normalized_status,
                    reason=reason,
                    request_id=request_id,
                    actor_identity=actor,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            return {
                "ok": True,
                "request_id": request_id,
                "scope_type": "channel_override",
                "channel_slug": channel_slug,
                "status": normalized_status,
                "previous_status": previous_status,
                "actor_identity": actor,
            }
        finally:
            conn.close()

    @router.get("/history")
    def get_history(
        channel_slug: str | None = None,
        limit: int = 50,
        _: bool = Depends(require_basic_auth(env)),
    ):
        safe_limit = min(max(limit, 1), 200)
        conn = dbm.connect(env)
        try:
            where = ""
            args: list[Any] = []
            if channel_slug:
                where = "WHERE channel_slug = ?"
                args.append(channel_slug)
            args.append(safe_limit)
            items = conn.execute(
                f"""
                SELECT id, scope_type, channel_slug, previous_status, status, reason, request_id, actor_identity, created_at
                FROM publish_audit_status_history
                {where}
                ORDER BY id DESC
                LIMIT ?
                """,
                tuple(args),
            ).fetchall()
            return {"items": items, "limit": safe_limit}
        finally:
            conn.close()

    return router
