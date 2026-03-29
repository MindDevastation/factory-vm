from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.publish_audit_status import resolve_effective_audit_status
from services.factory_api.security import require_basic_auth

ALLOWED_PUBLISH_MODES: tuple[str, ...] = ("auto", "manual_only", "hold")
ALLOWED_TARGET_VISIBILITY: tuple[str, ...] = ("public", "unlisted")
ALLOWED_PUBLISH_REASON_CODES: tuple[str, ...] = (
    "audit_not_approved",
    "manual_only_mode",
    "channel_policy_block",
    "item_override_block",
    "global_pause_active",
    "suspended_status",
    "transient_api_error",
    "rate_limited",
    "timeout",
    "unknown_external_error",
    "invalid_configuration",
    "terminal_publish_rejection",
    "operator_forced_manual",
    "retries_exhausted",
    "external_manual_publish_detected",
    "missed_schedule_operator_review",
    "policy_requires_manual",
)


class PublishPolicyMutationPayload(BaseModel):
    confirm: bool
    reason: str
    request_id: str
    publish_mode: str | None = None
    target_visibility: str | None = None
    reason_code: str | None = None


class PublishControlsMutationPayload(BaseModel):
    auto_publish_paused: bool
    reason: str | None = None


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


def validate_publish_mode(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value)
    if normalized not in ALLOWED_PUBLISH_MODES:
        raise ValueError("invalid publish_mode")
    return normalized


def validate_target_visibility(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value)
    if normalized not in ALLOWED_TARGET_VISIBILITY:
        raise ValueError("invalid target_visibility")
    return normalized


def validate_publish_reason_code(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value)
    if not normalized:
        raise ValueError("invalid reason_code")
    if normalized not in ALLOWED_PUBLISH_REASON_CODES:
        raise ValueError("invalid reason_code")
    return normalized


def _mutation_error(*, code: str, message: str, request_id: str, status_code: int = 422) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "request_id": request_id}},
    )


def _validate_mutation_payload(payload: PublishPolicyMutationPayload) -> tuple[str, str, str | None, str | None, str | None] | JSONResponse:
    request_id = str(payload.request_id or "").strip()
    if not request_id:
        return _mutation_error(code="PPP_REQUEST_ID_REQUIRED", message="request_id is required", request_id="", status_code=422)
    if payload.confirm is not True:
        return _mutation_error(
            code="PPP_CONFIRM_REQUIRED",
            message="confirm must be true",
            request_id=request_id,
            status_code=422,
        )
    reason = str(payload.reason or "").strip()
    if not reason:
        return _mutation_error(
            code="PPP_REASON_REQUIRED",
            message="reason is required",
            request_id=request_id,
            status_code=422,
        )
    try:
        return (
            reason,
            request_id,
            validate_publish_mode(payload.publish_mode),
            validate_target_visibility(payload.target_visibility),
            validate_publish_reason_code(payload.reason_code),
        )
    except ValueError as exc:
        code = "PPP_INVALID_POLICY_FIELD"
        if "publish_mode" in str(exc):
            code = "PPP_INVALID_PUBLISH_MODE"
        elif "target_visibility" in str(exc):
            code = "PPP_INVALID_TARGET_VISIBILITY"
        elif "reason_code" in str(exc):
            code = "PPP_INVALID_REASON_CODE"
        return _mutation_error(code=code, message=str(exc), request_id=request_id)


def _load_policy_rows(conn: Any, *, release_id: int, channel_slug: str) -> dict[str, Any]:
    project_row = conn.execute(
        "SELECT publish_mode, target_visibility, reason_code FROM publish_policy_project_defaults WHERE singleton_key = 1"
    ).fetchone()
    channel_row = conn.execute(
        "SELECT publish_mode, target_visibility, reason_code FROM publish_policy_channel_overrides WHERE channel_slug = ?",
        (channel_slug,),
    ).fetchone()
    item_row = conn.execute(
        "SELECT publish_mode, target_visibility, reason_code FROM publish_policy_item_overrides WHERE release_id = ?",
        (release_id,),
    ).fetchone()
    return {"project": project_row, "channel": channel_row, "item": item_row}


def _resolve_effective_policy(conn: Any, *, release_id: int, channel_slug: str) -> dict[str, Any]:
    rows = _load_policy_rows(conn, release_id=release_id, channel_slug=channel_slug)

    def _pick(field: str) -> tuple[Any, str | None]:
        item = rows["item"][field] if rows["item"] else None
        if item is not None:
            return item, "item"
        channel = rows["channel"][field] if rows["channel"] else None
        if channel is not None:
            return channel, "channel"
        project = rows["project"][field] if rows["project"] else None
        if project is not None:
            return project, "project"
        return None, None

    mode_value, mode_scope = _pick("publish_mode")
    visibility_value, _ = _pick("target_visibility")
    reason_code_value, _ = _pick("reason_code")

    effective_mode = str(mode_value) if mode_value is not None else "manual_only"
    resolved_scope = mode_scope or "project"

    return {
        "project_default": {
            "publish_mode": rows["project"]["publish_mode"] if rows["project"] else None,
            "target_visibility": rows["project"]["target_visibility"] if rows["project"] else None,
            "reason_code": rows["project"]["reason_code"] if rows["project"] else None,
        },
        "channel_override": {
            "publish_mode": rows["channel"]["publish_mode"] if rows["channel"] else None,
            "target_visibility": rows["channel"]["target_visibility"] if rows["channel"] else None,
            "reason_code": rows["channel"]["reason_code"] if rows["channel"] else None,
        },
        "item_override": {
            "publish_mode": rows["item"]["publish_mode"] if rows["item"] else None,
            "target_visibility": rows["item"]["target_visibility"] if rows["item"] else None,
            "reason_code": rows["item"]["reason_code"] if rows["item"] else None,
        },
        "effective_publish_mode": effective_mode,
        "effective_target_visibility": (str(visibility_value) if visibility_value is not None else None),
        "effective_reason_code": (str(reason_code_value) if reason_code_value is not None else None),
        "resolved_scope": resolved_scope,
    }


def _load_global_controls(conn: Any) -> dict[str, Any]:
    row = conn.execute(
        "SELECT auto_publish_paused, reason FROM publish_global_controls WHERE singleton_key = 1"
    ).fetchone()
    if not row:
        return {"auto_publish_paused": False, "reason": None}
    return {
        "auto_publish_paused": bool(row["auto_publish_paused"]),
        "reason": (str(row["reason"]) if row["reason"] is not None else None),
    }


def create_publish_policy_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/publish", tags=["publish-policy"])

    @router.put("/policy/project-default")
    def put_project_default(payload: PublishPolicyMutationPayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        validated = _validate_mutation_payload(payload)
        if isinstance(validated, JSONResponse):
            return validated
        reason, request_id, publish_mode, target_visibility, reason_code = validated
        actor = _actor_identity_from_request(request)
        now_iso = _now_iso()

        conn = dbm.connect(env)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO publish_policy_project_defaults(
                        singleton_key, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(1,?,?,?,?,?,?,?,?)
                    ON CONFLICT(singleton_key) DO UPDATE SET
                        publish_mode = excluded.publish_mode,
                        target_visibility = excluded.target_visibility,
                        reason_code = excluded.reason_code,
                        updated_at = excluded.updated_at,
                        updated_by = excluded.updated_by,
                        last_reason = excluded.last_reason,
                        last_request_id = excluded.last_request_id
                    """,
                    (publish_mode, target_visibility, reason_code, now_iso, now_iso, actor, reason, request_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            return {
                "ok": True,
                "scope_type": "project",
                "publish_mode": publish_mode,
                "target_visibility": target_visibility,
                "reason_code": reason_code,
                "request_id": request_id,
            }
        finally:
            conn.close()

    @router.put("/policy/channels/{channel_slug}")
    def put_channel_override(channel_slug: str, payload: PublishPolicyMutationPayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        validated = _validate_mutation_payload(payload)
        if isinstance(validated, JSONResponse):
            return validated
        reason, request_id, publish_mode, target_visibility, reason_code = validated
        actor = _actor_identity_from_request(request)
        now_iso = _now_iso()

        conn = dbm.connect(env)
        try:
            channel = dbm.get_channel_by_slug(conn, channel_slug)
            if not channel:
                return JSONResponse(status_code=404, content={"error": {"code": "PPP_CHANNEL_NOT_FOUND", "message": "channel not found", "request_id": request_id}})
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO publish_policy_channel_overrides(
                        channel_slug, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(channel_slug) DO UPDATE SET
                        publish_mode = excluded.publish_mode,
                        target_visibility = excluded.target_visibility,
                        reason_code = excluded.reason_code,
                        updated_at = excluded.updated_at,
                        updated_by = excluded.updated_by,
                        last_reason = excluded.last_reason,
                        last_request_id = excluded.last_request_id
                    """,
                    (channel_slug, publish_mode, target_visibility, reason_code, now_iso, now_iso, actor, reason, request_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            return {
                "ok": True,
                "scope_type": "channel",
                "channel_slug": channel_slug,
                "publish_mode": publish_mode,
                "target_visibility": target_visibility,
                "reason_code": reason_code,
                "request_id": request_id,
            }
        finally:
            conn.close()

    @router.put("/policy/items/{release_id}")
    def put_item_override(release_id: int, payload: PublishPolicyMutationPayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        validated = _validate_mutation_payload(payload)
        if isinstance(validated, JSONResponse):
            return validated
        reason, request_id, publish_mode, target_visibility, reason_code = validated
        actor = _actor_identity_from_request(request)
        now_iso = _now_iso()

        conn = dbm.connect(env)
        try:
            release = conn.execute("SELECT id FROM releases WHERE id = ?", (release_id,)).fetchone()
            if not release:
                return JSONResponse(status_code=404, content={"error": {"code": "PPP_RELEASE_NOT_FOUND", "message": "release not found", "request_id": request_id}})
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO publish_policy_item_overrides(
                        release_id, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(release_id) DO UPDATE SET
                        publish_mode = excluded.publish_mode,
                        target_visibility = excluded.target_visibility,
                        reason_code = excluded.reason_code,
                        updated_at = excluded.updated_at,
                        updated_by = excluded.updated_by,
                        last_reason = excluded.last_reason,
                        last_request_id = excluded.last_request_id
                    """,
                    (release_id, publish_mode, target_visibility, reason_code, now_iso, now_iso, actor, reason, request_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            return {
                "ok": True,
                "scope_type": "item",
                "release_id": release_id,
                "publish_mode": publish_mode,
                "target_visibility": target_visibility,
                "reason_code": reason_code,
                "request_id": request_id,
            }
        finally:
            conn.close()

    @router.get("/policy/resolve")
    def get_resolve(job_id: int, release_id: int | None = None, channel_slug: str | None = None, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            job = dbm.get_job(conn, job_id)
            if not job:
                return JSONResponse(status_code=404, content={"error": {"code": "PPP_JOB_NOT_FOUND", "message": "job not found"}})
            derived_release_id = int(job["release_id"])
            derived_channel_slug = str(job["channel_slug"])
            if release_id is not None and int(release_id) != derived_release_id:
                return JSONResponse(status_code=422, content={"error": {"code": "PPP_RELEASE_MISMATCH", "message": "release_id does not match job"}})
            if channel_slug is not None and str(channel_slug) != derived_channel_slug:
                return JSONResponse(status_code=422, content={"error": {"code": "PPP_CHANNEL_MISMATCH", "message": "channel_slug does not match job"}})

            policy = _resolve_effective_policy(conn, release_id=derived_release_id, channel_slug=derived_channel_slug)
            audit = resolve_effective_audit_status(conn, channel_slug=derived_channel_slug)
            controls = _load_global_controls(conn)

            job_hold_active = bool(job.get("publish_hold_active") or 0)
            job_hold_reason_code = validate_publish_reason_code(job.get("publish_hold_reason_code"))
            if job_hold_active and job_hold_reason_code is None:
                return JSONResponse(status_code=422, content={"error": {"code": "PPP_INVALID_JOB_HOLD", "message": "publish_hold_active requires publish_hold_reason_code"}})

            effective_reason_code = policy["effective_reason_code"]
            if job_hold_active:
                effective_reason_code = job_hold_reason_code
            elif controls["auto_publish_paused"]:
                effective_reason_code = "global_pause_active"

            decision_mode = str(policy["effective_publish_mode"])
            if job_hold_active or controls["auto_publish_paused"]:
                decision_mode = "hold"

            return {
                "job_id": int(job["id"]),
                "release_id": derived_release_id,
                "channel_slug": derived_channel_slug,
                "resolved_scope": policy["resolved_scope"],
                "effective_publish_mode": str(policy["effective_publish_mode"]),
                "effective_target_visibility": policy["effective_target_visibility"],
                "effective_reason_code": effective_reason_code,
                "effective_audit_status": audit["effective_status"],
                "global_auto_publish_paused": controls["auto_publish_paused"],
                "global_pause_reason": controls["reason"],
                "job_publish_hold_active": job_hold_active,
                "job_publish_hold_reason_code": job_hold_reason_code,
                "decision": decision_mode,
            }
        finally:
            conn.close()

    @router.get("/controls")
    def get_controls(_: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            controls = _load_global_controls(conn)
            return {
                "auto_publish_paused": controls["auto_publish_paused"],
                "reason": controls["reason"],
            }
        finally:
            conn.close()

    @router.put("/controls")
    def put_controls(payload: PublishControlsMutationPayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        actor = _actor_identity_from_request(request)
        reason = str(payload.reason).strip() if payload.reason is not None else None
        if reason == "":
            return JSONResponse(status_code=422, content={"error": {"code": "PPP_REASON_EMPTY", "message": "reason must be null or non-empty"}})

        now_iso = _now_iso()
        conn = dbm.connect(env)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO publish_global_controls(singleton_key, auto_publish_paused, reason, updated_at, updated_by)
                    VALUES(1,?,?,?,?)
                    ON CONFLICT(singleton_key) DO UPDATE SET
                        auto_publish_paused = excluded.auto_publish_paused,
                        reason = excluded.reason,
                        updated_at = excluded.updated_at,
                        updated_by = excluded.updated_by
                    """,
                    (1 if payload.auto_publish_paused else 0, reason, now_iso, actor),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            return {"ok": True, "auto_publish_paused": payload.auto_publish_paused, "reason": reason}
        finally:
            conn.close()

    return router
