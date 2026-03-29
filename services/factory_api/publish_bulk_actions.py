from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import sqlite3
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.publish_job_actions import (
    _actor_identity_from_request,
    _apply_publish_transition,
    _guard_job_exists,
    _guard_not_cancelled,
    _mutation_error,
    _parse_iso_datetime,
)
from services.factory_api.security import require_basic_auth
from services.publish_runtime.orchestrator import is_publish_transition_allowed

ALLOWED_BULK_ACTIONS = {"retry", "move_to_manual", "acknowledge", "reschedule", "hold", "unblock"}
MAX_BULK_SELECTION = 200
PREVIEW_TTL_SECONDS = 1800


class PublishBulkPreviewPayload(BaseModel):
    action: str
    selected_job_ids: list[int]


class PublishBulkExecutePayload(BaseModel):
    preview_session_id: str
    selected_job_ids: list[int] | None = None
    selection_fingerprint: str | None = None


class PublishBulkActionError(Exception):
    def __init__(self, *, code: str, message: str, status_code: int = 409, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}


def create_publish_bulk_actions_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/publish/bulk", tags=["publish-bulk-actions"])

    @router.post("/preview")
    def preview(payload: PublishBulkPreviewPayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            out = create_bulk_preview_session(
                conn,
                action=str(payload.action or ""),
                selected_job_ids=list(payload.selected_job_ids or []),
                created_by=_actor_identity_from_request(request),
                ttl_seconds=PREVIEW_TTL_SECONDS,
            )
            return out
        except PublishBulkActionError as exc:
            return _bulk_error(exc)
        finally:
            conn.close()

    @router.post("/execute")
    def execute(payload: PublishBulkExecutePayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            out = execute_bulk_preview_session(
                conn,
                preview_session_id=str(payload.preview_session_id or "").strip(),
                selected_job_ids=payload.selected_job_ids,
                selection_fingerprint=(str(payload.selection_fingerprint or "").strip() or None),
                executed_by=_actor_identity_from_request(request),
            )
            return out
        except PublishBulkActionError as exc:
            return _bulk_error(exc)
        finally:
            conn.close()

    return router


def create_bulk_preview_session(
    conn: sqlite3.Connection,
    *,
    action: str,
    selected_job_ids: list[int],
    created_by: str | None,
    ttl_seconds: int,
) -> dict[str, Any]:
    normalized_action = _normalize_action(action)
    normalized_ids = _normalize_selected_ids(selected_job_ids)
    jobs_by_id = _load_jobs(conn, normalized_ids)

    items: list[dict[str, Any]] = []
    selected_count = 0
    rejected_count = 0
    snapshot_ids: list[int] = []
    for job_id in normalized_ids:
        job = jobs_by_id.get(job_id)
        if job is None:
            rejected_count += 1
            items.append({
                "job_id": job_id,
                "action": normalized_action,
                "preview_result": "REJECTED",
                "reason": {"code": "PBA_JOB_NOT_FOUND", "message": "job not found"},
            })
            continue
        preview_item = _preview_item(action=normalized_action, job=dict(job))
        if preview_item["preview_result"] == "ALLOWED":
            selected_count += 1
            snapshot_ids.append(job_id)
        else:
            rejected_count += 1
        items.append(preview_item)

    fingerprint = _build_selection_fingerprint(action=normalized_action, selected_job_ids=snapshot_ids)
    aggregate = {
        "total_requested": len(normalized_ids),
        "selected_count": selected_count,
        "rejected_count": rejected_count,
    }

    now = datetime.now(timezone.utc)
    created_at = now.isoformat()
    expires_at = (now + timedelta(seconds=max(1, int(ttl_seconds)))).isoformat()
    session_id = uuid.uuid4().hex

    dbm.insert_publish_bulk_action_session(
        conn,
        session_id=session_id,
        action_type=normalized_action,
        selection_fingerprint=fingerprint,
        selected_job_ids_json=dbm.json_dumps(snapshot_ids),
        preview_status="OPEN",
        aggregate_preview_json=dbm.json_dumps(aggregate),
        item_preview_json=dbm.json_dumps(items),
        invalidation_reason_code=None,
        created_by=created_by,
        created_at=created_at,
        expires_at=expires_at,
        executed_at=None,
    )
    conn.commit()

    return {
        "preview_session_id": session_id,
        "action": normalized_action,
        "selection_fingerprint": fingerprint,
        "selected_count": selected_count,
        "rejected_count": rejected_count,
        "aggregate": aggregate,
        "items": items,
        "created_at": created_at,
        "expires_at": expires_at,
    }


def execute_bulk_preview_session(
    conn: sqlite3.Connection,
    *,
    preview_session_id: str,
    selected_job_ids: list[int] | None,
    selection_fingerprint: str | None,
    executed_by: str | None,
) -> dict[str, Any]:
    del executed_by
    session = _load_session(conn, preview_session_id=preview_session_id)
    _validate_session_freshness(conn, session=session)

    action = str(session["action_type"])
    snapshot_ids = [int(item) for item in dbm.json_loads(str(session["selected_job_ids_json"]))]
    stored_fingerprint = str(session["selection_fingerprint"])

    execute_ids = snapshot_ids if selected_job_ids is None else [int(item) for item in selected_job_ids]
    if len(set(execute_ids)) != len(execute_ids):
        raise PublishBulkActionError(
            code="PBA_EXECUTE_OUTSIDE_SNAPSHOT",
            message="execute selected_job_ids must not contain duplicates",
            status_code=409,
            details={
                "invalidation": {
                    "kind": "scope_mismatch",
                    "preview_session_id": preview_session_id,
                    "stored_fingerprint": stored_fingerprint,
                    "request_fingerprint": _build_selection_fingerprint(action=action, selected_job_ids=execute_ids),
                }
            },
        )

    snapshot_set = set(snapshot_ids)
    if any(job_id not in snapshot_set for job_id in execute_ids):
        raise PublishBulkActionError(
            code="PBA_EXECUTE_OUTSIDE_SNAPSHOT",
            message="execute selected_job_ids must be contained in preview snapshot",
            status_code=409,
            details={
                "invalidation": {
                    "kind": "scope_mismatch",
                    "preview_session_id": preview_session_id,
                    "stored_fingerprint": stored_fingerprint,
                    "request_fingerprint": _build_selection_fingerprint(action=action, selected_job_ids=execute_ids),
                }
            },
        )

    request_fingerprint = selection_fingerprint or _build_selection_fingerprint(action=action, selected_job_ids=execute_ids)
    if request_fingerprint != stored_fingerprint:
        conn.execute(
            "UPDATE publish_bulk_action_sessions SET preview_status = 'INVALIDATED', invalidation_reason_code = ? WHERE id = ? AND preview_status = 'OPEN'",
            ("FINGERPRINT_MISMATCH", preview_session_id),
        )
        conn.commit()
        raise PublishBulkActionError(
            code="PBA_SCOPE_MISMATCH",
            message="bulk preview scope no longer matches execute request",
            status_code=409,
            details={
                "invalidation": {
                    "kind": "scope_mismatch",
                    "preview_session_id": preview_session_id,
                    "stored_fingerprint": stored_fingerprint,
                    "request_fingerprint": request_fingerprint,
                }
            },
        )

    conn.execute("BEGIN IMMEDIATE")
    try:
        items: list[dict[str, Any]] = []
        for job_id in execute_ids:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            items.append(_execute_item(conn, action=action, job_id=job_id, job_row=(dict(row) if row else None)))

        summary = {
            "executed_count": len(execute_ids),
            "succeeded_count": sum(1 for item in items if item["result_kind"] == "SUCCESS_UPDATED"),
            "failed_count": sum(1 for item in items if item["result_kind"] == "FAILED"),
            "skipped_count": sum(1 for item in items if item["result_kind"] == "SKIPPED_FORBIDDEN"),
        }
        executed_at = datetime.now(timezone.utc).isoformat()
        updated = conn.execute(
            "UPDATE publish_bulk_action_sessions SET preview_status = 'EXECUTED', executed_at = ? WHERE id = ? AND preview_status = 'OPEN'",
            (executed_at, preview_session_id),
        )
        if int(updated.rowcount or 0) != 1:
            raise PublishBulkActionError(
                code="PBA_SCOPE_MISMATCH",
                message="bulk preview session is stale and cannot be executed",
                status_code=409,
                details={
                    "invalidation": {
                        "kind": "scope_mismatch",
                        "preview_session_id": preview_session_id,
                        "stored_fingerprint": stored_fingerprint,
                        "request_fingerprint": request_fingerprint,
                    }
                },
            )

        conn.commit()
        return {
            "preview_session_id": preview_session_id,
            "action": action,
            "selection_fingerprint": stored_fingerprint,
            "summary": summary,
            "items": items,
            "executed_at": executed_at,
        }
    except PublishBulkActionError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise


def _bulk_error(exc: PublishBulkActionError) -> JSONResponse:
    payload: dict[str, Any] = {"error": {"code": exc.code, "message": exc.message}}
    if exc.details:
        payload["error"]["details"] = exc.details
    return JSONResponse(status_code=exc.status_code, content=payload)


def _normalize_action(action: str) -> str:
    value = str(action or "").strip()
    if value not in ALLOWED_BULK_ACTIONS:
        raise PublishBulkActionError(code="PBA_INVALID_ACTION", message="unsupported bulk action", status_code=422)
    return value


def _normalize_selected_ids(selected_job_ids: list[int]) -> list[int]:
    if not isinstance(selected_job_ids, list) or not selected_job_ids:
        raise PublishBulkActionError(code="PBA_SELECTION_EMPTY", message="selected_job_ids must not be empty", status_code=422)
    if len(selected_job_ids) > MAX_BULK_SELECTION:
        raise PublishBulkActionError(code="PBA_SELECTION_TOO_LARGE", message="selected_job_ids exceeds limit", status_code=422)
    normalized = [int(item) for item in selected_job_ids]
    if len(set(normalized)) != len(normalized):
        raise PublishBulkActionError(code="PBA_SELECTION_DUPLICATE", message="selected_job_ids must not contain duplicates", status_code=422)
    return normalized


def _load_jobs(conn: sqlite3.Connection, selected_job_ids: list[int]) -> dict[int, Any]:
    placeholders = ",".join("?" for _ in selected_job_ids)
    rows = conn.execute(f"SELECT * FROM jobs WHERE id IN ({placeholders})", tuple(selected_job_ids)).fetchall()
    return {int(row["id"]): row for row in rows}


def _preview_item(*, action: str, job: dict[str, Any]) -> dict[str, Any]:
    blocked = _guard_not_cancelled(job, request_id="bulk-preview")
    if blocked is not None:
        return {
            "job_id": int(job["id"]),
            "action": action,
            "preview_result": "REJECTED",
            "reason": {"code": "PJA_JOB_CANCELLED", "message": "job is cancelled"},
        }

    from_state = str(job.get("publish_state") or "")
    if action == "hold":
        if bool(job.get("publish_hold_active") or 0):
            return {
                "job_id": int(job["id"]),
                "action": action,
                "preview_result": "REJECTED",
                "reason": {"code": "PBA_HOLD_ALREADY_ACTIVE", "message": "publish hold already active"},
            }
        return {
            "job_id": int(job["id"]),
            "action": action,
            "preview_result": "ALLOWED",
            "publish_state_before": from_state or None,
            "publish_state_after": from_state or None,
            "hold_after": True,
        }

    target_state = _target_state_for_action(action)
    if not from_state or not target_state:
        return {
            "job_id": int(job["id"]),
            "action": action,
            "preview_result": "REJECTED",
            "reason": {"code": "PJA_ACTION_FORBIDDEN_STATE", "message": f"action {action} not allowed from publish_state={from_state}"},
        }
    if not is_publish_transition_allowed(
        from_publish_state=from_state,
        to_publish_state=target_state,
        transition_actor_class="operator_manual",
        job_state=str(job.get("state") or ""),
    ):
        return {
            "job_id": int(job["id"]),
            "action": action,
            "preview_result": "REJECTED",
            "reason": {"code": "PJA_ACTION_FORBIDDEN_STATE", "message": f"action {action} not allowed from publish_state={from_state}"},
        }

    return {
        "job_id": int(job["id"]),
        "action": action,
        "preview_result": "ALLOWED",
        "publish_state_before": from_state,
        "publish_state_after": target_state,
    }


def _target_state_for_action(action: str) -> str | None:
    mapping = {
        "retry": "ready_to_publish",
        "move_to_manual": "manual_handoff_pending",
        "acknowledge": "manual_handoff_acknowledged",
        "reschedule": "waiting_for_schedule",
        "unblock": "ready_to_publish",
    }
    return mapping.get(action)


def _execute_item(conn: sqlite3.Connection, *, action: str, job_id: int, job_row: dict[str, Any] | None) -> dict[str, Any]:
    if job_row is None:
        return {
            "job_id": job_id,
            "action": action,
            "result_kind": "FAILED",
            "reason": {"code": "PBA_JOB_NOT_FOUND", "message": "job not found"},
        }

    found = _guard_job_exists(conn, job_id, request_id="bulk-execute")
    if isinstance(found, JSONResponse):
        return {
            "job_id": job_id,
            "action": action,
            "result_kind": "FAILED",
            "reason": {"code": "PBA_JOB_NOT_FOUND", "message": "job not found"},
        }

    blocked = _guard_not_cancelled(job_row, request_id="bulk-execute")
    if blocked is not None:
        return {
            "job_id": job_id,
            "action": action,
            "result_kind": "SKIPPED_FORBIDDEN",
            "reason": {"code": "PJA_JOB_CANCELLED", "message": "job is cancelled"},
        }

    if action == "hold":
        hold_before = bool(job_row.get("publish_hold_active") or 0)
        if hold_before:
            return {
                "job_id": job_id,
                "action": action,
                "result_kind": "SKIPPED_FORBIDDEN",
                "hold_before": True,
                "hold_after": True,
                "reason": {"code": "PBA_HOLD_ALREADY_ACTIVE", "message": "publish hold already active"},
            }
        conn.execute(
            "UPDATE jobs SET publish_hold_active = 1, publish_hold_reason_code = ?, publish_last_transition_at = ?, updated_at = ? WHERE id = ?",
            ("operator_forced_manual", dbm.now_ts(), dbm.now_ts(), job_id),
        )
        return {
            "job_id": job_id,
            "action": action,
            "result_kind": "SUCCESS_UPDATED",
            "publish_state_before": str(job_row.get("publish_state") or "") or None,
            "publish_state_after": str(job_row.get("publish_state") or "") or None,
            "hold_before": False,
            "hold_after": True,
        }

    mutation = _run_transition_action(conn, action=action, row=job_row)
    if isinstance(mutation, JSONResponse):
        body = mutation.body.decode("utf-8") if hasattr(mutation, "body") else ""
        message = "forbidden"
        if body:
            try:
                payload = dbm.json_loads(body)
                message = str((payload.get("error") or {}).get("message") or message)
            except Exception:
                pass
        return {
            "job_id": job_id,
            "action": action,
            "result_kind": "SKIPPED_FORBIDDEN",
            "reason": {"code": "PJA_ACTION_FORBIDDEN_STATE", "message": message},
        }

    return {
        "job_id": job_id,
        "action": action,
        "result_kind": "SUCCESS_UPDATED",
        "publish_state_before": mutation.get("publish_state_before"),
        "publish_state_after": mutation.get("publish_state_after"),
    }


def _run_transition_action(conn: sqlite3.Connection, *, action: str, row: dict[str, Any]) -> dict[str, Any] | JSONResponse:
    request_id = f"bulk-{action}"
    if action == "retry":
        return _apply_publish_transition(
            conn,
            job_row=row,
            action_type="retry",
            request_id=request_id,
            to_state="ready_to_publish",
            updates={"publish_retry_at": None, "publish_last_error_code": None, "publish_last_error_message": None},
        )
    if action == "move_to_manual":
        return _apply_publish_transition(
            conn,
            job_row=row,
            action_type="move_to_manual",
            request_id=request_id,
            to_state="manual_handoff_pending",
            updates={"publish_reason_code": "operator_forced_manual", "publish_reason_detail": "bulk operator action"},
        )
    if action == "acknowledge":
        return _apply_publish_transition(
            conn,
            job_row=row,
            action_type="acknowledge",
            request_id=request_id,
            to_state="manual_handoff_acknowledged",
            updates={"publish_manual_ack_at": dbm.now_ts()},
        )
    if action == "reschedule":
        return _apply_publish_transition(
            conn,
            job_row=row,
            action_type="reschedule",
            request_id=request_id,
            to_state="waiting_for_schedule",
            updates={"publish_scheduled_at": dbm.now_ts() + 86400.0, "publish_retry_at": None},
        )
    if action == "unblock":
        return _apply_publish_transition(
            conn,
            job_row=row,
            action_type="unblock",
            request_id=request_id,
            to_state="ready_to_publish",
            updates={"publish_hold_active": 0, "publish_hold_reason_code": None},
        )
    return _mutation_error(code="PBA_INVALID_ACTION", message="unsupported bulk action", request_id=request_id)


def _build_selection_fingerprint(*, action: str, selected_job_ids: list[int]) -> str:
    payload = {"action": action, "selected_job_ids": [int(item) for item in selected_job_ids]}
    raw = dbm.json_dumps(payload)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _load_session(conn: sqlite3.Connection, *, preview_session_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM publish_bulk_action_sessions WHERE id = ?", (preview_session_id,)).fetchone()
    if row is None:
        raise PublishBulkActionError(code="PBA_SESSION_NOT_FOUND", message="bulk preview session not found", status_code=404)
    return dict(row)


def _validate_session_freshness(conn: sqlite3.Connection, *, session: dict[str, Any]) -> None:
    session_id = str(session["id"])
    status = str(session["preview_status"])
    expires_at = str(session["expires_at"])
    now = datetime.now(timezone.utc)
    expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))

    if now > expires_dt:
        conn.execute(
            "UPDATE publish_bulk_action_sessions SET preview_status = 'EXPIRED' WHERE id = ? AND preview_status = 'OPEN'",
            (session_id,),
        )
        conn.commit()
        raise PublishBulkActionError(
            code="PBA_SESSION_EXPIRED",
            message="bulk preview session has expired",
            status_code=409,
            details={
                "invalidation": {
                    "kind": "expired",
                    "preview_session_id": session_id,
                    "preview_status": "EXPIRED",
                    "expires_at": expires_at,
                }
            },
        )

    if status == "INVALIDATED":
        raise PublishBulkActionError(
            code="PBA_SESSION_INVALIDATED",
            message="bulk preview session was invalidated",
            status_code=409,
            details={
                "invalidation": {
                    "kind": "invalidated",
                    "preview_session_id": session_id,
                    "preview_status": "INVALIDATED",
                    "reason_code": str(session.get("invalidation_reason_code") or "UNKNOWN"),
                }
            },
        )

    if status != "OPEN":
        raise PublishBulkActionError(
            code="PBA_SCOPE_MISMATCH",
            message="bulk preview session is stale and cannot be executed",
            status_code=409,
            details={
                "invalidation": {
                    "kind": "scope_mismatch",
                    "preview_session_id": session_id,
                    "stored_fingerprint": str(session["selection_fingerprint"]),
                    "request_fingerprint": str(session["selection_fingerprint"]),
                }
            },
        )


__all__ = [
    "create_publish_bulk_actions_router",
    "create_bulk_preview_session",
    "execute_bulk_preview_session",
    "_build_selection_fingerprint",
]
