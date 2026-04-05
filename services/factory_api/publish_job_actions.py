from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.security import require_basic_auth
from services.publish_runtime.orchestrator import is_publish_transition_allowed

_E3_ERROR_MAP: dict[str, str] = {
    "PJA_REQUEST_ID_REQUIRED": "E3_ACTION_CONFIRMATION_REQUIRED",
    "PJA_CONFIRM_REQUIRED": "E3_ACTION_CONFIRMATION_REQUIRED",
    "PJA_REASON_REQUIRED": "E3_ACTION_CONFIRMATION_REQUIRED",
    "PJA_JOB_NOT_FOUND": "E3_ACTION_NOT_ALLOWED",
    "PJA_JOB_CANCELLED": "E3_ACTION_NOT_ALLOWED",
    "PJA_ACTION_FORBIDDEN_STATE": "E3_ACTION_NOT_ALLOWED",
    "PJA_INVALID_DATETIME": "E3_ACTION_NOT_ALLOWED",
    "PJA_MARK_COMPLETED_MEDIA_REQUIRED": "E3_ACTION_NOT_ALLOWED",
    "PJA_RESCHEDULE_NOT_FUTURE": "E3_ACTION_NOT_ALLOWED",
    "PJA_ACTION_UNSUPPORTED": "E3_ACTION_NOT_ALLOWED",
}


class PublishActionEnvelope(BaseModel):
    confirm: bool
    reason: str
    request_id: str


class MarkCompletedPayload(PublishActionEnvelope):
    actual_published_at: str
    video_id: str | None = None
    url: str | None = None


class ReschedulePayload(PublishActionEnvelope):
    scheduled_at: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


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


def _mutation_error(*, code: str, message: str, request_id: str, status_code: int = 422) -> JSONResponse:
    e3_code = _E3_ERROR_MAP.get(code, "E3_ACTION_NOT_ALLOWED")
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": e3_code, "legacy_code": code, "message": message, "request_id": request_id}},
    )


def _validate_envelope(payload: PublishActionEnvelope) -> tuple[str, str] | JSONResponse:
    request_id = str(payload.request_id or "").strip()
    if not request_id:
        return _mutation_error(code="PJA_REQUEST_ID_REQUIRED", message="request_id is required", request_id="")
    if payload.confirm is not True:
        return _mutation_error(code="PJA_CONFIRM_REQUIRED", message="confirm must be true", request_id=request_id)
    reason = str(payload.reason or "").strip()
    if not reason:
        return _mutation_error(code="PJA_REASON_REQUIRED", message="reason is required", request_id=request_id)
    return reason, request_id


def replay_logged_mutation(row: Any) -> dict[str, Any]:
    payload = json.loads(str(row["response_json"]))
    return {
        "replayed": True,
        "action_type": str(row["action_type"]),
        "request_id": str(row["request_id"]),
        "job_id": int(row["job_id"]),
        "result": payload,
    }


def _guard_job_exists(conn: Any, job_id: int, *, request_id: str) -> Any | JSONResponse:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return _mutation_error(code="PJA_JOB_NOT_FOUND", message="job not found", request_id=request_id, status_code=404)
    return row


def _guard_not_cancelled(job_row: Any, *, request_id: str) -> JSONResponse | None:
    if str(job_row.get("state") or "").upper() == "CANCELLED":
        return _mutation_error(code="PJA_JOB_CANCELLED", message="job is cancelled", request_id=request_id, status_code=409)
    return None


def _forbidden_state(action_type: str, job_row: Any, request_id: str) -> JSONResponse:
    return _mutation_error(
        code="PJA_ACTION_FORBIDDEN_STATE",
        message=f"action {action_type} not allowed from publish_state={job_row.get('publish_state')}",
        request_id=request_id,
        status_code=409,
    )


def _parse_iso_datetime(raw: str, *, request_id: str, field_name: str) -> float | JSONResponse:
    text = str(raw or "").strip()
    if not text:
        return _mutation_error(code="PJA_INVALID_DATETIME", message=f"{field_name} is required", request_id=request_id)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return _mutation_error(code="PJA_INVALID_DATETIME", message=f"{field_name} must be ISO datetime", request_id=request_id)


def _apply_publish_transition(
    conn: Any,
    *,
    job_row: Any,
    action_type: str,
    request_id: str,
    to_state: str,
    updates: dict[str, Any],
) -> dict[str, Any] | JSONResponse:
    from_state = str(job_row.get("publish_state") or "")
    if not from_state:
        return _forbidden_state(action_type, job_row, request_id)
    if not is_publish_transition_allowed(
        from_publish_state=from_state,
        to_publish_state=to_state,
        transition_actor_class="operator_manual",
        job_state=str(job_row.get("state") or ""),
    ):
        return _forbidden_state(action_type, job_row, request_id)

    assignments = ["publish_state = ?", "publish_last_transition_at = ?"]
    params: list[Any] = [to_state, _now_ts()]
    for key, value in updates.items():
        assignments.append(f"{key} = ?")
        params.append(value)
    params.append(int(job_row["id"]))

    conn.execute(f"UPDATE jobs SET {', '.join(assignments)} WHERE id = ?", tuple(params))
    return {"ok": True, "publish_state_before": from_state, "publish_state_after": to_state}


def _execute_operator_action(
    conn: Any,
    *,
    job_id: int,
    action_type: str,
    request_id: str,
    actor: str,
    reason: str,
    mutate_fn: Callable[[Any], dict[str, Any] | JSONResponse],
) -> dict[str, Any] | JSONResponse:
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT action_type, request_id, job_id, response_json FROM publish_action_log WHERE action_type = ? AND request_id = ?",
            (action_type, request_id),
        ).fetchone()
        if existing:
            conn.commit()
            return replay_logged_mutation(existing)

        row = _guard_job_exists(conn, job_id, request_id=request_id)
        if isinstance(row, JSONResponse):
            conn.rollback()
            return row
        blocked = _guard_not_cancelled(row, request_id=request_id)
        if blocked is not None and action_type != "cancel":
            conn.rollback()
            return blocked

        result = mutate_fn(row)
        if isinstance(result, JSONResponse):
            conn.rollback()
            return result

        conn.execute(
            """
            INSERT INTO publish_action_log(action_type, request_id, job_id, actor_identity, reason, response_json, created_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (action_type, request_id, job_id, actor, reason, json.dumps(result, sort_keys=True), _now_iso()),
        )
        conn.commit()
        return {"replayed": False, "action_type": action_type, "request_id": request_id, "job_id": job_id, "result": result}
    except Exception:
        conn.rollback()
        raise




def execute_publish_job_action(
    conn: Any,
    *,
    job_id: int,
    action_type: str,
    actor: str,
    request_id: str,
    reason: str,
    extra_payload: dict[str, Any] | None = None,
) -> dict[str, Any] | JSONResponse:
    payload = dict(extra_payload or {})

    def _mutate(row: Any) -> dict[str, Any] | JSONResponse:
        if action_type == "retry":
            return _apply_publish_transition(
                conn,
                job_row=row,
                action_type="retry",
                request_id=request_id,
                to_state="ready_to_publish",
                updates={"publish_retry_at": None, "publish_last_error_code": None, "publish_last_error_message": None},
            )
        if action_type == "reset_failure":
            return _reset_failure_mutation(conn, row, request_id)
        if action_type == "move_to_manual":
            return _apply_publish_transition(
                conn,
                job_row=row,
                action_type="move_to_manual",
                request_id=request_id,
                to_state="manual_handoff_pending",
                updates={"publish_reason_code": "operator_forced_manual", "publish_reason_detail": reason.strip()},
            )
        if action_type == "acknowledge":
            return _apply_publish_transition(
                conn,
                job_row=row,
                action_type="acknowledge",
                request_id=request_id,
                to_state="manual_handoff_acknowledged",
                updates={"publish_manual_ack_at": _now_ts()},
            )
        if action_type == "mark_completed":
            actual_published_at = payload.get("actual_published_at")
            video_id = payload.get("video_id")
            url = payload.get("url")
            if actual_published_at is None:
                return _mutation_error(code="PJA_INVALID_DATETIME", message="actual_published_at is required", request_id=request_id)
            parsed = _parse_iso_datetime(str(actual_published_at), request_id=request_id, field_name="actual_published_at")
            if isinstance(parsed, JSONResponse):
                return parsed
            video_id_s = str(video_id or "").strip() or None
            url_s = str(url or "").strip() or None
            if not video_id_s and not url_s:
                return _mutation_error(
                    code="PJA_MARK_COMPLETED_MEDIA_REQUIRED",
                    message="at least one of video_id or url is required",
                    request_id=request_id,
                    status_code=422,
                )
            return _apply_publish_transition(
                conn,
                job_row=row,
                action_type="mark_completed",
                request_id=request_id,
                to_state="manual_publish_completed",
                updates={
                    "publish_manual_completed_at": _now_ts(),
                    "publish_manual_published_at": parsed,
                    "publish_manual_video_id": video_id_s,
                    "publish_manual_url": url_s,
                },
            )
        if action_type == "cancel":
            return _cancel_mutation(conn, row, request_id)
        if action_type == "unblock":
            return _apply_publish_transition(
                conn,
                job_row=row,
                action_type="unblock",
                request_id=request_id,
                to_state="ready_to_publish",
                updates={"publish_hold_active": 0, "publish_hold_reason_code": None},
            )
        if action_type == "reschedule":
            scheduled_at = payload.get("scheduled_at")
            parsed = _parse_iso_datetime(str(scheduled_at or ""), request_id=request_id, field_name="scheduled_at")
            if isinstance(parsed, JSONResponse):
                return parsed
            if parsed <= _now_ts():
                return _mutation_error(code="PJA_RESCHEDULE_NOT_FUTURE", message="scheduled_at must be in the future", request_id=request_id)
            return _apply_publish_transition(
                conn,
                job_row=row,
                action_type="reschedule",
                request_id=request_id,
                to_state="waiting_for_schedule",
                updates={"publish_scheduled_at": parsed, "publish_retry_at": None},
            )
        return _mutation_error(code="PJA_ACTION_UNSUPPORTED", message="unsupported action", request_id=request_id, status_code=422)

    return _execute_operator_action(
        conn,
        job_id=job_id,
        action_type=action_type,
        request_id=request_id,
        actor=actor,
        reason=reason,
        mutate_fn=_mutate,
    )

def create_publish_job_actions_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/publish/jobs", tags=["publish-job-actions"])

    def _run_action(
        job_id: int,
        payload: PublishActionEnvelope,
        request: Request,
        *,
        action_type: str,
        extra_payload: dict[str, Any] | None = None,
    ):
        validated = _validate_envelope(payload)
        if isinstance(validated, JSONResponse):
            return validated
        reason, request_id = validated
        actor = _actor_identity_from_request(request)
        conn = dbm.connect(env)
        try:
            return execute_publish_job_action(
                conn,
                job_id=job_id,
                action_type=action_type,
                actor=actor,
                request_id=request_id,
                reason=reason,
                extra_payload=extra_payload,
            )
        finally:
            conn.close()

    @router.post("/{job_id}/retry")
    def retry(job_id: int, payload: PublishActionEnvelope, request: Request, _: bool = Depends(require_basic_auth(env))):
        return _run_action(job_id, payload, request, action_type="retry")

    @router.post("/{job_id}/reset-failure")
    def reset_failure(job_id: int, payload: PublishActionEnvelope, request: Request, _: bool = Depends(require_basic_auth(env))):
        return _run_action(job_id, payload, request, action_type="reset_failure")

    @router.post("/{job_id}/move-to-manual")
    def move_to_manual(job_id: int, payload: PublishActionEnvelope, request: Request, _: bool = Depends(require_basic_auth(env))):
        return _run_action(job_id, payload, request, action_type="move_to_manual")

    @router.post("/{job_id}/acknowledge")
    def acknowledge(job_id: int, payload: PublishActionEnvelope, request: Request, _: bool = Depends(require_basic_auth(env))):
        return _run_action(job_id, payload, request, action_type="acknowledge")

    @router.post("/{job_id}/mark-completed")
    def mark_completed(job_id: int, payload: MarkCompletedPayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        validated = _validate_envelope(payload)
        if isinstance(validated, JSONResponse):
            return validated
        reason, request_id = validated
        actual_published_at = _parse_iso_datetime(payload.actual_published_at, request_id=request_id, field_name="actual_published_at")
        if isinstance(actual_published_at, JSONResponse):
            return actual_published_at
        video_id = str(payload.video_id or "").strip() or None
        url = str(payload.url or "").strip() or None
        if not video_id and not url:
            return _mutation_error(
                code="PJA_MARK_COMPLETED_MEDIA_REQUIRED",
                message="at least one of video_id or url is required",
                request_id=request_id,
                status_code=422,
            )
        actor = _actor_identity_from_request(request)

        conn = dbm.connect(env)
        try:
            return execute_publish_job_action(conn, job_id=job_id, action_type="mark_completed", actor=actor, request_id=request_id, reason=reason, extra_payload={"actual_published_at": payload.actual_published_at, "video_id": video_id, "url": url})
        finally:
            conn.close()

    @router.post("/{job_id}/cancel")
    def cancel(job_id: int, payload: PublishActionEnvelope, request: Request, _: bool = Depends(require_basic_auth(env))):
        return _run_action(job_id, payload, request, action_type="cancel")

    @router.post("/{job_id}/unblock")
    def unblock(job_id: int, payload: PublishActionEnvelope, request: Request, _: bool = Depends(require_basic_auth(env))):
        return _run_action(job_id, payload, request, action_type="unblock")

    @router.post("/{job_id}/reschedule")
    def reschedule(job_id: int, payload: ReschedulePayload, request: Request, _: bool = Depends(require_basic_auth(env))):
        validated = _validate_envelope(payload)
        if isinstance(validated, JSONResponse):
            return validated
        reason, request_id = validated
        scheduled_at = _parse_iso_datetime(payload.scheduled_at, request_id=request_id, field_name="scheduled_at")
        if isinstance(scheduled_at, JSONResponse):
            return scheduled_at
        if scheduled_at <= _now_ts():
            return _mutation_error(code="PJA_RESCHEDULE_NOT_FUTURE", message="scheduled_at must be in the future", request_id=request_id)
        actor = _actor_identity_from_request(request)
        conn = dbm.connect(env)
        try:
            return execute_publish_job_action(conn, job_id=job_id, action_type="reschedule", actor=actor, request_id=request_id, reason=reason, extra_payload={"scheduled_at": payload.scheduled_at})
        finally:
            conn.close()

    return router


def _cancel_mutation(conn: Any, row: Any, request_id: str) -> dict[str, Any] | JSONResponse:
    current_job_state = str(row.get("state") or "").upper()
    if current_job_state == "CANCELLED":
        return _mutation_error(code="PJA_JOB_CANCELLED", message="job is cancelled", request_id=request_id, status_code=409)
    from_state = str(row.get("publish_state") or "")
    updates = {
        "state": "CANCELLED",
        "stage": "CANCELLED",
        "publish_reason_code": "operator_cancelled",
        "publish_reason_detail": "cancelled by operator",
        "locked_by": None,
        "locked_at": None,
        "retry_at": None,
        "publish_retry_at": None,
    }
    if from_state and is_publish_transition_allowed(
        from_publish_state=from_state,
        to_publish_state="publish_failed_terminal",
        transition_actor_class="operator_manual",
        job_state=current_job_state,
    ):
        conn.execute(
            """
            UPDATE jobs
            SET state = ?, stage = ?, publish_state = ?, publish_reason_code = ?, publish_reason_detail = ?, retry_at = NULL, publish_retry_at = NULL, locked_by = NULL, locked_at = NULL, publish_last_transition_at = ?, updated_at = ?
            WHERE id = ?
            """,
            ("CANCELLED", "CANCELLED", "publish_failed_terminal", "operator_cancelled", "cancelled by operator", _now_ts(), _now_ts(), int(row["id"])),
        )
        return {
            "ok": True,
            "publish_state_before": from_state,
            "publish_state_after": "publish_failed_terminal",
            "state_after": "CANCELLED",
            "stage_after": "CANCELLED",
        }

    publish_state_after = None if from_state == "retry_pending" else (from_state or None)
    conn.execute(
        "UPDATE jobs SET state = ?, stage = ?, publish_state = ?, publish_reason_code = ?, publish_reason_detail = ?, retry_at = NULL, publish_retry_at = NULL, locked_by = NULL, locked_at = NULL, updated_at = ? WHERE id = ?",
        (
            updates["state"],
            updates["stage"],
            publish_state_after,
            updates["publish_reason_code"],
            updates["publish_reason_detail"],
            _now_ts(),
            int(row["id"]),
        ),
    )
    return {
        "ok": True,
        "publish_state_before": from_state or None,
        "publish_state_after": publish_state_after,
        "state_after": "CANCELLED",
        "stage_after": "CANCELLED",
    }


def _reset_failure_mutation(conn: Any, row: Any, request_id: str) -> dict[str, Any] | JSONResponse:
    from_state = str(row.get("publish_state") or "")
    if from_state != "publish_failed_terminal":
        return _forbidden_state("reset_failure", row, request_id)
    conn.execute(
        """
        UPDATE jobs
        SET publish_state = ?, publish_retry_at = ?, publish_last_error_code = NULL, publish_last_error_message = NULL, publish_last_transition_at = ?
        WHERE id = ?
        """,
        ("retry_pending", _now_ts(), _now_ts(), int(row["id"])),
    )
    return {"ok": True, "publish_state_before": from_state, "publish_state_after": "retry_pending"}


__all__ = ["create_publish_job_actions_router", "replay_logged_mutation", "execute_publish_job_action"]
