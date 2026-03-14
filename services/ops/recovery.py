from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from services.common import db as dbm
from services.ui_jobs import UiJobRetryNotFoundError, UiJobRetryStatusError, retry_failed_ui_job

SAFE_ACTIONS = {"retry_failed", "reclaim_stale"}
RISKY_ACTIONS = {"cancel_job", "force_cleanup_artifacts", "reenqueue_allowed_stage"}
CANONICAL_ACTIONS = SAFE_ACTIONS | RISKY_ACTIONS


class RecoveryActionError(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: int = 409):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True)
class RecoveryRuntime:
    retry_backoff_sec: int
    max_render_attempts: int
    job_lock_ttl_sec: int


def _is_stale_locked_job(job: dict[str, Any], now_ts: float, lock_ttl_sec: int) -> bool:
    if str(job.get("state") or "") not in {"FETCHING_INPUTS", "RENDERING"}:
        return False
    locked_at = job.get("locked_at")
    locked_by = job.get("locked_by")
    if locked_at is None or not locked_by:
        return False
    return float(locked_at) < (now_ts - float(lock_ttl_sec))


def preview_action(conn: sqlite3.Connection, *, job: dict[str, Any], action: str, runtime: RecoveryRuntime) -> dict[str, Any]:
    now_ts = dbm.now_ts()
    state = str(job.get("state") or "")

    if action == "retry_failed":
        retry_child = conn.execute("SELECT id FROM jobs WHERE retry_of_job_id = ? LIMIT 1", (int(job["id"]),)).fetchone()
        return {
            "allowed": bool(state == "FAILED" and retry_child is None),
            "safe": True,
            "reason": "ok" if state == "FAILED" and retry_child is None else "retry_not_allowed",
        }

    if action == "reclaim_stale":
        stale = _is_stale_locked_job(job, now_ts, runtime.job_lock_ttl_sec)
        return {
            "allowed": stale,
            "safe": True,
            "reason": "ok" if stale else "job_not_stale_or_not_locked",
        }

    if action == "cancel_job":
        terminal = state in {"PUBLISHED", "CANCELLED", "CLEANED"}
        return {
            "allowed": not terminal,
            "safe": False,
            "reason": "ok" if not terminal else "cancel_not_allowed",
            "warning": "Risky action: this permanently cancels current job execution.",
        }

    if action in {"force_cleanup_artifacts", "reenqueue_allowed_stage"}:
        return {
            "allowed": False,
            "safe": False,
            "reason": "primitive_not_available",
            "warning": "This action is not yet wired to a backend primitive.",
        }

    raise RecoveryActionError("OPS_RECOVERY_ACTION_UNKNOWN", "Unknown recovery action", status_code=404)


def execute_action(
    conn: sqlite3.Connection,
    *,
    job: dict[str, Any],
    action: str,
    runtime: RecoveryRuntime,
) -> dict[str, Any]:
    preview = preview_action(conn, job=job, action=action, runtime=runtime)
    if not preview.get("allowed"):
        raise RecoveryActionError("OPS_RECOVERY_PRECONDITION_FAILED", "Action preconditions are not met", status_code=409)

    job_id = int(job["id"])

    if action == "retry_failed":
        try:
            result = retry_failed_ui_job(conn, source_job_id=job_id)
        except UiJobRetryNotFoundError as exc:
            raise RecoveryActionError("OPS_RECOVERY_JOB_NOT_FOUND", str(exc), status_code=404) from exc
        except UiJobRetryStatusError as exc:
            raise RecoveryActionError("OPS_RECOVERY_PRECONDITION_FAILED", str(exc), status_code=409) from exc
        return {"ok": True, "retry_job_id": int(result.retry_job_id), "created": bool(result.created)}

    if action == "reclaim_stale":
        attempt = dbm.increment_attempt(conn, job_id)
        reason = f"reclaimed stale lock from {job.get('state')}"
        if attempt < int(runtime.max_render_attempts):
            dbm.schedule_retry(
                conn,
                job_id,
                next_state="READY_FOR_RENDER",
                stage="FETCH",
                error_reason=f"attempt={attempt} retry: {reason}",
                backoff_sec=int(runtime.retry_backoff_sec),
            )
            return {"ok": True, "reclaimed": True, "terminal": False, "attempt": attempt}
        dbm.update_job_state(
            conn,
            job_id,
            state="RENDER_FAILED",
            stage="RENDER",
            error_reason=f"attempt={attempt} terminal: {reason}",
        )
        dbm.clear_retry(conn, job_id)
        dbm.force_unlock(conn, job_id)
        return {"ok": True, "reclaimed": True, "terminal": True, "attempt": attempt}

    if action == "cancel_job":
        dbm.cancel_job(conn, job_id, reason="cancelled by recovery console")
        return {"ok": True, "cancelled": True}

    raise RecoveryActionError("OPS_RECOVERY_ACTION_UNAVAILABLE", "Action primitive is not available", status_code=409)


def insert_recovery_audit(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    action: str,
    phase: str,
    requested_by: str | None,
    request_payload: dict[str, Any],
    result_payload: dict[str, Any],
    ok: bool,
    error_code: str | None = None,
) -> None:
    risk_level = "safe" if action in SAFE_ACTIONS else "risky"
    preview_allowed = result_payload.get("allowed") if phase == "preview" else None
    requested_at = str(dbm.now_ts())
    message = str(result_payload.get("message") or "") or None
    state_before = request_payload.get("state_before")
    state_after = result_payload.get("state_after")
    status = "ok" if ok else "failed"

    conn.execute(
        """
        INSERT INTO recovery_action_audit(
            job_id, action_name, risk_level, requested_by, requested_at,
            preview_allowed, execute_attempted, result_status, result_code,
            message, state_before, state_after, details_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(job_id),
            action,
            risk_level,
            requested_by,
            requested_at,
            None if preview_allowed is None else (1 if bool(preview_allowed) else 0),
            1 if phase == "execute" else 0,
            status,
            error_code,
            message,
            None if state_before is None else str(state_before),
            None if state_after is None else str(state_after),
            json.dumps(
                {
                    "phase": phase,
                    "request_payload": request_payload,
                    "result_payload": result_payload,
                },
                ensure_ascii=False,
            ),
        ),
    )
