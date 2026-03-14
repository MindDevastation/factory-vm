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
TERMINAL_STATES = {"PUBLISHED", "CANCELLED", "CLEANED"}
FAILED_STATES = {"FAILED", "RENDER_FAILED", "QA_FAILED", "UPLOAD_FAILED"}


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
        allowed = bool(state in FAILED_STATES and retry_child is None)
        summary = "Create or return retry child job from failed source job"
        warnings = [] if allowed else ["Retry is only available for failed jobs with no existing retry child."]
        preconditions = [
            {"name": "job_in_failed_state", "ok": state in FAILED_STATES, "detail": f"state={state}"},
            {
                "name": "no_existing_retry_child",
                "ok": retry_child is None,
                "detail": "retry child absent" if retry_child is None else f"retry child id={int(retry_child['id'])}",
            },
        ]
        return {
            "allowed": allowed,
            "risk_level": "safe",
            "summary": summary,
            "warnings": warnings,
            "preconditions": preconditions,
            "reason": "ok" if allowed else "retry_not_allowed",
            "safe": True,
        }

    if action == "reclaim_stale":
        stale = _is_stale_locked_job(job, now_ts, runtime.job_lock_ttl_sec)
        summary = "Reclaim stale lock and route job through retry/fallback path"
        warnings = [] if stale else ["Job must be stale and lock-expired to reclaim."]
        preconditions = [
            {
                "name": "stale_lock_detected",
                "ok": stale,
                "detail": (
                    f"locked_at={job.get('locked_at')} locked_by={job.get('locked_by')} state={state}"
                    if not stale
                    else "lock exceeded ttl"
                ),
            }
        ]
        return {
            "allowed": stale,
            "risk_level": "safe",
            "summary": summary,
            "warnings": warnings,
            "preconditions": preconditions,
            "safe": True,
            "reason": "ok" if stale else "job_not_stale_or_not_locked",
        }

    if action == "cancel_job":
        terminal = state in TERMINAL_STATES
        allowed = not terminal
        summary = "Cancel active job and clear lock/retry markers"
        warnings = ["Risky action: cancellation is not reversible."]
        preconditions = [
            {
                "name": "job_not_terminal",
                "ok": not terminal,
                "detail": f"state={state}",
            }
        ]
        return {
            "allowed": allowed,
            "risk_level": "risky",
            "summary": summary,
            "warnings": warnings,
            "preconditions": preconditions,
            "safe": False,
            "reason": "ok" if allowed else "cancel_not_allowed",
            "warning": "Risky action: this permanently cancels current job execution.",
        }

    if action in {"force_cleanup_artifacts", "reenqueue_allowed_stage"}:
        return {
            "allowed": False,
            "safe": False,
            "reason": "primitive_not_available",
            "warning": "This action is not yet wired to a backend primitive.",
        }

    raise RecoveryActionError("ORC_ACTION_NOT_SUPPORTED", "Unknown recovery action", status_code=404)


def execute_action(
    conn: sqlite3.Connection,
    *,
    job: dict[str, Any],
    action: str,
    runtime: RecoveryRuntime,
) -> dict[str, Any]:
    preview = preview_action(conn, job=job, action=action, runtime=runtime)
    if not preview.get("allowed"):
        raise RecoveryActionError("ORC_ACTION_NOT_ALLOWED", "Action preconditions are not met", status_code=409)

    job_id = int(job["id"])

    if action == "retry_failed":
        try:
            result = retry_failed_ui_job(conn, source_job_id=job_id)
        except UiJobRetryNotFoundError as exc:
            raise RecoveryActionError("ORC_JOB_NOT_FOUND", str(exc), status_code=404) from exc
        except UiJobRetryStatusError as exc:
            raise RecoveryActionError("ORC_ACTION_NOT_ALLOWED", str(exc), status_code=409) from exc
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

    raise RecoveryActionError("ORC_ACTION_NOT_SUPPORTED", "Action primitive is not available", status_code=409)


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
) -> int | None:
    cols = {
        str(row.get("name"))
        for row in conn.execute("PRAGMA table_info(recovery_action_audit)").fetchall()
        if isinstance(row, dict) and row.get("name")
    }
    legacy_write_cols = {
        "job_id",
        "action",
        "phase",
        "requested_by",
        "request_payload_json",
        "result_payload_json",
        "ok",
        "error_code",
        "created_at",
    }
    if not legacy_write_cols.issubset(cols):
        # Read-only slice: migration scaffold may be present without execute/preview write wiring yet.
        return None

    cursor = conn.execute(
        """
        INSERT INTO recovery_action_audit(
            job_id, action, phase, requested_by, request_payload_json, result_payload_json, ok, error_code, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            int(job_id),
            action,
            phase,
            requested_by,
            json.dumps(request_payload, ensure_ascii=False),
            json.dumps(result_payload, ensure_ascii=False),
            1 if ok else 0,
            error_code,
            dbm.now_ts(),
        ),
    )
    return int(cursor.lastrowid)
