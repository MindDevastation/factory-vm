from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from services.common import db as dbm
from services.ui_jobs import UiJobRetryNotFoundError, UiJobRetryStatusError, retry_failed_ui_job
from services.workers.cleanup import cleanup_published_artifacts

SAFE_ACTIONS = {"retry_failed", "reclaim_stale"}
RISKY_ACTIONS = {"cancel_job", "force_cleanup_artifacts", "reenqueue_allowed_stage"}
CANONICAL_ACTIONS = SAFE_ACTIONS | RISKY_ACTIONS
TERMINAL_STATES = {"PUBLISHED", "CANCELLED", "CLEANED"}
FAILED_STATES = {"FAILED", "RENDER_FAILED", "QA_FAILED", "UPLOAD_FAILED"}
ACTIVE_STATES = {"WAITING_INPUTS", "FETCHING_INPUTS", "READY_FOR_RENDER", "RENDERING", "QA_RUNNING", "UPLOADING"}


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
    storage_root: str


def allowed_reenqueue_stage_tokens(job: dict[str, Any]) -> list[dict[str, str]]:
    state = str(job.get("state") or "")
    if state not in FAILED_STATES:
        return []
    return [
        {
            "token": "restart_from_fetch_v1",
            "label": "Restart pipeline from FETCH and re-enter READY_FOR_RENDER",
            "next_state": "READY_FOR_RENDER",
            "next_stage": "FETCH",
        }
    ]


def _cleanup_actionability(conn: sqlite3.Connection, *, job: dict[str, Any], now_ts: float) -> dict[str, Any]:
    job_id = int(job["id"])
    state = str(job.get("state") or "")
    active_conflict = state in ACTIVE_STATES
    cleanup_pending = state == "PUBLISHED" and job.get("delete_mp4_at") is not None and float(job.get("delete_mp4_at") or 0) <= now_ts
    output_row = conn.execute(
        "SELECT 1 FROM job_outputs WHERE job_id = ? AND role IN ('MP4', 'PREVIEW_60S') LIMIT 1",
        (job_id,),
    ).fetchone()
    artifacts_pending = state == "PUBLISHED" and output_row is not None
    has_target = bool(output_row)
    cleanup_safe = (cleanup_pending or artifacts_pending) and not active_conflict
    return {
        "cleanup_pending": cleanup_pending,
        "artifacts_pending": artifacts_pending,
        "active_conflict": active_conflict,
        "has_target": has_target,
        "cleanup_safe": cleanup_safe,
    }


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

    if action == "force_cleanup_artifacts":
        actionability = _cleanup_actionability(conn, job=job, now_ts=now_ts)
        allowed = bool(
            actionability["cleanup_safe"]
            and actionability["has_target"]
            and (actionability["cleanup_pending"] or actionability["artifacts_pending"])
            and not actionability["active_conflict"]
        )
        warnings = ["Risky action: this marks published artifacts as force-cleaned."]
        if actionability["active_conflict"]:
            warnings.append("Blocked: job appears active/running.")
        if not actionability["has_target"]:
            warnings.append("Blocked: no cleanup target exists for this job.")
        return {
            "allowed": allowed,
            "risk_level": "risky",
            "summary": "Force cleanup of published artifacts using cleanup worker primitive",
            "warnings": warnings,
            "preconditions": [
                {
                    "name": "cleanup_pending_or_artifacts_pending",
                    "ok": bool(actionability["cleanup_pending"] or actionability["artifacts_pending"]),
                    "detail": f"cleanup_pending={actionability['cleanup_pending']} artifacts_pending={actionability['artifacts_pending']}",
                },
                {
                    "name": "job_not_actively_running",
                    "ok": not actionability["active_conflict"],
                    "detail": f"state={state}",
                },
                {
                    "name": "cleanup_target_exists",
                    "ok": actionability["has_target"],
                    "detail": "job_outputs has MP4/PREVIEW target" if actionability["has_target"] else "no MP4/PREVIEW output rows",
                },
                {
                    "name": "cleanup_primitive_safe",
                    "ok": actionability["cleanup_safe"],
                    "detail": f"safe={actionability['cleanup_safe']}",
                },
            ],
            "safe": False,
            "reason": "ok" if allowed else "cleanup_not_allowed",
        }

    if action == "reenqueue_allowed_stage":
        tokens = allowed_reenqueue_stage_tokens(job)
        allowed = bool(tokens)
        return {
            "allowed": allowed,
            "risk_level": "risky",
            "summary": "Re-enqueue job to a backend-approved recovery stage token",
            "warnings": ["Risky action: execution only accepts backend-issued stage tokens."],
            "preconditions": [
                {
                    "name": "restartable_stage_token_available",
                    "ok": allowed,
                    "detail": f"tokens={','.join(token['token'] for token in tokens)}" if tokens else "no restartable tokens for this state",
                }
            ],
            "safe": False,
            "allowed_stage_tokens": tokens,
            "reason": "ok" if allowed else "stage_token_not_available",
        }

    raise RecoveryActionError("ORC_ACTION_NOT_SUPPORTED", "Unknown recovery action", status_code=404)


def execute_action(
    conn: sqlite3.Connection,
    *,
    job: dict[str, Any],
    action: str,
    runtime: RecoveryRuntime,
    stage_token: str | None = None,
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

    if action == "force_cleanup_artifacts":
        result = cleanup_published_artifacts(conn, storage_root=runtime.storage_root, job_id=job_id)
        if not result.get("ok"):
            raise RecoveryActionError("ORC_ACTION_NOT_ALLOWED", str(result.get("reason") or "cleanup blocked"), status_code=409)
        return result

    if action == "reenqueue_allowed_stage":
        allowed_tokens = {item["token"] for item in allowed_reenqueue_stage_tokens(job)}
        token = str(stage_token or "").strip()
        if not token or token not in allowed_tokens:
            raise RecoveryActionError("ORC_STAGE_TOKEN_INVALID", "Invalid or missing stage token", status_code=409)
        attempt = dbm.increment_attempt(conn, job_id)
        dbm.schedule_retry(
            conn,
            job_id,
            next_state="READY_FOR_RENDER",
            stage="FETCH",
            error_reason=f"recovery_reenqueue token={token} attempt={attempt}",
            backoff_sec=0,
        )
        return {
            "ok": True,
            "reenqueued": True,
            "stage_token": token,
            "next_state": "READY_FOR_RENDER",
            "next_stage": "FETCH",
            "attempt": attempt,
        }

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
    scaffold_cols = {
        "job_id",
        "action_name",
        "risk_level",
        "requested_by",
        "requested_at",
        "preview_allowed",
        "execute_attempted",
        "result_status",
        "result_code",
        "message",
        "state_before",
        "state_after",
        "details_json",
    }
    if not legacy_write_cols.issubset(cols) and not scaffold_cols.issubset(cols):
        # Read-only slice: migration scaffold may be present without write wiring.
        return None

    now_ts = dbm.now_ts()
    details_payload = result_payload.get("details") if isinstance(result_payload, dict) else None
    row_payload: dict[str, Any] = {
        "job_id": int(job_id),
        "action": action,
        "phase": phase,
        "requested_by": requested_by,
        "request_payload_json": json.dumps(request_payload, ensure_ascii=False),
        "result_payload_json": json.dumps(result_payload, ensure_ascii=False),
        "ok": 1 if ok else 0,
        "error_code": error_code,
        "created_at": now_ts,
        "action_name": action,
        "risk_level": "risky" if action in RISKY_ACTIONS else "safe",
        "requested_at": now_ts,
        "preview_allowed": 1 if ok else 0,
        "execute_attempted": 1 if phase == "execute" else 0,
        "result_status": str(result_payload.get("result") or ("success" if ok else "failure")),
        "result_code": error_code,
        "message": result_payload.get("message") if isinstance(result_payload, dict) else None,
        "state_before": result_payload.get("state_before") if isinstance(result_payload, dict) else None,
        "state_after": result_payload.get("state_after") if isinstance(result_payload, dict) else None,
        "details_json": json.dumps(details_payload, ensure_ascii=False) if details_payload is not None else None,
    }
    write_cols = [name for name in row_payload.keys() if name in cols]
    placeholders = ", ".join("?" for _ in write_cols)
    cursor = conn.execute(
        f"INSERT INTO recovery_action_audit({', '.join(write_cols)}) VALUES({placeholders})",
        tuple(row_payload[name] for name in write_cols),
    )
    return int(cursor.lastrowid)
