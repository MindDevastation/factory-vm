from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any

from services.common.db import UI_JOB_STATES

logger = logging.getLogger(__name__)

TERMINAL_JOB_STATUSES: frozenset[str] = frozenset(
    {
        "RENDER_FAILED",
        "FAILED",
        "QA_FAILED",
        "UPLOAD_FAILED",
        "REJECTED",
        "PUBLISHED",
        "CANCELLED",
        "CLEANED",
    }
)
KNOWN_JOB_STATUSES: frozenset[str] = frozenset(UI_JOB_STATES)
OPEN_JOB_STATUSES: frozenset[str] = frozenset(status for status in UI_JOB_STATES if status not in TERMINAL_JOB_STATUSES)


class ReleaseJobCreationFoundationError(Exception):
    def __init__(self, *, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


@dataclass(frozen=True)
class OpenJobDiagnostics:
    release_id: int
    current_open_job_id: int | None
    linked_job_exists: bool
    open_jobs_count: int
    invariant_status: str
    invariant_reason: str | None


def map_job_status_to_category(status: Any) -> str:
    value = str(status or "").strip().upper()
    if not value or value not in KNOWN_JOB_STATUSES:
        raise ReleaseJobCreationFoundationError(
            code="PRJ_RELEASE_STATE_INVALID",
            message="Job state is invalid for release-job invariant evaluation.",
            details={"job_state": value or None},
        )
    if value in TERMINAL_JOB_STATUSES:
        return "TERMINAL"
    if value in OPEN_JOB_STATUSES:
        return "OPEN"
    raise ReleaseJobCreationFoundationError(
        code="PRJ_RELEASE_STATE_INVALID",
        message="Job state is invalid for release-job invariant evaluation.",
        details={"job_state": value},
    )


def get_release_by_id(conn: sqlite3.Connection, *, release_id: int) -> dict[str, Any] | None:
    return conn.execute(
        """
        SELECT r.*, c.slug AS channel_slug
        FROM releases r
        JOIN channels c ON c.id = r.channel_id
        WHERE r.id = ?
        LIMIT 1
        """,
        (release_id,),
    ).fetchone()


def get_current_open_job_for_release(conn: sqlite3.Connection, *, release: dict[str, Any]) -> dict[str, Any] | None:
    pointer = release.get("current_open_job_id")
    if pointer is None:
        return None
    return conn.execute("SELECT * FROM jobs WHERE id = ? LIMIT 1", (int(pointer),)).fetchone()


def find_open_jobs_for_release(conn: sqlite3.Connection, *, release_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM jobs WHERE release_id = ? ORDER BY id ASC",
        (release_id,),
    ).fetchall()
    open_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            category = map_job_status_to_category(row.get("state"))
        except ReleaseJobCreationFoundationError as exc:
            _log_invariant_violation(
                release_id=release_id,
                current_open_job_id=None,
                job_id=int(row["id"]),
                error_code=exc.code,
                invariant_status="CURRENT_POINTER_INCONSISTENT",
            )
            raise
        if category == "OPEN":
            open_rows.append(row)
    return open_rows


def validate_current_open_pointer(
    *,
    release: dict[str, Any],
    current_open_job: dict[str, Any] | None,
) -> None:
    release_id = int(release["id"])
    pointer = release.get("current_open_job_id")
    if pointer is None:
        return
    current_open_job_id = int(pointer)

    if current_open_job is None:
        _log_invariant_violation(
            release_id=release_id,
            current_open_job_id=current_open_job_id,
            job_id=current_open_job_id,
            error_code="PRJ_OPEN_JOB_NOT_FOUND",
            invariant_status="CURRENT_POINTER_INCONSISTENT",
        )
        raise ReleaseJobCreationFoundationError(
            code="PRJ_OPEN_JOB_NOT_FOUND",
            message="Current open job pointer points to a missing job.",
            details={"release_id": release_id, "current_open_job_id": current_open_job_id},
        )

    job_id = int(current_open_job["id"])
    job_release_id = int(current_open_job["release_id"])
    if job_release_id != release_id:
        _log_invariant_violation(
            release_id=release_id,
            current_open_job_id=current_open_job_id,
            job_id=job_id,
            error_code="PRJ_OPEN_JOB_RELATION_INCONSISTENT",
            invariant_status="CURRENT_POINTER_INCONSISTENT",
        )
        raise ReleaseJobCreationFoundationError(
            code="PRJ_OPEN_JOB_RELATION_INCONSISTENT",
            message="Current open job pointer targets a job belonging to another release.",
            details={
                "release_id": release_id,
                "current_open_job_id": current_open_job_id,
                "job_release_id": job_release_id,
            },
        )

    try:
        current_category = map_job_status_to_category(current_open_job.get("state"))
    except ReleaseJobCreationFoundationError as exc:
        _log_invariant_violation(
            release_id=release_id,
            current_open_job_id=current_open_job_id,
            job_id=job_id,
            error_code=exc.code,
            invariant_status="CURRENT_POINTER_INCONSISTENT",
        )
        raise

    if current_category != "OPEN":
        _log_invariant_violation(
            release_id=release_id,
            current_open_job_id=current_open_job_id,
            job_id=job_id,
            error_code="PRJ_OPEN_JOB_STATUS_INVALID",
            invariant_status="CURRENT_POINTER_INCONSISTENT",
        )
        raise ReleaseJobCreationFoundationError(
            code="PRJ_OPEN_JOB_STATUS_INVALID",
            message="Current open job pointer targets a terminal job.",
            details={
                "release_id": release_id,
                "current_open_job_id": current_open_job_id,
                "job_state": current_open_job.get("state"),
            },
        )


def validate_open_job_invariants(conn: sqlite3.Connection, *, release: dict[str, Any]) -> OpenJobDiagnostics:
    release_id = int(release["id"])
    pointer = release.get("current_open_job_id")
    pointer_id = int(pointer) if pointer is not None else None
    current_open_job = get_current_open_job_for_release(conn, release=release)

    validate_current_open_pointer(release=release, current_open_job=current_open_job)

    open_jobs = find_open_jobs_for_release(conn, release_id=release_id)
    open_job_ids = [int(row["id"]) for row in open_jobs]

    if len(open_job_ids) > 1:
        _log_invariant_violation(
            release_id=release_id,
            current_open_job_id=pointer_id,
            job_id=None,
            error_code="PRJ_MULTIPLE_OPEN_JOBS",
            invariant_status="MULTIPLE_OPEN_INCONSISTENT",
        )
        raise ReleaseJobCreationFoundationError(
            code="PRJ_MULTIPLE_OPEN_JOBS",
            message="Multiple open jobs detected for release.",
            details={"release_id": release_id, "open_job_ids": open_job_ids},
        )

    if len(open_job_ids) == 1 and pointer_id is None:
        _log_invariant_violation(
            release_id=release_id,
            current_open_job_id=None,
            job_id=open_job_ids[0],
            error_code="PRJ_OPEN_JOB_RELATION_INCONSISTENT",
            invariant_status="CURRENT_POINTER_INCONSISTENT",
        )
        raise ReleaseJobCreationFoundationError(
            code="PRJ_OPEN_JOB_RELATION_INCONSISTENT",
            message="Open job exists but current open pointer is missing.",
            details={"release_id": release_id, "open_job_id": open_job_ids[0]},
        )

    if len(open_job_ids) == 1 and pointer_id is not None and pointer_id != open_job_ids[0]:
        _log_invariant_violation(
            release_id=release_id,
            current_open_job_id=pointer_id,
            job_id=open_job_ids[0],
            error_code="PRJ_OPEN_JOB_RELATION_INCONSISTENT",
            invariant_status="CURRENT_POINTER_INCONSISTENT",
        )
        raise ReleaseJobCreationFoundationError(
            code="PRJ_OPEN_JOB_RELATION_INCONSISTENT",
            message="Current open pointer does not match discovered open job.",
            details={
                "release_id": release_id,
                "current_open_job_id": pointer_id,
                "discovered_open_job_id": open_job_ids[0],
            },
        )

    invariant_status = "OK"
    invariant_reason = None
    if pointer_id is None:
        invariant_status = "NO_OPEN_JOB"
    else:
        invariant_status = "HAS_OPEN_JOB"

    return OpenJobDiagnostics(
        release_id=release_id,
        current_open_job_id=pointer_id,
        linked_job_exists=current_open_job is not None,
        open_jobs_count=len(open_job_ids),
        invariant_status=invariant_status,
        invariant_reason=invariant_reason,
    )


def build_release_job_create_payload(*, release: dict[str, Any]) -> dict[str, Any]:
    release_id = release.get("id")
    if release_id is None:
        raise ReleaseJobCreationFoundationError(
            code="PRJ_RELEASE_STATE_INVALID",
            message="Release payload is structurally invalid for job payload derivation.",
            details={"release_id": release_id},
        )

    # The current jobs schema persists only release_id as release-derived mandatory field.
    # State/stage/priority/attempt are create-time system defaults in current subsystem.
    return {
        "job": {
            "release_id": int(release_id),
            "state": "DRAFT",
            "stage": "DRAFT",
            "priority": 0,
            "attempt": 0,
        },
        "context": {
            "release_id": int(release_id),
            "channel_slug": release.get("channel_slug"),
        },
    }


def derive_job_creation_state_summary_inputs(
    *,
    release: dict[str, Any],
    diagnostics: OpenJobDiagnostics,
    action_enabled: bool,
) -> dict[str, Any]:
    state = "ACTION_DISABLED"
    if action_enabled:
        if diagnostics.invariant_status == "HAS_OPEN_JOB":
            state = "HAS_OPEN_JOB"
        elif diagnostics.invariant_status == "NO_OPEN_JOB":
            state = "NO_OPEN_JOB"
        elif diagnostics.invariant_status == "MULTIPLE_OPEN_INCONSISTENT":
            state = "MULTIPLE_OPEN_INCONSISTENT"
        elif diagnostics.invariant_status == "CURRENT_POINTER_INCONSISTENT":
            state = "CURRENT_POINTER_INCONSISTENT"

    return {
        "release_id": int(release["id"]),
        "current_open_job_id": release.get("current_open_job_id"),
        "job_creation_state": state,
        "invariant_status": diagnostics.invariant_status,
        "invariant_reason": diagnostics.invariant_reason,
        "action_enabled": bool(action_enabled),
    }


def derive_open_job_diagnostics_inputs(*, diagnostics: OpenJobDiagnostics) -> dict[str, Any]:
    return {
        "release_id": diagnostics.release_id,
        "current_open_job_id": diagnostics.current_open_job_id,
        "linked_job_exists": diagnostics.linked_job_exists,
        "open_jobs_count": diagnostics.open_jobs_count,
        "invariant_status": diagnostics.invariant_status,
        "invariant_reason": diagnostics.invariant_reason,
    }


def _log_invariant_violation(
    *,
    release_id: int,
    current_open_job_id: int | None,
    job_id: int | None,
    error_code: str,
    invariant_status: str,
) -> None:
    logger.warning(
        "release.job_creation.invariant_violation "
        "release_id=%s current_open_job_id=%s job_id=%s error_code=%s invariant_status=%s",
        release_id,
        current_open_job_id,
        job_id,
        error_code,
        invariant_status,
    )
