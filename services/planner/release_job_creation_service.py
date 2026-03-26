from __future__ import annotations

from dataclasses import dataclass
import logging
import sqlite3
from typing import Any

from services.common import db as dbm
from services.planner.release_job_creation_foundation import (
    ReleaseJobCreationFoundationError,
    derive_open_job_diagnostics_inputs,
    get_current_open_job_for_release,
    get_release_by_id,
    validate_open_job_invariants,
)

logger = logging.getLogger(__name__)


class ReleaseJobCreationError(Exception):
    def __init__(self, *, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


@dataclass(frozen=True)
class ReleaseJobCreateOrSelectResult:
    release_id: int
    result: str
    job: dict[str, Any]
    current_open_relation: dict[str, int]
    job_creation_state_summary: dict[str, Any]
    open_job_diagnostics: dict[str, Any]


class ReleaseJobCreationService:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def create_or_select(self, *, release_id: int) -> ReleaseJobCreateOrSelectResult:
        self._log_event(
            "release.job_creation.started",
            release_id=release_id,
            job_id=None,
            result="STARTED",
            error_code=None,
            transaction_path="fail",
        )
        try:
            return self._create_or_select_with_transaction(release_id=release_id)
        except ReleaseJobCreationError:
            raise
        except Exception as exc:
            self._log_event(
                "release.job_creation.failed",
                release_id=release_id,
                job_id=None,
                result="FAILED",
                error_code="PRJ_JOB_CREATE_FAILED",
                transaction_path="fail",
            )
            raise ReleaseJobCreationError(
                code="PRJ_JOB_CREATE_FAILED",
                message="Job creation failed.",
            ) from exc

    def _create_or_select_with_transaction(self, *, release_id: int) -> ReleaseJobCreateOrSelectResult:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            out = self._create_or_select_in_tx(release_id=release_id)
            self._conn.execute("COMMIT")
            return out
        except sqlite3.IntegrityError:
            self._conn.execute("ROLLBACK")
            return self._recover_after_concurrency_conflict(release_id=release_id)
        except ReleaseJobCreationError:
            self._conn.execute("ROLLBACK")
            raise
        except ReleaseJobCreationFoundationError as exc:
            self._conn.execute("ROLLBACK")
            raise self._map_foundation_error(exc) from exc
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _create_or_select_in_tx(self, *, release_id: int) -> ReleaseJobCreateOrSelectResult:
        release = self._load_release_or_raise(release_id=release_id)
        self._validate_structural_integrity(release=release)
        self._validate_materialized_release_eligibility(release=release)

        diagnostics = validate_open_job_invariants(self._conn, release=release)
        current_open_job = get_current_open_job_for_release(self._conn, release=release)

        if current_open_job is not None:
            return self._build_result(
                release=release,
                job=current_open_job,
                result="RETURNED_EXISTING_OPEN_JOB",
                diagnostics=diagnostics,
                transaction_path="return_existing",
            )

        ts = dbm.now_ts()
        new_job_id = dbm.insert_job_with_lineage_defaults(
            self._conn,
            release_id=int(release["id"]),
            job_type="UI",
            state="DRAFT",
            stage="DRAFT",
            priority=0,
            attempt=0,
            created_at=ts,
            updated_at=ts,
        )
        self._conn.execute(
            "UPDATE releases SET current_open_job_id = ? WHERE id = ?",
            (new_job_id, int(release["id"])),
        )

        release_after = self._load_release_or_raise(release_id=release_id)
        diagnostics_after = validate_open_job_invariants(self._conn, release=release_after)
        job_after = get_current_open_job_for_release(self._conn, release=release_after)
        if job_after is None:
            raise ReleaseJobCreationError(
                code="PRJ_JOB_CREATE_FAILED",
                message="Job creation failed.",
                details={"release_id": release_id},
            )

        return self._build_result(
            release=release_after,
            job=job_after,
            result="CREATED_NEW_JOB",
            diagnostics=diagnostics_after,
            transaction_path="create_new",
        )

    def _recover_after_concurrency_conflict(self, *, release_id: int) -> ReleaseJobCreateOrSelectResult:
        try:
            release = self._load_release_or_raise(release_id=release_id)
            self._validate_structural_integrity(release=release)
            self._validate_materialized_release_eligibility(release=release)
            diagnostics = validate_open_job_invariants(self._conn, release=release)
            current_open_job = get_current_open_job_for_release(self._conn, release=release)
        except ReleaseJobCreationError:
            raise
        except ReleaseJobCreationFoundationError as exc:
            raise self._map_foundation_error(exc) from exc

        if current_open_job is not None:
            return self._build_result(
                release=release,
                job=current_open_job,
                result="RETURNED_EXISTING_OPEN_JOB",
                diagnostics=diagnostics,
                transaction_path="return_existing",
            )

        self._log_event(
            "release.job_creation.failed",
            release_id=release_id,
            job_id=None,
            result="FAILED",
            error_code="PRJ_CONCURRENCY_CONFLICT",
            transaction_path="fail",
        )
        raise ReleaseJobCreationError(
            code="PRJ_CONCURRENCY_CONFLICT",
            message="Concurrency conflict could not be resolved.",
            details={"release_id": release_id},
        )

    def _load_release_or_raise(self, *, release_id: int) -> dict[str, Any]:
        release = get_release_by_id(self._conn, release_id=release_id)
        if release is None:
            self._log_event(
                "release.job_creation.failed",
                release_id=release_id,
                job_id=None,
                result="FAILED",
                error_code="PRJ_RELEASE_NOT_FOUND",
                transaction_path="fail",
            )
            raise ReleaseJobCreationError(
                code="PRJ_RELEASE_NOT_FOUND",
                message="Release was not found.",
                details={"release_id": release_id},
            )
        return release

    def _validate_structural_integrity(self, *, release: dict[str, Any]) -> None:
        release_id = release.get("id")
        channel_id = release.get("channel_id")
        channel_slug = str(release.get("channel_slug") or "").strip()
        if release_id is None or channel_id is None or not channel_slug:
            self._log_event(
                "release.job_creation.failed",
                release_id=int(release_id) if release_id is not None else -1,
                job_id=None,
                result="FAILED",
                error_code="PRJ_RELEASE_STATE_INVALID",
                transaction_path="fail",
            )
            raise ReleaseJobCreationError(
                code="PRJ_RELEASE_STATE_INVALID",
                message="Release state is invalid for job creation.",
                details={
                    "release_id": release_id,
                    "channel_id": channel_id,
                    "channel_slug": channel_slug or None,
                },
            )

    def _validate_materialized_release_eligibility(self, *, release: dict[str, Any]) -> None:
        origin_meta_file_id = str(release.get("origin_meta_file_id") or "").strip()
        if not origin_meta_file_id:
            release_id = int(release["id"])
            self._log_event(
                "release.job_creation.not_eligible",
                release_id=release_id,
                job_id=None,
                result="FAILED",
                error_code="PRJ_RELEASE_NOT_ELIGIBLE",
                transaction_path="fail",
            )
            raise ReleaseJobCreationError(
                code="PRJ_RELEASE_NOT_ELIGIBLE",
                message="Release is not currently eligible for job creation.",
                details={"release_id": release_id},
            )

    def _build_result(
        self,
        *,
        release: dict[str, Any],
        job: dict[str, Any],
        result: str,
        diagnostics: Any,
        transaction_path: str,
    ) -> ReleaseJobCreateOrSelectResult:
        release_id = int(release["id"])
        job_id = int(job["id"])
        summary = {
            "job_creation_state": "HAS_OPEN_JOB",
            "job_id": job_id,
            "action_reason": None,
        }
        diag_payload = derive_open_job_diagnostics_inputs(diagnostics=diagnostics)
        self._log_event(
            "release.job_creation.created_new" if result == "CREATED_NEW_JOB" else "release.job_creation.returned_existing",
            release_id=release_id,
            job_id=job_id,
            result=result,
            error_code=None,
            transaction_path=transaction_path,
        )
        return ReleaseJobCreateOrSelectResult(
            release_id=release_id,
            result=result,
            job={
                "id": job_id,
                "release_id": int(job["release_id"]),
                "channel_slug": str(release.get("channel_slug") or ""),
                "status": str(job.get("state") or ""),
            },
            current_open_relation={
                "release_id": release_id,
                "job_id": job_id,
            },
            job_creation_state_summary=summary,
            open_job_diagnostics=diag_payload,
        )

    def _map_foundation_error(self, exc: ReleaseJobCreationFoundationError) -> ReleaseJobCreationError:
        code = exc.code
        event = "release.job_creation.failed"
        if code == "PRJ_MULTIPLE_OPEN_JOBS":
            event = "release.job_creation.multiple_open_detected"
        elif code in {"PRJ_OPEN_JOB_NOT_FOUND", "PRJ_OPEN_JOB_RELATION_INCONSISTENT", "PRJ_OPEN_JOB_STATUS_INVALID"}:
            event = "release.job_creation.relation_inconsistent"
        self._log_event(
            event,
            release_id=int(exc.details.get("release_id") or -1),
            job_id=exc.details.get("current_open_job_id") or exc.details.get("open_job_id"),
            result="FAILED",
            error_code=code,
            transaction_path="fail",
        )
        return ReleaseJobCreationError(
            code=code,
            message=exc.message,
            details=exc.details,
        )

    def _log_event(
        self,
        event: str,
        *,
        release_id: int,
        job_id: int | None,
        result: str,
        error_code: str | None,
        transaction_path: str,
    ) -> None:
        logger.info(
            "%s release_id=%s job_id=%s result=%s error_code=%s transaction_path=%s",
            event,
            release_id,
            job_id,
            result,
            error_code,
            transaction_path,
        )
