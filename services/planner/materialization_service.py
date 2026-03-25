from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import sqlite3
import time
from typing import Any

from services.planner.materialization_foundation import (
    BindingInvariantResult,
    MaterializationBindingError,
    build_release_payload_from_planned_release,
    derive_binding_diagnostics_inputs,
    derive_materialization_state_summary_inputs,
    get_bound_release_for_planned_release,
    get_planned_release_by_id,
    set_materialized_release_id,
    validate_binding_invariants,
)
from services.planner.planned_release_readiness_service import (
    PlannedReleaseReadinessNotFoundError,
    PlannedReleaseReadinessService,
)

logger = logging.getLogger(__name__)


class PlannerMaterializationError(Exception):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        planned_release_id: int | None = None,
        materialization_state_summary: dict[str, Any] | None = None,
        binding_diagnostics: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.planned_release_id = planned_release_id
        self.materialization_state_summary = materialization_state_summary
        self.binding_diagnostics = binding_diagnostics


@dataclass(frozen=True)
class MaterializationResult:
    planned_release_id: int
    result: str
    release_id: int
    release_channel_slug: str | None
    materialized_binding: dict[str, Any]
    materialization_state_summary: dict[str, Any]
    binding_diagnostics: dict[str, Any]

    @property
    def planner_item_id(self) -> int:
        return self.planned_release_id

    @property
    def planner_status(self) -> str:
        return "LOCKED"

    @property
    def materialization_status(self) -> str:
        return "CREATED" if self.result == "CREATED_NEW" else "EXISTING_BINDING"


class PlannerMaterializationService:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._readiness = PlannedReleaseReadinessService(conn)

    def materialize_planned_release(self, *, planned_release_id: int, created_by: str | None) -> MaterializationResult:
        del created_by  # no side effects stored in v1 canonical materialization
        logger.info(
            "planner.materialization.started planned_release_id=%s result=STARTED transaction_path=fail",
            planned_release_id,
        )

        try:
            return self._materialize_with_transaction(planned_release_id=planned_release_id)
        except PlannerMaterializationError:
            raise
        except Exception as exc:
            raise PlannerMaterializationError(
                code="PRM_RELEASE_CREATE_FAILED",
                message="Materialization failed.",
                planned_release_id=planned_release_id,
            ) from exc

    # compatibility shim for older endpoint path
    def materialize_or_get(self, *, planner_item_id: int, created_by: str | None) -> MaterializationResult:
        return self.materialize_planned_release(planned_release_id=planner_item_id, created_by=created_by)

    def _materialize_with_transaction(self, *, planned_release_id: int) -> MaterializationResult:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            out = self._materialize_core(planned_release_id=planned_release_id)
            self._conn.execute("COMMIT")
            return out
        except sqlite3.IntegrityError:
            self._conn.execute("ROLLBACK")
            return self._recover_after_concurrency_conflict(planned_release_id=planned_release_id)
        except PlannerMaterializationError:
            self._conn.execute("ROLLBACK")
            raise
        except MaterializationBindingError as exc:
            self._conn.execute("ROLLBACK")
            raise PlannerMaterializationError(
                code=exc.code,
                message=exc.message,
                planned_release_id=planned_release_id,
            ) from exc
        except Exception as exc:
            self._conn.execute("ROLLBACK")
            raise PlannerMaterializationError(
                code="PRM_RELEASE_CREATE_FAILED",
                message="Materialization failed.",
                planned_release_id=planned_release_id,
            ) from exc

    def _materialize_core(self, *, planned_release_id: int) -> MaterializationResult:
        planned_release = get_planned_release_by_id(self._conn, planned_release_id=planned_release_id)
        if planned_release is None:
            raise PlannerMaterializationError(
                code="PRM_NOT_FOUND",
                message="Planned release not found.",
                planned_release_id=planned_release_id,
            )

        readiness_status = self._readiness_status(planned_release_id=planned_release_id)
        if readiness_status == "NOT_READY":
            self._log_event(
                "planner.materialization.not_ready",
                planned_release_id=planned_release_id,
                readiness_status=readiness_status,
                result="FAILED",
                error_code="PRM_NOT_READY",
                transaction_path="fail",
            )
            raise PlannerMaterializationError(
                code="PRM_NOT_READY",
                message="Planned release is not ready for materialization.",
                details={"readiness_status": readiness_status},
                planned_release_id=planned_release_id,
            )
        if readiness_status == "BLOCKED":
            self._log_event(
                "planner.materialization.blocked",
                planned_release_id=planned_release_id,
                readiness_status=readiness_status,
                result="FAILED",
                error_code="PRM_BLOCKED",
                transaction_path="fail",
            )
            raise PlannerMaterializationError(
                code="PRM_BLOCKED",
                message="Planned release is blocked and cannot be materialized.",
                details={"readiness_status": readiness_status},
                planned_release_id=planned_release_id,
            )

        invariant_result = validate_binding_invariants(self._conn, planned_release=planned_release)
        action_enabled = readiness_status == "READY_FOR_MATERIALIZATION"
        summary = derive_materialization_state_summary_inputs(
            planned_release=planned_release,
            invariant_result=invariant_result,
            action_enabled=action_enabled,
        )
        diagnostics = derive_binding_diagnostics_inputs(
            planned_release=planned_release,
            invariant_result=invariant_result,
        )

        if invariant_result.invariant_status != "OK":
            self._log_event(
                "planner.materialization.binding_inconsistent",
                planned_release_id=planned_release_id,
                readiness_status=readiness_status,
                result="FAILED",
                error_code="PRM_BINDING_INCONSISTENT",
                transaction_path="fail",
                release_id=planned_release.get("materialized_release_id"),
            )
            raise PlannerMaterializationError(
                code="PRM_BINDING_INCONSISTENT",
                message="Materialization binding is inconsistent and cannot be auto-healed.",
                details={},
                planned_release_id=planned_release_id,
                materialization_state_summary=summary,
                binding_diagnostics=diagnostics,
            )

        materialized_release_id = planned_release.get("materialized_release_id")
        if materialized_release_id is not None:
            release = get_bound_release_for_planned_release(self._conn, planned_release_id=planned_release_id)
            if release is None:
                raise PlannerMaterializationError(
                    code="PRM_BINDING_INCONSISTENT",
                    message="Materialization binding is inconsistent and cannot be auto-healed.",
                    details={},
                    planned_release_id=planned_release_id,
                    materialization_state_summary=summary,
                    binding_diagnostics=diagnostics,
                )
            self._log_event(
                "planner.materialization.returned_existing",
                planned_release_id=planned_release_id,
                readiness_status=readiness_status,
                result="RETURNED_EXISTING",
                transaction_path="select_existing",
                release_id=int(release["id"]),
            )
            return self._build_result(
                planned_release_id=planned_release_id,
                result="RETURNED_EXISTING",
                release=release,
                summary=summary,
                diagnostics=diagnostics,
            )

        release = self._create_release_from_planned_release(planned_release=planned_release)
        release_id = int(release["id"])
        set_materialized_release_id(
            self._conn,
            planned_release_id=planned_release_id,
            materialized_release_id=release_id,
        )

        planned_after = get_planned_release_by_id(self._conn, planned_release_id=planned_release_id)
        if planned_after is None:
            raise PlannerMaterializationError(
                code="PRM_INVALID_PLANNED_RELEASE_STATE",
                message="Planned release not found after materialization update.",
                planned_release_id=planned_release_id,
            )
        invariant_after = validate_binding_invariants(self._conn, planned_release=planned_after)
        if invariant_after.invariant_status != "OK":
            raise PlannerMaterializationError(
                code="PRM_BINDING_INCONSISTENT",
                message="Materialization binding is inconsistent and cannot be auto-healed.",
                details={},
                planned_release_id=planned_release_id,
                materialization_state_summary=derive_materialization_state_summary_inputs(
                    planned_release=planned_after,
                    invariant_result=invariant_after,
                    action_enabled=action_enabled,
                ),
                binding_diagnostics=derive_binding_diagnostics_inputs(
                    planned_release=planned_after,
                    invariant_result=invariant_after,
                ),
            )

        release_after = get_bound_release_for_planned_release(self._conn, planned_release_id=planned_release_id)
        if release_after is None:
            raise PlannerMaterializationError(
                code="PRM_BINDING_INCONSISTENT",
                message="Materialization binding is inconsistent and cannot be auto-healed.",
                details={},
                planned_release_id=planned_release_id,
            )

        summary_after = derive_materialization_state_summary_inputs(
            planned_release=planned_after,
            invariant_result=invariant_after,
            action_enabled=action_enabled,
        )
        diagnostics_after = derive_binding_diagnostics_inputs(
            planned_release=planned_after,
            invariant_result=invariant_after,
        )

        self._log_event(
            "planner.materialization.created_new",
            planned_release_id=planned_release_id,
            readiness_status=readiness_status,
            result="CREATED_NEW",
            transaction_path="create_new",
            release_id=release_id,
        )
        return self._build_result(
            planned_release_id=planned_release_id,
            result="CREATED_NEW",
            release=release_after,
            summary=summary_after,
            diagnostics=diagnostics_after,
        )

    def _recover_after_concurrency_conflict(self, *, planned_release_id: int) -> MaterializationResult:
        planned_release: dict[str, Any] | None = None
        invariant_result: BindingInvariantResult = BindingInvariantResult(
            invariant_status="INCONSISTENT",
            invariant_reason="CONCURRENCY_CONFLICT_UNRESOLVED",
            linked_release_exists=False,
        )
        for _ in range(5):
            planned_release = get_planned_release_by_id(self._conn, planned_release_id=planned_release_id)
            if planned_release is None:
                time.sleep(0.02)
                continue

            invariant_result = validate_binding_invariants(self._conn, planned_release=planned_release)
            if invariant_result.invariant_status == "OK" and planned_release.get("materialized_release_id") is not None:
                release = get_bound_release_for_planned_release(self._conn, planned_release_id=planned_release_id)
                if release is not None:
                    summary = derive_materialization_state_summary_inputs(
                        planned_release=planned_release,
                        invariant_result=invariant_result,
                        action_enabled=True,
                    )
                    diagnostics = derive_binding_diagnostics_inputs(
                        planned_release=planned_release,
                        invariant_result=invariant_result,
                    )
                    self._log_event(
                        "planner.materialization.returned_existing",
                        planned_release_id=planned_release_id,
                        readiness_status=None,
                        result="RETURNED_EXISTING",
                        transaction_path="select_existing",
                        release_id=int(release["id"]),
                    )
                    return self._build_result(
                        planned_release_id=planned_release_id,
                        result="RETURNED_EXISTING",
                        release=release,
                        summary=summary,
                        diagnostics=diagnostics,
                    )
            time.sleep(0.02)

        self._log_event(
            "planner.materialization.failed",
            planned_release_id=planned_release_id,
            readiness_status=None,
            result="FAILED",
            error_code="PRM_CONCURRENCY_CONFLICT",
            transaction_path="fail",
            release_id=planned_release.get("materialized_release_id") if planned_release else None,
        )
        raise PlannerMaterializationError(
            code="PRM_CONCURRENCY_CONFLICT",
            message="Concurrency conflict could not be resolved.",
            planned_release_id=planned_release_id,
            materialization_state_summary=derive_materialization_state_summary_inputs(
                planned_release=planned_release,
                invariant_result=invariant_result,
                action_enabled=True,
            ),
            binding_diagnostics=derive_binding_diagnostics_inputs(
                planned_release=planned_release,
                invariant_result=invariant_result,
            ),
        )

    def _create_release_from_planned_release(self, *, planned_release: dict[str, Any]) -> dict[str, Any]:
        payload = build_release_payload_from_planned_release(self._conn, planned_release=planned_release)
        title = str(payload.get("title") or "").strip() or f"Planned release #{int(planned_release['id'])}"
        description = ""
        tags_json = "[]"
        planned_at = payload.get("planned_at")
        origin_meta_file_id = f"planned-release-{int(planned_release['id'])}"
        created_at = datetime.now(timezone.utc).timestamp()

        try:
            cur = self._conn.execute(
                """
                INSERT INTO releases(
                    channel_id,
                    title,
                    description,
                    tags_json,
                    planned_at,
                    origin_release_folder_id,
                    origin_meta_file_id,
                    created_at
                )
                VALUES(?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    int(payload["channel_id"]),
                    title,
                    description,
                    tags_json,
                    planned_at,
                    origin_meta_file_id,
                    created_at,
                ),
            )
        except sqlite3.IntegrityError:
            raise
        except Exception as exc:
            raise PlannerMaterializationError(
                code="PRM_RELEASE_CREATE_FAILED",
                message="Release creation failed.",
                planned_release_id=int(planned_release["id"]),
            ) from exc

        release_id = int(cur.lastrowid)
        row = self._conn.execute(
            """
            SELECT r.id, c.slug AS channel_slug
            FROM releases r
            LEFT JOIN channels c ON c.id = r.channel_id
            WHERE r.id = ?
            """,
            (release_id,),
        ).fetchone()
        if row is None:
            raise PlannerMaterializationError(
                code="PRM_RELEASE_CREATE_FAILED",
                message="Release creation failed.",
                planned_release_id=int(planned_release["id"]),
            )
        return dict(row)

    def _readiness_status(self, *, planned_release_id: int) -> str:
        try:
            readiness = self._readiness.evaluate(planned_release_id=planned_release_id)
        except PlannedReleaseReadinessNotFoundError as exc:
            raise PlannerMaterializationError(
                code="PRM_NOT_FOUND",
                message="Planned release not found.",
                planned_release_id=planned_release_id,
            ) from exc
        return str(readiness.get("aggregate_status") or "NOT_READY")

    def _build_result(
        self,
        *,
        planned_release_id: int,
        result: str,
        release: dict[str, Any],
        summary: dict[str, Any],
        diagnostics: dict[str, Any],
    ) -> MaterializationResult:
        release_id = int(release["id"])
        return MaterializationResult(
            planned_release_id=planned_release_id,
            result=result,
            release_id=release_id,
            release_channel_slug=release.get("channel_slug"),
            materialized_binding={
                "planned_release_id": planned_release_id,
                "release_id": release_id,
            },
            materialization_state_summary=summary,
            binding_diagnostics=diagnostics,
        )

    def _log_event(
        self,
        event_name: str,
        *,
        planned_release_id: int,
        readiness_status: str | None,
        result: str,
        transaction_path: str,
        release_id: int | None = None,
        error_code: str | None = None,
    ) -> None:
        logger.info(
            "%s planned_release_id=%s release_id=%s readiness_status=%s result=%s error_code=%s transaction_path=%s",
            event_name,
            planned_release_id,
            release_id,
            readiness_status,
            result,
            error_code,
            transaction_path,
        )
