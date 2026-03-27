from __future__ import annotations

from datetime import datetime, timezone
import logging
import sqlite3
from typing import Any

from services.common import db as dbm
from services.planner.mass_actions_preview_service import ACTION_CREATE_JOBS, ACTION_MATERIALIZE
from services.planner.materialization_foundation import get_planned_release_by_id
from services.planner.materialization_service import PlannerMaterializationError, PlannerMaterializationService
from services.planner.release_job_creation_service import ReleaseJobCreationError, ReleaseJobCreationService

logger = logging.getLogger(__name__)

RESULT_CREATED = "SUCCESS_CREATED_NEW"
RESULT_EXISTING = "SUCCESS_RETURNED_EXISTING"
RESULT_SKIPPED = "SKIPPED_NON_EXECUTABLE"
RESULT_FAILED = "FAILED_INVALID_OR_INCONSISTENT"


class PlannerMassActionExecuteError(Exception):
    def __init__(self, *, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


def execute_mass_action_session(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    selected_item_ids: list[int] | None,
    executed_by: str | None,
) -> dict[str, Any]:
    del executed_by
    session = _load_session(conn, session_id=session_id)
    action_type = str(session["action_type"])
    preview_selected_ids = dbm.json_loads(str(session["selected_item_ids_json"]))
    preview_status = str(session["preview_status"])
    expires_at = str(session["expires_at"])

    execute_selected_ids = _resolve_execute_selected_ids(
        selected_item_ids=selected_item_ids,
        preview_selected_ids=preview_selected_ids,
    )

    _validate_session_freshness(
        conn,
        session_id=session_id,
        preview_status=preview_status,
        expires_at=expires_at,
    )

    logger.info(
        "planner.mass_action.execute_started %s",
        {
            "session_id": session_id,
            "action_type": action_type,
            "selected_count": len(execute_selected_ids),
            "executed_count": 0,
            "succeeded_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "created_new_count": 0,
            "returned_existing_count": 0,
            "stale_session_flag": False,
            "error_codes": [],
        },
    )

    items: list[dict[str, Any]] = []
    error_codes: list[str] = []
    for planned_release_id in execute_selected_ids:
        if action_type == ACTION_MATERIALIZE:
            item = _execute_materialize_item(conn, planned_release_id=planned_release_id)
        elif action_type == ACTION_CREATE_JOBS:
            item = _execute_create_job_item(conn, planned_release_id=planned_release_id)
        else:
            raise PlannerMassActionExecuteError(
                code="PMA_SELECTION_SCOPE_MISMATCH",
                message="Unsupported action type for execute session.",
                details={"session_id": session_id, "action_type": action_type},
            )
        if item.get("reason") and isinstance(item["reason"], dict) and item["reason"].get("code"):
            error_codes.append(str(item["reason"]["code"]))
        items.append(item)

    summary = _build_summary(items=items)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE planner_mass_action_sessions SET preview_status = ?, executed_at = ? WHERE id = ?",
        ("EXECUTED", now, session_id),
    )
    conn.commit()

    event_name = "planner.mass_action.execute_completed"
    if summary["succeeded"] > 0 and (summary["failed"] > 0 or summary["skipped"] > 0):
        event_name = "planner.mass_action.execute_partial_success"

    logger.info(
        f"{event_name} %s",
        {
            "session_id": session_id,
            "action_type": action_type,
            "selected_count": len(execute_selected_ids),
            "executed_count": len(items),
            "succeeded_count": summary["succeeded"],
            "failed_count": summary["failed"],
            "skipped_count": summary["skipped"],
            "created_new_count": summary["created_new_entities"],
            "returned_existing_count": summary["returned_existing_entities"],
            "stale_session_flag": False,
            "error_codes": sorted(set(error_codes)),
        },
    )

    return {
        "session_id": session_id,
        "action_type": action_type,
        "preview_status": "EXECUTED",
        "summary": summary,
        "items": items,
        "executed_at": now,
    }


def _load_session(conn: sqlite3.Connection, *, session_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM planner_mass_action_sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        raise PlannerMassActionExecuteError(
            code="PMA_SESSION_NOT_FOUND",
            message="Planner mass-action preview session not found.",
        )
    return dict(row)


def _resolve_execute_selected_ids(*, selected_item_ids: list[int] | None, preview_selected_ids: list[int]) -> list[int]:
    if selected_item_ids is None:
        return [int(item_id) for item_id in preview_selected_ids]
    preview_set = {int(item_id) for item_id in preview_selected_ids}
    normalized = [int(item_id) for item_id in selected_item_ids]
    if any(item_id not in preview_set for item_id in normalized):
        raise PlannerMassActionExecuteError(
            code="PMA_EXECUTE_SUBSET_INVALID",
            message="Execute selected_item_ids must be a subset of preview selected items.",
        )
    return normalized


def _validate_session_freshness(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    preview_status: str,
    expires_at: str,
) -> None:
    expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    if now > expires_dt:
        conn.execute(
            "UPDATE planner_mass_action_sessions SET preview_status = ? WHERE id = ? AND preview_status = ?",
            ("EXPIRED", session_id, "OPEN"),
        )
        conn.commit()
        raise PlannerMassActionExecuteError(
            code="PMA_SESSION_EXPIRED",
            message="Planner mass-action session has expired.",
            details={
                "session_id": session_id,
                "preview_status": "EXPIRED",
                "expires_at": expires_at,
            },
        )

    if preview_status == "INVALIDATED":
        raise PlannerMassActionExecuteError(
            code="PMA_SESSION_INVALIDATED",
            message="Planner mass-action session was invalidated.",
            details={
                "session_id": session_id,
                "preview_status": preview_status,
                "expires_at": expires_at,
            },
        )

    if preview_status != "OPEN":
        raise PlannerMassActionExecuteError(
            code="PMA_SELECTION_SCOPE_MISMATCH",
            message="Planner mass-action session is stale and cannot be executed.",
            details={
                "session_id": session_id,
                "preview_status": preview_status,
                "expires_at": expires_at,
            },
        )


def _execute_materialize_item(conn: sqlite3.Connection, *, planned_release_id: int) -> dict[str, Any]:
    planned_release = get_planned_release_by_id(conn, planned_release_id=planned_release_id)
    if planned_release is None:
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_FAILED,
            "reason": {
                "code": "PMA_ITEM_NOT_FOUND_IN_SCOPE",
                "message": "Planned release item is no longer available in scope.",
            },
        }

    svc = PlannerMaterializationService(conn)
    try:
        out = svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="mass-action")
    except PlannerMaterializationError as exc:
        if exc.code in {"PRM_NOT_READY", "PRM_BLOCKED"}:
            return {
                "planned_release_id": planned_release_id,
                "result_kind": RESULT_SKIPPED,
                "reason": {"code": exc.code, "message": exc.message},
            }
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_FAILED,
            "reason": {"code": exc.code or "PMA_CANONICAL_FLOW_FAILED", "message": exc.message},
        }

    if out.result == "CREATED_NEW":
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_CREATED,
            "entity_type": "release",
            "release_id": out.release_id,
            "message": "Created new materialized release.",
        }

    return {
        "planned_release_id": planned_release_id,
        "result_kind": RESULT_EXISTING,
        "entity_type": "release",
        "release_id": out.release_id,
        "message": "Returned existing materialized release.",
    }


def _execute_create_job_item(conn: sqlite3.Connection, *, planned_release_id: int) -> dict[str, Any]:
    planned_release = get_planned_release_by_id(conn, planned_release_id=planned_release_id)
    if planned_release is None:
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_FAILED,
            "reason": {
                "code": "PMA_ITEM_NOT_FOUND_IN_SCOPE",
                "message": "Planned release item is no longer available in scope.",
            },
        }

    materialized_release_id = planned_release.get("materialized_release_id")
    if materialized_release_id is None:
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_SKIPPED,
            "reason": {
                "code": "PMA_RELEASE_NOT_MATERIALIZED",
                "message": "Planned release has no canonical materialized release.",
            },
        }

    svc = ReleaseJobCreationService(conn)
    try:
        out = svc.create_or_select(release_id=int(materialized_release_id))
    except ReleaseJobCreationError as exc:
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_FAILED,
            "reason": {"code": exc.code or "PMA_CANONICAL_FLOW_FAILED", "message": exc.message},
        }

    if out.result == "CREATED_NEW_JOB":
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_CREATED,
            "entity_type": "job",
            "job_id": int(out.job["id"]),
            "message": "Created new DRAFT job.",
        }

    return {
        "planned_release_id": planned_release_id,
        "result_kind": RESULT_EXISTING,
        "entity_type": "job",
        "job_id": int(out.job["id"]),
        "message": "Returned existing open job.",
    }


def _build_summary(*, items: list[dict[str, Any]]) -> dict[str, int]:
    succeeded = 0
    failed = 0
    skipped = 0
    created_new = 0
    returned_existing = 0
    for item in items:
        result_kind = str(item.get("result_kind") or "")
        if result_kind == RESULT_CREATED:
            succeeded += 1
            created_new += 1
        elif result_kind == RESULT_EXISTING:
            succeeded += 1
            returned_existing += 1
        elif result_kind == RESULT_SKIPPED:
            skipped += 1
        else:
            failed += 1
    return {
        "total_selected": len(items),
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "created_new_entities": created_new,
        "returned_existing_entities": returned_existing,
    }
