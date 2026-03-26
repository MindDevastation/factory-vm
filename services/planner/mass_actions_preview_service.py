from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import logging
import sqlite3
import uuid
from typing import Any

from services.common import db as dbm
from services.planner.materialization_foundation import (
    get_planned_release_by_id,
    validate_binding_invariants,
)
from services.planner.planned_release_readiness_service import PlannedReleaseReadinessService
from services.planner.release_job_creation_foundation import (
    ReleaseJobCreationFoundationError,
    derive_job_creation_state_summary_inputs,
    get_release_by_id,
    validate_open_job_invariants,
)

logger = logging.getLogger(__name__)

ACTION_MATERIALIZE = "BATCH_MATERIALIZE_SELECTED"
ACTION_CREATE_JOBS = "BATCH_CREATE_JOBS_FOR_SELECTED"
ALLOWED_ACTIONS = {ACTION_MATERIALIZE, ACTION_CREATE_JOBS}
MAX_SELECTED_ITEMS = 200

RESULT_CREATED = "SUCCESS_CREATED_NEW"
RESULT_EXISTING = "SUCCESS_RETURNED_EXISTING"
RESULT_SKIPPED = "SKIPPED_NON_EXECUTABLE"
RESULT_FAILED = "FAILED_INVALID_OR_INCONSISTENT"


class PlannerMassActionPreviewError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def create_mass_action_preview_session(
    conn: sqlite3.Connection,
    *,
    action_type: str,
    selected_item_ids: list[int],
    created_by: str | None,
    ttl_seconds: int,
) -> dict[str, Any]:
    normalized_action = _normalize_action_type(action_type)
    normalized_ids = _normalize_selected_ids(selected_item_ids)
    planned_by_id = _load_selected_planned_releases(conn, selected_item_ids=normalized_ids)

    items: list[dict[str, Any]] = []
    for planned_release_id in normalized_ids:
        planned_release = planned_by_id[planned_release_id]
        if normalized_action == ACTION_MATERIALIZE:
            item = _preview_materialization_item(conn, planned_release=planned_release)
        else:
            item = _preview_job_creation_item(conn, planned_release=planned_release)
        items.append(item)

    aggregate = _build_aggregate_summary(items=items, selected_count=len(normalized_ids))
    now = datetime.now(timezone.utc)
    created_at = now.isoformat()
    expires_at = (now + timedelta(seconds=max(1, ttl_seconds))).isoformat()
    session_id = uuid.uuid4().hex
    fingerprint = _build_scope_fingerprint(selected_item_ids=normalized_ids)

    dbm.insert_planner_mass_action_session(
        conn,
        session_id=session_id,
        action_type=normalized_action,
        planner_scope_fingerprint=fingerprint,
        selected_item_ids_json=dbm.json_dumps(normalized_ids),
        preview_status="OPEN",
        aggregate_preview_json=dbm.json_dumps(aggregate),
        item_preview_json=dbm.json_dumps(items),
        created_by=created_by,
        created_at=created_at,
        expires_at=expires_at,
        executed_at=None,
    )
    conn.commit()

    logger.info(
        "planner.mass_action.preview_created %s",
        {
            "session_id": session_id,
            "action_type": normalized_action,
            "selected_count": len(normalized_ids),
            "created_new_count": aggregate["would_create_new"],
            "returned_existing_count": aggregate["would_return_existing"],
            "skipped_count": aggregate["would_skip"],
            "failed_count": aggregate["would_fail"],
            "stale_session_flag": False,
            "error_codes": [],
        },
    )

    return {
        "preview_session_id": session_id,
        "action_type": normalized_action,
        "selected_count": len(normalized_ids),
        "aggregate": aggregate,
        "items": items,
        "created_at": created_at,
        "expires_at": expires_at,
    }


def get_mass_action_preview_session(conn: sqlite3.Connection, *, session_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM planner_mass_action_sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        raise PlannerMassActionPreviewError(code="PMA_SESSION_NOT_FOUND", message="Planner mass-action preview session not found")
    body = dict(row)
    return {
        "preview_session_id": str(body["id"]),
        "action_type": str(body["action_type"]),
        "selected_item_ids": dbm.json_loads(str(body["selected_item_ids_json"])),
        "preview_status": str(body["preview_status"]),
        "aggregate": dbm.json_loads(str(body["aggregate_preview_json"])),
        "items": dbm.json_loads(str(body["item_preview_json"])),
        "created_at": str(body["created_at"]),
        "expires_at": str(body["expires_at"]),
        "executed_at": body.get("executed_at"),
    }


def _normalize_action_type(action_type: str) -> str:
    value = str(action_type or "").strip()
    if value not in ALLOWED_ACTIONS:
        raise PlannerMassActionPreviewError(code="PMA_INVALID_ACTION_TYPE", message="Unsupported action_type")
    return value


def _normalize_selected_ids(selected_item_ids: list[int]) -> list[int]:
    if not isinstance(selected_item_ids, list) or any(not isinstance(item, int) for item in selected_item_ids):
        raise PlannerMassActionPreviewError(code="PMA_SELECTION_EMPTY", message="selected_item_ids must be an integer array")
    if not selected_item_ids:
        raise PlannerMassActionPreviewError(code="PMA_SELECTION_EMPTY", message="selected_item_ids must not be empty")
    if len(selected_item_ids) > MAX_SELECTED_ITEMS:
        raise PlannerMassActionPreviewError(code="PMA_SELECTION_TOO_LARGE", message="selected_item_ids exceeds max size of 200")
    return [int(item) for item in selected_item_ids]


def _load_selected_planned_releases(conn: sqlite3.Connection, *, selected_item_ids: list[int]) -> dict[int, dict[str, Any]]:
    placeholders = ",".join("?" for _ in selected_item_ids)
    rows = conn.execute(
        f"SELECT * FROM planned_releases WHERE id IN ({placeholders})",
        tuple(selected_item_ids),
    ).fetchall()
    by_id = {int(row["id"]): dict(row) for row in rows}
    missing = [item_id for item_id in selected_item_ids if item_id not in by_id]
    if missing:
        raise PlannerMassActionPreviewError(
            code="PMA_ITEM_NOT_FOUND_IN_SCOPE",
            message=f"Planner items not found in scope: {missing}",
        )
    return by_id


def _preview_materialization_item(conn: sqlite3.Connection, *, planned_release: dict[str, Any]) -> dict[str, Any]:
    planned_release_id = int(planned_release["id"])
    readiness_service = PlannedReleaseReadinessService(conn)
    readiness = readiness_service.evaluate(planned_release_id=planned_release_id)
    readiness_status = str(readiness.get("aggregate_status") or "")
    invariant_result = validate_binding_invariants(conn, planned_release=planned_release)

    if invariant_result.invariant_status != "OK":
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_FAILED,
            "expected_outcome": "Would fail due to inconsistent materialization binding",
            "reason": {
                "code": "PRM_BINDING_INCONSISTENT",
                "message": "Materialization binding is inconsistent.",
            },
        }

    if readiness_status == "NOT_READY":
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_SKIPPED,
            "expected_outcome": "Would not execute",
            "reason": {
                "code": "PMA_NOT_READY",
                "message": "Planned release is not READY_FOR_MATERIALIZATION.",
            },
        }

    if readiness_status == "BLOCKED":
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_SKIPPED,
            "expected_outcome": "Would not execute",
            "reason": {
                "code": "PMA_BLOCKED",
                "message": "Planned release is BLOCKED for materialization.",
            },
        }

    materialized_release_id = planned_release.get("materialized_release_id")
    if materialized_release_id is not None:
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_EXISTING,
            "expected_outcome": "Would return existing materialized release",
            "details": {
                "release_id": int(materialized_release_id),
            },
        }

    return {
        "planned_release_id": planned_release_id,
        "result_kind": RESULT_CREATED,
        "expected_outcome": "Would create new release",
        "details": {},
    }


def _preview_job_creation_item(conn: sqlite3.Connection, *, planned_release: dict[str, Any]) -> dict[str, Any]:
    planned_release_id = int(planned_release["id"])
    invariant_result = validate_binding_invariants(conn, planned_release=planned_release)
    if invariant_result.invariant_status != "OK":
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_FAILED,
            "expected_outcome": "Would fail due to inconsistent release binding",
            "reason": {
                "code": "PRM_BINDING_INCONSISTENT",
                "message": "Planned release binding is inconsistent for job creation.",
            },
        }

    materialized_release_id = planned_release.get("materialized_release_id")
    if materialized_release_id is None:
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_SKIPPED,
            "expected_outcome": "Would not execute",
            "reason": {
                "code": "PMA_RELEASE_NOT_MATERIALIZED",
                "message": "Planned release has no canonical materialized release.",
            },
        }

    release = get_release_by_id(conn, release_id=int(materialized_release_id))
    if release is None:
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_FAILED,
            "expected_outcome": "Would fail due to inconsistent release binding",
            "reason": {
                "code": "PRM_BINDING_INCONSISTENT",
                "message": "Materialized release binding points to a missing release.",
            },
        }

    try:
        diagnostics = validate_open_job_invariants(conn, release=release)
    except ReleaseJobCreationFoundationError as exc:
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_FAILED,
            "expected_outcome": "Would fail due to invalid or inconsistent job state",
            "reason": {
                "code": exc.code,
                "message": exc.message,
            },
        }

    summary = derive_job_creation_state_summary_inputs(
        release=release,
        diagnostics=diagnostics,
        action_enabled=True,
    )
    state = str(summary.get("job_creation_state") or "")
    if state == "HAS_OPEN_JOB":
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_EXISTING,
            "expected_outcome": "Would return existing open job",
            "details": {
                "job_id": release.get("current_open_job_id"),
                "release_id": int(release["id"]),
            },
        }
    if state == "NO_OPEN_JOB":
        return {
            "planned_release_id": planned_release_id,
            "result_kind": RESULT_CREATED,
            "expected_outcome": "Would create new open job",
            "details": {
                "release_id": int(release["id"]),
            },
        }

    return {
        "planned_release_id": planned_release_id,
        "result_kind": RESULT_FAILED,
        "expected_outcome": "Would fail due to invalid or inconsistent job state",
        "reason": {
            "code": "PMA_CANONICAL_FLOW_FAILED",
            "message": "Could not classify job creation preview outcome.",
        },
    }


def _build_aggregate_summary(*, items: list[dict[str, Any]], selected_count: int) -> dict[str, int]:
    would_create_new = sum(1 for item in items if item.get("result_kind") == RESULT_CREATED)
    would_return_existing = sum(1 for item in items if item.get("result_kind") == RESULT_EXISTING)
    would_skip = sum(1 for item in items if item.get("result_kind") == RESULT_SKIPPED)
    would_fail = sum(1 for item in items if item.get("result_kind") == RESULT_FAILED)
    would_succeed = would_create_new + would_return_existing
    return {
        "total_selected": int(selected_count),
        "eligible": int(would_succeed),
        "would_succeed": int(would_succeed),
        "would_fail": int(would_fail),
        "would_skip": int(would_skip),
        "would_create_new": int(would_create_new),
        "would_return_existing": int(would_return_existing),
        "blocked_or_prereq_problem": int(would_skip + would_fail),
    }


def _build_scope_fingerprint(*, selected_item_ids: list[int]) -> str:
    raw = "planner_mass_action_selection|" + ",".join(str(item_id) for item_id in selected_item_ids)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"selection:{digest}"
