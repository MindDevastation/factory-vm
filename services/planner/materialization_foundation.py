from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


class MaterializationBindingError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class BindingInvariantResult:
    invariant_status: str
    invariant_reason: str | None
    linked_release_exists: bool


def get_planned_release_by_id(conn: sqlite3.Connection, *, planned_release_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM planned_releases WHERE id = ?", (planned_release_id,)).fetchone()
    return dict(row) if row is not None else None


def get_bound_release_for_planned_release(conn: sqlite3.Connection, *, planned_release_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT r.*
        FROM planned_releases pr
        JOIN releases r ON r.id = pr.materialized_release_id
        WHERE pr.id = ?
        """,
        (planned_release_id,),
    ).fetchone()
    return dict(row) if row is not None else None


def set_materialized_release_id(
    conn: sqlite3.Connection,
    *,
    planned_release_id: int,
    materialized_release_id: int | None,
) -> None:
    cur = conn.execute(
        "UPDATE planned_releases SET materialized_release_id = ?, updated_at = updated_at WHERE id = ?",
        (materialized_release_id, planned_release_id),
    )
    if int(cur.rowcount or 0) != 1:
        raise MaterializationBindingError(
            code="PRM_INVALID_PLANNED_RELEASE_STATE",
            message="planned_release not found for binding update",
        )


def find_planned_release_by_materialized_release_id(
    conn: sqlite3.Connection,
    *,
    materialized_release_id: int,
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM planned_releases WHERE materialized_release_id = ? LIMIT 1",
        (materialized_release_id,),
    ).fetchone()
    return dict(row) if row is not None else None


def validate_binding_invariants(
    conn: sqlite3.Connection,
    *,
    planned_release: dict[str, Any],
) -> BindingInvariantResult:
    planned_release_id = planned_release.get("id")
    if not isinstance(planned_release_id, int):
        _log_binding_violation(
            planned_release_id=planned_release_id,
            materialized_release_id=planned_release.get("materialized_release_id"),
            release_id=None,
            error_code="PRM_INVALID_PLANNED_RELEASE_STATE",
            reason="planned_release.id missing or invalid",
        )
        raise MaterializationBindingError(
            code="PRM_INVALID_PLANNED_RELEASE_STATE",
            message="planned_release.id missing or invalid",
        )

    materialized_release_id = planned_release.get("materialized_release_id")
    if materialized_release_id is None:
        legacy_link = conn.execute(
            "SELECT release_id FROM planner_release_links WHERE planned_release_id = ?",
            (planned_release_id,),
        ).fetchone()
        if legacy_link is not None:
            _log_binding_violation(
                planned_release_id=planned_release_id,
                materialized_release_id=None,
                release_id=int(legacy_link["release_id"]),
                error_code="PRM_BINDING_INCONSISTENT",
                reason="legacy link exists without canonical binding",
            )
            return BindingInvariantResult(
                invariant_status="INCONSISTENT",
                invariant_reason="LEGACY_LINK_WITHOUT_CANONICAL_BINDING",
                linked_release_exists=True,
            )
        return BindingInvariantResult(
            invariant_status="OK",
            invariant_reason=None,
            linked_release_exists=False,
        )

    release = conn.execute("SELECT id FROM releases WHERE id = ?", (materialized_release_id,)).fetchone()
    if release is None:
        _log_binding_violation(
            planned_release_id=planned_release_id,
            materialized_release_id=materialized_release_id,
            release_id=materialized_release_id,
            error_code="PRM_BINDING_INCONSISTENT",
            reason="materialized_release_id points to missing release",
        )
        return BindingInvariantResult(
            invariant_status="INCONSISTENT",
            invariant_reason="MATERIALIZED_RELEASE_MISSING",
            linked_release_exists=False,
        )

    duplicate = conn.execute(
        """
        SELECT id
        FROM planned_releases
        WHERE materialized_release_id = ?
          AND id != ?
        LIMIT 1
        """,
        (materialized_release_id, planned_release_id),
    ).fetchone()
    if duplicate is not None:
        _log_binding_violation(
            planned_release_id=planned_release_id,
            materialized_release_id=materialized_release_id,
            release_id=materialized_release_id,
            error_code="PRM_BINDING_INCONSISTENT",
            reason="release already bound to another planned_release",
        )
        return BindingInvariantResult(
            invariant_status="INCONSISTENT",
            invariant_reason="RELEASE_BOUND_TO_ANOTHER_PLANNED_RELEASE",
            linked_release_exists=True,
        )

    legacy_link = conn.execute(
        "SELECT release_id FROM planner_release_links WHERE planned_release_id = ?",
        (planned_release_id,),
    ).fetchone()
    if legacy_link is not None and int(legacy_link["release_id"]) != int(materialized_release_id):
        _log_binding_violation(
            planned_release_id=planned_release_id,
            materialized_release_id=materialized_release_id,
            release_id=int(legacy_link["release_id"]),
            error_code="PRM_BINDING_INCONSISTENT",
            reason="contradictory linkage state",
        )
        return BindingInvariantResult(
            invariant_status="INCONSISTENT",
            invariant_reason="CONTRADICTORY_LINKAGE_STATE",
            linked_release_exists=True,
        )

    return BindingInvariantResult(
        invariant_status="OK",
        invariant_reason=None,
        linked_release_exists=True,
    )


def build_release_payload_from_planned_release(
    conn: sqlite3.Connection,
    *,
    planned_release: dict[str, Any],
) -> dict[str, Any]:
    channel_slug = planned_release.get("channel_slug")
    if not isinstance(channel_slug, str) or not channel_slug.strip():
        raise MaterializationBindingError(
            code="PRM_INVALID_PLANNED_RELEASE_STATE",
            message="planned_release.channel_slug missing or invalid",
        )
    channel = conn.execute("SELECT id FROM channels WHERE slug = ?", (channel_slug.strip(),)).fetchone()
    if channel is None:
        raise MaterializationBindingError(
            code="PRM_RELEASE_CREATE_FAILED",
            message="channel_id could not be resolved from planned_release.channel_slug",
        )

    payload: dict[str, Any] = {
        "channel_id": int(channel["id"]),
        "planned_at": planned_release.get("publish_at"),
    }
    title = planned_release.get("title")
    if isinstance(title, str) and title.strip():
        payload["title"] = title.strip()
    return payload


def derive_materialization_state_summary_inputs(
    *,
    planned_release: dict[str, Any],
    invariant_result: BindingInvariantResult,
    action_enabled: bool,
) -> dict[str, Any]:
    materialized_release_id = planned_release.get("materialized_release_id")
    if invariant_result.invariant_status != "OK":
        state = "BINDING_INCONSISTENT"
    elif not action_enabled:
        state = "ACTION_DISABLED"
    elif materialized_release_id is None:
        state = "NOT_MATERIALIZED"
    else:
        state = "ALREADY_MATERIALIZED"
    return {
        "planned_release_id": planned_release.get("id"),
        "materialized_release_id": materialized_release_id,
        "materialization_state": state,
        "invariant_status": invariant_result.invariant_status,
        "invariant_reason": invariant_result.invariant_reason,
    }


def derive_binding_diagnostics_inputs(
    *,
    planned_release: dict[str, Any],
    invariant_result: BindingInvariantResult,
) -> dict[str, Any]:
    return {
        "planned_release_id": planned_release.get("id"),
        "materialized_release_id": planned_release.get("materialized_release_id"),
        "linked_release_exists": invariant_result.linked_release_exists,
        "invariant_status": invariant_result.invariant_status,
        "invariant_reason": invariant_result.invariant_reason,
    }


def _log_binding_violation(
    *,
    planned_release_id: Any,
    materialized_release_id: Any,
    release_id: Any,
    error_code: str,
    reason: str,
) -> None:
    logger.warning(
        "planner.materialization.binding_inconsistent planned_release_id=%s materialized_release_id=%s release_id=%s error_code=%s transaction_path=fail reason=%s",
        planned_release_id,
        materialized_release_id,
        release_id,
        error_code,
        reason,
    )
