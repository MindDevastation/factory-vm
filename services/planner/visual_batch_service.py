from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import secrets
import sqlite3
from typing import Any

from services.planner import background_assignment_service
from services.planner import cover_assignment_service

ALLOWED_ACTION_TYPES = {
    "BULK_ASSIGN_BACKGROUND",
    "BULK_GENERATE_PREVIEWS",
    "BULK_APPROVE_APPLY",
    "BULK_RERUN_ASSISTED",
}


class VisualBatchError(Exception):
    def __init__(self, *, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


def create_visual_batch_preview_session(
    conn: sqlite3.Connection,
    *,
    action_type: str,
    selected_release_ids: list[int],
    created_by: str | None,
    action_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if action_type not in ALLOWED_ACTION_TYPES:
        raise VisualBatchError(code="VBATCH_ACTION_INVALID", message="unsupported visual batch action")
    release_ids = sorted({int(item) for item in selected_release_ids if int(item) > 0})
    if not release_ids:
        raise VisualBatchError(code="VBATCH_SCOPE_EMPTY", message="selected_release_ids must not be empty")

    rows = _load_release_rows(conn, release_ids)
    scope_fingerprint = _scope_fingerprint(action_type=action_type, release_ids=release_ids)
    payload = dict(action_payload or {})
    selected_background_asset_id = _as_optional_positive_int(payload.get("background_asset_id"))
    per_item: list[dict[str, Any]] = []
    warnings_total = 0
    preview_ready_total = 0
    for row in rows:
        release_id = int(row["id"])
        applied = conn.execute(
            "SELECT background_asset_id, cover_asset_id, applied_at FROM release_visual_applied_packages WHERE release_id = ?",
            (release_id,),
        ).fetchone()
        item_warning_codes: list[str] = []
        if applied:
            item_warning_codes.append("OVERWRITE_REQUIRES_EXPLICIT_DECISION")
        warnings_total += len(item_warning_codes)
        item_entry: dict[str, Any] = {
            "release_id": release_id,
            "channel_id": int(row["channel_id"]),
            "status": "READY",
            "warning_codes": item_warning_codes,
            "applied_package_exists": bool(applied),
        }
        if action_type in {"BULK_ASSIGN_BACKGROUND", "BULK_GENERATE_PREVIEWS", "BULK_RERUN_ASSISTED"}:
            try:
                preview = background_assignment_service.preview_background_assignment(
                    conn,
                    release_id=release_id,
                    background_asset_id=selected_background_asset_id,
                    source_family=None,
                    source_reference=None,
                    template_assisted=(action_type == "BULK_RERUN_ASSISTED"),
                    selected_by=created_by,
                )
                preview_ready_total += 1
                item_entry["preview_id"] = preview["preview_id"]
                item_entry["preview_selection"] = preview["selection"]
            except Exception as preview_exc:
                item_entry["status"] = "BLOCKED"
                item_entry["reason_code"] = getattr(preview_exc, "code", "VBATCH_PREVIEW_FAILED")
                item_entry["reason_detail"] = str(preview_exc)
        per_item.append(
            item_entry
        )

    created_at = _now_iso()
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=15)).replace(microsecond=0).isoformat()
    session_id = f"vbatch-{secrets.token_hex(10)}"
    aggregate = {
        "action_type": action_type,
        "background_asset_id": selected_background_asset_id,
        "scope_total": len(release_ids),
        "warnings_total": warnings_total,
        "ready_total": sum(1 for item in per_item if str(item["status"]) == "READY"),
        "preview_ready_total": preview_ready_total,
    }
    conn.execute(
        """
        INSERT INTO release_visual_batch_preview_sessions(
            id, action_type, selected_release_ids_json, scope_fingerprint, session_status,
            aggregate_preview_json, per_item_preview_json, invalidation_reason_code,
            created_by, created_at, expires_at, executed_at
        ) VALUES(?, ?, ?, ?, 'OPEN', ?, ?, NULL, ?, ?, ?, NULL)
        """,
        (
            session_id,
            action_type,
            json.dumps(release_ids),
            scope_fingerprint,
            json.dumps(aggregate, sort_keys=True),
            json.dumps(per_item, sort_keys=True),
            created_by,
            created_at,
            expires_at,
        ),
    )
    return {
        "preview_session_id": session_id,
        "action_type": action_type,
        "scope_fingerprint": scope_fingerprint,
        "aggregate": aggregate,
        "items": per_item,
        "expires_at": expires_at,
    }


def execute_visual_batch_preview_session(
    conn: sqlite3.Connection,
    *,
    preview_session_id: str,
    selected_release_ids: list[int],
    overwrite_confirmed: bool,
    reuse_override_confirmed: bool,
    executed_by: str | None,
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM release_visual_batch_preview_sessions WHERE id = ?",
        (preview_session_id,),
    ).fetchone()
    if not row:
        raise VisualBatchError(code="VBATCH_SESSION_NOT_FOUND", message="batch preview session not found")

    now = datetime.now(timezone.utc)
    expires_at = datetime.fromisoformat(str(row["expires_at"]))
    if now > expires_at:
        conn.execute(
            "UPDATE release_visual_batch_preview_sessions SET session_status = 'EXPIRED', invalidation_reason_code = 'PREVIEW_SESSION_EXPIRED' WHERE id = ?",
            (preview_session_id,),
        )
        raise VisualBatchError(
            code="VBATCH_PREVIEW_SESSION_EXPIRED",
            message="preview session expired",
            details={
                "invalidation_reason_code": "PREVIEW_SESSION_EXPIRED",
                "preview_session_id": preview_session_id,
                "expires_at": str(row["expires_at"]),
            },
        )

    if str(row["session_status"]) != "OPEN":
        raise VisualBatchError(code="VBATCH_PREVIEW_NOT_OPEN", message="preview session is not open for execute")

    preview_scope = sorted(json.loads(str(row["selected_release_ids_json"]) or "[]"))
    requested_scope = sorted({int(item) for item in selected_release_ids if int(item) > 0})
    if requested_scope != preview_scope:
        conn.execute(
            "UPDATE release_visual_batch_preview_sessions SET invalidation_reason_code = 'PREVIEW_SCOPE_INVALIDATED' WHERE id = ?",
            (preview_session_id,),
        )
        raise VisualBatchError(
            code="VBATCH_PREVIEW_SCOPE_INVALIDATED",
            message="execute scope does not match preview scope",
            details={
                "invalidation_reason_code": "PREVIEW_SCOPE_INVALIDATED",
                "preview_scope": preview_scope,
                "requested_scope": requested_scope,
            },
        )

    action_type = str(row["action_type"])
    results: list[dict[str, Any]] = []
    executed_count = 0
    blocked_count = 0
    warning_count = 0
    session_payload = json.loads(str(row["aggregate_preview_json"]) or "{}")
    selected_background_asset_id = _as_optional_positive_int(session_payload.get("background_asset_id"))
    for release_id in requested_scope:
        applied = conn.execute(
            "SELECT 1 FROM release_visual_applied_packages WHERE release_id = ?",
            (release_id,),
        ).fetchone()
        if applied and not overwrite_confirmed:
            blocked_count += 1
            warning_count += 1
            results.append(
                {
                    "release_id": release_id,
                    "status": "BLOCKED",
                    "reason_code": "OVERWRITE_REQUIRES_EXPLICIT_DECISION",
                    "reason_detail": "release already has applied package",
                    "overwrite_requires_confirmation": True,
                }
            )
            continue

        if action_type == "BULK_APPROVE_APPLY":
            try:
                output = cover_assignment_service.apply_cover_candidate(
                    conn,
                    release_id=release_id,
                    applied_by=executed_by,
                    reuse_override_confirmed=reuse_override_confirmed,
                )
                executed_count += 1
                results.append(
                    {
                        "release_id": release_id,
                        "status": "APPLIED",
                        "applied_preview_id": output["preview_id"],
                    }
                )
            except Exception as cover_exc:
                if getattr(cover_exc, "code", "") == "VCOVER_APPROVAL_REQUIRED":
                    try:
                        output = background_assignment_service.apply_background_assignment(
                            conn,
                            release_id=release_id,
                            applied_by=executed_by,
                            reuse_override_confirmed=reuse_override_confirmed,
                        )
                        executed_count += 1
                        results.append(
                            {
                                "release_id": release_id,
                                "status": "APPLIED",
                                "applied_preview_id": output["preview_id"],
                            }
                        )
                    except Exception as bg_exc:
                        blocked_count += 1
                        results.append(
                            {
                                "release_id": release_id,
                                "status": "BLOCKED",
                                "reason_code": getattr(bg_exc, "code", "VBATCH_APPLY_FAILED"),
                                "reason_detail": str(bg_exc),
                            }
                        )
                else:
                    blocked_count += 1
                    results.append(
                        {
                            "release_id": release_id,
                            "status": "BLOCKED",
                            "reason_code": getattr(cover_exc, "code", "VBATCH_APPLY_FAILED"),
                            "reason_detail": str(cover_exc),
                        }
                    )
        elif action_type == "BULK_ASSIGN_BACKGROUND":
            try:
                preview = background_assignment_service.preview_background_assignment(
                    conn,
                    release_id=release_id,
                    background_asset_id=selected_background_asset_id,
                    source_family=None,
                    source_reference=None,
                    template_assisted=False,
                    selected_by=executed_by,
                )
                background_assignment_service.approve_background_assignment(
                    conn,
                    release_id=release_id,
                    preview_id=str(preview["preview_id"]),
                    approved_by=executed_by,
                )
                output = background_assignment_service.apply_background_assignment(
                    conn,
                    release_id=release_id,
                    applied_by=executed_by,
                    reuse_override_confirmed=reuse_override_confirmed,
                )
                executed_count += 1
                results.append(
                    {
                        "release_id": release_id,
                        "status": "APPLIED",
                        "applied_preview_id": output["preview_id"],
                    }
                )
            except Exception as bg_exc:
                blocked_count += 1
                results.append(
                    {
                        "release_id": release_id,
                        "status": "BLOCKED",
                        "reason_code": getattr(bg_exc, "code", "VBATCH_APPLY_FAILED"),
                        "reason_detail": str(bg_exc),
                    }
                )
        elif action_type in {"BULK_GENERATE_PREVIEWS", "BULK_RERUN_ASSISTED"}:
            try:
                preview = background_assignment_service.preview_background_assignment(
                    conn,
                    release_id=release_id,
                    background_asset_id=selected_background_asset_id,
                    source_family=None,
                    source_reference=None,
                    template_assisted=(action_type == "BULK_RERUN_ASSISTED"),
                    selected_by=executed_by,
                )
                executed_count += 1
                results.append(
                    {
                        "release_id": release_id,
                        "status": "PREVIEWED",
                        "preview_id": preview["preview_id"],
                    }
                )
            except Exception as preview_exc:
                blocked_count += 1
                results.append(
                    {
                        "release_id": release_id,
                        "status": "BLOCKED",
                        "reason_code": getattr(preview_exc, "code", "VBATCH_PREVIEW_FAILED"),
                        "reason_detail": str(preview_exc),
                    }
                )
        else:
            executed_count += 1
            results.append(
                {
                    "release_id": release_id,
                    "status": "EXECUTED",
                    "reason_code": "NOOP_PREVIEWED_OPERATION",
                    "reason_detail": f"{action_type} executed in preview-safe mode",
                }
            )

    executed_at = _now_iso()
    conn.execute(
        """
        UPDATE release_visual_batch_preview_sessions
        SET session_status = 'APPLIED', executed_at = ?, invalidation_reason_code = NULL
        WHERE id = ?
        """,
        (executed_at, preview_session_id),
    )
    return {
        "preview_session_id": preview_session_id,
        "action_type": action_type,
        "aggregate": {
            "scope_total": len(requested_scope),
            "executed_count": executed_count,
            "blocked_count": blocked_count,
            "warning_count": warning_count,
        },
        "items": results,
        "invalidation_reason_code": None,
    }


def _load_release_rows(conn: sqlite3.Connection, release_ids: list[int]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in release_ids)
    rows = conn.execute(
        f"SELECT id, channel_id FROM releases WHERE id IN ({placeholders}) ORDER BY id ASC",
        tuple(release_ids),
    ).fetchall()
    if len(rows) != len(release_ids):
        found = {int(row["id"]) for row in rows}
        missing = sorted(set(release_ids) - found)
        raise VisualBatchError(code="VBATCH_RELEASES_NOT_FOUND", message=f"releases not found: {missing}")
    return [dict(row) for row in rows]


def _scope_fingerprint(*, action_type: str, release_ids: list[int]) -> str:
    payload = f"{action_type}|{','.join(str(item) for item in release_ids)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _as_optional_positive_int(value: Any) -> int | None:
    if value is None:
        return None
    parsed = int(value)
    return parsed if parsed > 0 else None
