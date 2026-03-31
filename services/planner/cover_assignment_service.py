from __future__ import annotations

from datetime import datetime, timezone
import json
import secrets
import sqlite3
from typing import Any

from services.common import db as dbm
from services.planner.runtime_visual_resolver import apply_release_visual_package
from services.planner import visual_history_service
from services.visual_domain import VisualLifecycleError, build_apply_tokens, build_visual_package_summary, validate_apply_safety

COVER_PREVIEW_SCOPE = dbm.VISUAL_PREVIEW_SCOPE_COVER


class CoverAssignmentError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def create_cover_selection_input(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    provider_family: str,
    input_payload: dict[str, Any],
    template_ref: dict[str, Any] | None,
    created_by: str | None,
) -> dict[str, Any]:
    release = _get_release(conn, release_id=release_id)
    provider_family_clean = str(provider_family or "").strip()
    if not provider_family_clean:
        raise CoverAssignmentError(code="VCOVER_PROVIDER_FAMILY_REQUIRED", message="provider_family is required")
    created_at = _now_iso()
    row_id = int(
        conn.execute(
            """
            INSERT INTO release_visual_cover_selection_inputs(
                release_id, provider_family, input_payload_json, template_ref_json, created_by, created_at
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                release_id,
                provider_family_clean,
                json.dumps(input_payload or {}, sort_keys=True),
                json.dumps(template_ref, sort_keys=True) if template_ref is not None else None,
                created_by,
                created_at,
            ),
        ).lastrowid
    )
    return {
        "input_payload_id": row_id,
        "release_id": int(release["id"]),
        "provider_family": provider_family_clean,
        "input_payload": dict(input_payload or {}),
        "template_ref": dict(template_ref) if template_ref is not None else None,
        "created_at": created_at,
    }


def create_cover_candidate_reference(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    cover_asset_id: int,
    source_provider_family: str,
    source_reference: str | None,
    input_payload_id: int | None,
    selection_mode: str,
    template_ref: dict[str, Any] | None,
    created_by: str | None,
) -> dict[str, Any]:
    release = _get_release(conn, release_id=release_id)
    source_provider_family_clean = str(source_provider_family or "").strip()
    if not source_provider_family_clean:
        raise CoverAssignmentError(code="VCOVER_PROVIDER_FAMILY_REQUIRED", message="source_provider_family is required")
    if selection_mode not in {"manual", "auto_assisted"}:
        raise CoverAssignmentError(code="VCOVER_SELECTION_MODE_INVALID", message="selection_mode must be manual or auto_assisted")
    if input_payload_id is not None:
        row = conn.execute(
            "SELECT id, release_id FROM release_visual_cover_selection_inputs WHERE id = ?",
            (input_payload_id,),
        ).fetchone()
        if not row or int(row["release_id"]) != int(release_id):
            raise CoverAssignmentError(code="VCOVER_INPUT_PAYLOAD_NOT_FOUND", message="input_payload_id not found for release")

    asset = conn.execute("SELECT id, channel_id, name, path FROM assets WHERE id = ?", (cover_asset_id,)).fetchone()
    if not asset:
        raise CoverAssignmentError(code="VCOVER_ASSET_NOT_FOUND", message="cover asset not found")
    if int(asset["channel_id"]) != int(release["channel_id"]):
        raise CoverAssignmentError(code="VCOVER_ASSET_CHANNEL_MISMATCH", message="cover asset channel mismatch")

    candidate_id = f"vcov-{secrets.token_hex(8)}"
    created_at = _now_iso()
    candidate_ref = {
        "asset_id": int(asset["id"]),
        "asset_name": str(asset["name"] or ""),
        "asset_path": str(asset["path"] or ""),
    }
    conn.execute(
        """
        INSERT INTO release_visual_cover_candidates(
            id, release_id, source_provider_family, source_reference, input_payload_id,
            candidate_ref_json, selection_mode, template_ref_json, created_by, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            release_id,
            source_provider_family_clean,
            source_reference,
            input_payload_id,
            json.dumps(candidate_ref, sort_keys=True),
            selection_mode,
            json.dumps(template_ref, sort_keys=True) if template_ref is not None else None,
            created_by,
            created_at,
        ),
    )
    return _candidate_payload(conn, candidate_id=candidate_id)


def list_cover_candidates(conn: sqlite3.Connection, *, release_id: int) -> dict[str, Any]:
    _get_release(conn, release_id=release_id)
    rows = conn.execute(
        """
        SELECT c.*, i.provider_family AS input_provider_family
        FROM release_visual_cover_candidates c
        LEFT JOIN release_visual_cover_selection_inputs i ON i.id = c.input_payload_id
        WHERE c.release_id = ?
        ORDER BY c.created_at DESC, c.id DESC
        """,
        (release_id,),
    ).fetchall()
    selected = conn.execute(
        "SELECT candidate_id FROM release_visual_cover_selected_candidates WHERE release_id = ?",
        (release_id,),
    ).fetchone()
    return {
        "release_id": release_id,
        "selected_candidate_id": str(selected["candidate_id"]) if selected else None,
        "candidates": [_candidate_row_to_payload(row) for row in rows],
    }


def preview_cover_candidate(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    candidate_id: str,
) -> dict[str, Any]:
    _get_release(conn, release_id=release_id)
    candidate = _candidate_row(conn, release_id=release_id, candidate_id=candidate_id)
    return {
        "release_id": release_id,
        "candidate_id": str(candidate["candidate_id"]),
        "preview": {
            "cover_asset": candidate["cover_asset"],
            "source_provider_family": candidate["source_provider_family"],
            "source_reference": candidate["source_reference"],
            "selection_mode": candidate["selection_mode"],
            "input_payload_id": candidate["input_payload_id"],
            "template_ref": candidate["template_ref"],
        },
    }


def select_cover_candidate_for_approval(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    candidate_id: str,
    selected_by: str | None,
) -> dict[str, Any]:
    _get_release(conn, release_id=release_id)
    candidate = _candidate_row(conn, release_id=release_id, candidate_id=candidate_id)
    selected_at = _now_iso()
    conn.execute(
        """
        INSERT INTO release_visual_cover_selected_candidates(release_id, candidate_id, selected_by, selected_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(release_id) DO UPDATE SET
            candidate_id=excluded.candidate_id,
            selected_by=excluded.selected_by,
            selected_at=excluded.selected_at
        """,
        (release_id, candidate_id, selected_by, selected_at),
    )
    return {
        "release_id": release_id,
        "selected_candidate_id": candidate_id,
        "approval_path_ready": True,
        "selection_mode": candidate["selection_mode"],
    }


def approve_cover_candidate(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    candidate_id: str | None,
    approved_by: str | None,
) -> dict[str, Any]:
    _get_release(conn, release_id=release_id)
    candidate = _resolve_candidate_for_approval(conn, release_id=release_id, candidate_id=candidate_id)
    preview_id = f"vcvp-{secrets.token_hex(8)}"
    now_iso = _now_iso()
    intent_snapshot = {
        "cover": {
            "asset_id": int(candidate["cover_asset"]["asset_id"]),
            "source_provider_family": str(candidate["source_provider_family"]),
            "source_reference": candidate["source_reference"],
            "selection_mode": candidate["selection_mode"],
            "input_payload_id": candidate["input_payload_id"],
            "template_ref": candidate["template_ref"],
        }
    }
    preview_package = {
        "cover_asset_id": int(candidate["cover_asset"]["asset_id"]),
        "source_provider_family": str(candidate["source_provider_family"]),
        "source_reference": candidate["source_reference"],
        "selection_mode": candidate["selection_mode"],
        "input_payload_id": candidate["input_payload_id"],
        "template_ref": candidate["template_ref"],
    }
    conn.execute(
        """
        INSERT INTO release_visual_preview_snapshots(
            id, release_id, preview_scope, intent_snapshot_json, preview_package_json, created_by, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (
            preview_id,
            release_id,
            COVER_PREVIEW_SCOPE,
            json.dumps(intent_snapshot, sort_keys=True),
            json.dumps(preview_package, sort_keys=True),
            approved_by,
            now_iso,
        ),
    )
    visual_history_service.record_visual_history_event(
        conn,
        release_id=release_id,
        preview_scope=COVER_PREVIEW_SCOPE,
        history_stage="PREVIEWED",
        preview_id=preview_id,
        background_asset_id=None,
        cover_asset_id=int(candidate["cover_asset"]["asset_id"]),
        template_ref=candidate.get("template_ref"),
        decision_mode=str(candidate.get("selection_mode") or "manual"),
        reuse_warning=None,
        actor=approved_by,
    )
    dbm.upsert_release_visual_approved_preview(
        conn,
        release_id=release_id,
        preview_scope=COVER_PREVIEW_SCOPE,
        preview_id=preview_id,
        approved_by=approved_by,
        approved_at=now_iso,
    )
    visual_history_service.record_visual_history_event(
        conn,
        release_id=release_id,
        preview_scope=COVER_PREVIEW_SCOPE,
        history_stage="APPROVED",
        preview_id=preview_id,
        background_asset_id=None,
        cover_asset_id=int(candidate["cover_asset"]["asset_id"]),
        template_ref=candidate.get("template_ref"),
        decision_mode=str(candidate.get("selection_mode") or "manual"),
        reuse_warning=None,
        actor=approved_by,
    )
    background_asset_id = dbm.resolve_canonical_background_asset_id_for_cover_apply(conn, release_id=release_id)
    if background_asset_id is None:
        raise CoverAssignmentError(
            code="VCOVER_CANONICAL_BACKGROUND_REQUIRED",
            message="cover apply requires canonical background asset",
        )
    snapshot_row = conn.execute(
        "SELECT id, intent_snapshot_json, preview_package_json FROM release_visual_preview_snapshots WHERE id = ?",
        (preview_id,),
    ).fetchone()
    if not snapshot_row:
        raise CoverAssignmentError(code="VCOVER_PREVIEW_NOT_FOUND", message="approved cover preview not found")
    current_intent = {"background": {"asset_id": int(background_asset_id)}, "cover": {"asset_id": int(candidate["cover_asset"]["asset_id"])}}
    apply_tokens = build_apply_tokens(
        release_id=release_id,
        snapshot_row=dict(snapshot_row),
        current_intent_config_json=current_intent,
    )
    return {
        "release_id": release_id,
        "preview_id": preview_id,
        "candidate_id": candidate["candidate_id"],
        "approved": True,
        "stale_token": apply_tokens.stale_token,
        "conflict_token": apply_tokens.conflict_token,
    }


def apply_cover_candidate(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    applied_by: str | None,
    reuse_override_confirmed: bool = False,
    stale_token: str | None = None,
    conflict_token: str | None = None,
) -> dict[str, Any]:
    _get_release(conn, release_id=release_id)
    approved = dbm.get_release_visual_approved_preview(
        conn,
        release_id=release_id,
        preview_scope=COVER_PREVIEW_SCOPE,
    )
    if not approved:
        raise CoverAssignmentError(code="VCOVER_APPROVAL_REQUIRED", message="approved cover preview is required before apply")

    snapshot = conn.execute(
        "SELECT id, intent_snapshot_json, preview_package_json FROM release_visual_preview_snapshots WHERE id = ?",
        (str(approved["preview_id"]),),
    ).fetchone()
    if not snapshot:
        raise CoverAssignmentError(code="VCOVER_PREVIEW_NOT_FOUND", message="approved cover preview not found")
    parsed = json.loads(str(snapshot["preview_package_json"]) or "{}")
    cover_asset_id = int(parsed.get("cover_asset_id") or 0)
    if cover_asset_id <= 0:
        raise CoverAssignmentError(code="VCOVER_INVALID_PREVIEW", message="approved preview has no cover asset")

    background_asset_id = dbm.resolve_canonical_background_asset_id_for_cover_apply(conn, release_id=release_id)
    if background_asset_id is None:
        raise CoverAssignmentError(
            code="VCOVER_CANONICAL_BACKGROUND_REQUIRED",
            message="cover apply requires canonical background asset",
        )
    applied_package = conn.execute(
        "SELECT source_preview_id FROM release_visual_applied_packages WHERE release_id = ?",
        (release_id,),
    ).fetchone()
    current_intent = {"background": {"asset_id": int(background_asset_id)}, "cover": {"asset_id": cover_asset_id}}
    if not stale_token:
        raise CoverAssignmentError(code="VCOVER_PREVIEW_STALE", message="stale_token is required before apply")
    if not conflict_token:
        raise CoverAssignmentError(code="VCOVER_APPLY_CONFLICT", message="conflict_token is required before apply")
    try:
        validate_apply_safety(
            release_id=release_id,
            snapshot_row=dict(snapshot),
            approved_preview_row=dict(approved),
            applied_package_row=dict(applied_package) if applied_package else None,
            current_intent_config_json=current_intent,
            provided_stale_token=stale_token,
            provided_conflict_token=conflict_token,
        )
    except VisualLifecycleError as lifecycle_exc:
        code_map = {
            "VISUAL_PREVIEW_STALE": "VCOVER_PREVIEW_STALE",
            "VISUAL_APPLY_CONFLICT": "VCOVER_APPLY_CONFLICT",
            "VISUAL_ALREADY_APPLIED": "VCOVER_ALREADY_APPLIED",
            "VISUAL_APPROVAL_REQUIRED": "VCOVER_APPROVAL_REQUIRED",
        }
        raise CoverAssignmentError(
            code=code_map.get(lifecycle_exc.code, "VCOVER_APPLY_CONFLICT"),
            message=lifecycle_exc.message,
        ) from lifecycle_exc

    reuse = visual_history_service.lookup_exact_reuse_warnings(
        conn,
        release_id=release_id,
        background_asset_id=int(background_asset_id),
        cover_asset_id=cover_asset_id,
    )
    if reuse["requires_override"] and not reuse_override_confirmed:
        prior = reuse["prior_usage"][0] if reuse["prior_usage"] else {}
        raise CoverAssignmentError(
            code="VCOVER_REUSE_OVERRIDE_REQUIRED",
            message=(
                "Exact reuse warning requires explicit override; "
                f"prior_release_id={prior.get('release_id')}"
            ),
        )

    resolved = apply_release_visual_package(
        conn,
        release_id=release_id,
        background_asset_id=int(background_asset_id),
        cover_asset_id=cover_asset_id,
        source_preview_id=str(snapshot["id"]),
        applied_by=applied_by,
    )
    summary = build_visual_package_summary(
        release_id=release_id,
        package={
            "background_asset_id": int(background_asset_id),
            "cover_asset_id": cover_asset_id,
            "source_family": str(parsed.get("source_provider_family") or "known_resolved"),
            "source_reference": parsed.get("source_reference"),
            "selection_mode": str(parsed.get("selection_mode") or "manual"),
            "template_assisted": str(parsed.get("selection_mode") or "manual") == "auto_assisted",
            "is_auto_assisted": str(parsed.get("selection_mode") or "manual") == "auto_assisted",
        },
    )
    reuse_audit: dict[str, Any] | None = None
    if reuse["warnings"]:
        reuse_audit = dict(reuse)
        reuse_audit["override_confirmed"] = bool(reuse_override_confirmed)
        reuse_audit["override_applied"] = bool(reuse["requires_override"] and reuse_override_confirmed)
    visual_history_service.record_visual_history_event(
        conn,
        release_id=release_id,
        preview_scope=COVER_PREVIEW_SCOPE,
        history_stage="APPLIED",
        preview_id=str(snapshot["id"]),
        background_asset_id=int(background_asset_id),
        cover_asset_id=cover_asset_id,
        template_ref=parsed.get("template_ref"),
        decision_mode=str(parsed.get("selection_mode") or "manual"),
        reuse_warning=reuse_audit,
        actor=applied_by,
    )
    return {
        "release_id": release_id,
        "preview_id": str(snapshot["id"]),
        "background_asset_id": int(background_asset_id),
        "cover_asset_id": cover_asset_id,
        "runtime": {
            "runtime_bound": bool(resolved.runtime_bound),
            "deferred": bool(resolved.deferred),
            "job_id": resolved.job_id,
        },
        "summary": summary,
        "reuse": reuse,
    }


def _candidate_payload(conn: sqlite3.Connection, *, candidate_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT c.*, i.provider_family AS input_provider_family
        FROM release_visual_cover_candidates c
        LEFT JOIN release_visual_cover_selection_inputs i ON i.id = c.input_payload_id
        WHERE c.id = ?
        """,
        (candidate_id,),
    ).fetchone()
    assert row is not None
    return _candidate_row_to_payload(row)


def _candidate_row(conn: sqlite3.Connection, *, release_id: int, candidate_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT c.*, i.provider_family AS input_provider_family
        FROM release_visual_cover_candidates c
        LEFT JOIN release_visual_cover_selection_inputs i ON i.id = c.input_payload_id
        WHERE c.id = ? AND c.release_id = ?
        """,
        (candidate_id, release_id),
    ).fetchone()
    if not row:
        raise CoverAssignmentError(code="VCOVER_CANDIDATE_NOT_FOUND", message="cover candidate not found")
    return _candidate_row_to_payload(row)


def _resolve_candidate_for_approval(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    candidate_id: str | None,
) -> dict[str, Any]:
    if candidate_id:
        return _candidate_row(conn, release_id=release_id, candidate_id=candidate_id)
    selected = conn.execute(
        "SELECT candidate_id FROM release_visual_cover_selected_candidates WHERE release_id = ?",
        (release_id,),
    ).fetchone()
    if not selected:
        raise CoverAssignmentError(
            code="VCOVER_SELECTION_REQUIRED",
            message="select a cover candidate before approve",
        )
    return _candidate_row(conn, release_id=release_id, candidate_id=str(selected["candidate_id"]))


def _candidate_row_to_payload(row: dict[str, Any]) -> dict[str, Any]:
    ref = json.loads(str(row["candidate_ref_json"]) or "{}")
    template_ref = json.loads(str(row["template_ref_json"])) if row.get("template_ref_json") else None
    return {
        "candidate_id": str(row["id"]),
        "release_id": int(row["release_id"]),
        "source_provider_family": str(row["source_provider_family"]),
        "source_reference": row["source_reference"],
        "input_payload_id": int(row["input_payload_id"]) if row["input_payload_id"] is not None else None,
        "input_provider_family": row.get("input_provider_family"),
        "selection_mode": str(row["selection_mode"]),
        "is_manual_selection": str(row["selection_mode"]) == "manual",
        "template_ref": template_ref,
        "cover_asset": {
            "asset_id": int(ref.get("asset_id") or 0),
            "asset_name": str(ref.get("asset_name") or ""),
            "asset_path": str(ref.get("asset_path") or ""),
        },
        "created_at": str(row["created_at"]),
    }


def _get_release(conn: sqlite3.Connection, *, release_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT id, channel_id FROM releases WHERE id = ?", (release_id,)).fetchone()
    if not row:
        raise CoverAssignmentError(code="VCOVER_RELEASE_NOT_FOUND", message="release not found")
    return row


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
