from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
import secrets
import sqlite3
from typing import Any

from services.common import db as dbm
from services.metadata import channel_visual_style_template_service
from services.planner.background_source_adapter_registry import (
    ALLOWED_BACKGROUND_SOURCE_FAMILIES,
    BackgroundCandidate,
    build_default_background_source_adapter_registry,
)
from services.planner.runtime_visual_resolver import apply_release_visual_package
from services.planner import visual_history_service
from services.visual_domain import VisualLifecycleError, build_apply_tokens, build_visual_package_summary, validate_apply_safety

BACKGROUND_PREVIEW_SCOPE = dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND


class BackgroundAssignmentError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def list_background_candidates(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    template_assisted: bool = False,
) -> dict[str, Any]:
    release = _get_release(conn, release_id=release_id)
    registry = build_default_background_source_adapter_registry()

    candidates: list[BackgroundCandidate] = []
    for source_family in registry.list_families():
        candidates.extend(registry.get(source_family)(conn, release_id, int(release["channel_id"])))

    deduped: dict[int, BackgroundCandidate] = {}
    for candidate in candidates:
        deduped[candidate.asset_id] = candidate
    ordered = sorted(deduped.values(), key=lambda item: item.asset_id, reverse=True)

    prefill_asset_id, prefill_template_assisted = _resolve_prefill_background_asset_id(conn, release_id=release_id)
    payload_candidates: list[dict[str, Any]] = []
    for item in ordered:
        row = asdict(item)
        if prefill_asset_id is not None and item.asset_id == prefill_asset_id:
            row["selection_mode_prefill"] = "auto_assisted"
            row["template_assisted"] = bool(prefill_template_assisted)
        payload_candidates.append(row)

    return {
        "release_id": release_id,
        "channel_id": int(release["channel_id"]),
        "source_families": list(ALLOWED_BACKGROUND_SOURCE_FAMILIES),
        "prefill": {
            "background_asset_id": prefill_asset_id,
            "selection_mode": "auto_assisted" if prefill_asset_id is not None else "manual",
            "template_assisted": bool(prefill_template_assisted),
        },
        "candidates": payload_candidates,
    }


def preview_background_assignment(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    background_asset_id: int | None,
    source_family: str | None,
    source_reference: str | None,
    template_assisted: bool,
    selected_by: str | None,
) -> dict[str, Any]:
    candidates_payload = list_background_candidates(conn, release_id=release_id, template_assisted=template_assisted)
    selected = _select_candidate_or_error(
        candidates_payload=candidates_payload,
        requested_asset_id=background_asset_id,
        requested_source_family=source_family,
        requested_source_reference=source_reference,
    )

    selection_mode = "manual" if background_asset_id is not None else "auto_assisted"
    template_assisted_effective = False if background_asset_id is not None else bool(selected.get("template_assisted", False))
    now_iso = _now_iso()
    preview_id = f"vbg-{secrets.token_hex(8)}"
    intent_snapshot = {
        "background": {
            "asset_id": int(selected["asset_id"]),
            "source_family": str(selected["source_family"]),
            "source_reference": selected.get("source_reference"),
            "selection_mode": selection_mode,
            "template_assisted": bool(template_assisted_effective),
        }
    }
    preview_package = {
        "background_asset_id": int(selected["asset_id"]),
        "source_family": str(selected["source_family"]),
        "source_reference": selected.get("source_reference"),
        "selection_mode": selection_mode,
        "template_assisted": bool(template_assisted_effective),
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
            BACKGROUND_PREVIEW_SCOPE,
            json.dumps(intent_snapshot, sort_keys=True),
            json.dumps(preview_package, sort_keys=True),
            selected_by,
            now_iso,
        ),
    )
    visual_history_service.record_visual_history_event(
        conn,
        release_id=release_id,
        preview_scope=BACKGROUND_PREVIEW_SCOPE,
        history_stage="PREVIEWED",
        preview_id=preview_id,
        background_asset_id=int(selected["asset_id"]),
        cover_asset_id=None,
        template_ref=None,
        decision_mode=selection_mode,
        reuse_warning=None,
        actor=selected_by,
    )
    return {
        "release_id": release_id,
        "preview_id": preview_id,
        "selection": preview_package,
        "summary": {
            "requested_fields": ["background"],
            "prepared_fields": ["background"],
        },
    }


def approve_background_assignment(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    preview_id: str,
    approved_by: str | None,
) -> dict[str, Any]:
    preview_row = conn.execute(
        "SELECT id, release_id, preview_scope FROM release_visual_preview_snapshots WHERE id = ?",
        (preview_id,),
    ).fetchone()
    if not preview_row or int(preview_row["release_id"]) != int(release_id):
        raise BackgroundAssignmentError(code="VBG_PREVIEW_NOT_FOUND", message="Background preview not found")
    preview_scope = str(preview_row.get("preview_scope") or dbm.VISUAL_PREVIEW_SCOPE_PACKAGE)
    if preview_scope not in {BACKGROUND_PREVIEW_SCOPE, dbm.VISUAL_PREVIEW_SCOPE_PACKAGE}:
        raise BackgroundAssignmentError(code="VBG_PREVIEW_SCOPE_MISMATCH", message="Background preview scope mismatch")

    now_iso = _now_iso()
    dbm.upsert_release_visual_approved_preview(
        conn,
        release_id=release_id,
        preview_scope=BACKGROUND_PREVIEW_SCOPE,
        preview_id=preview_id,
        approved_by=approved_by,
        approved_at=now_iso,
    )
    snapshot = conn.execute(
        "SELECT id, intent_snapshot_json, preview_package_json FROM release_visual_preview_snapshots WHERE id = ?",
        (preview_id,),
    ).fetchone()
    parsed = json.loads(str(snapshot["preview_package_json"]) or "{}") if snapshot else {}
    background_asset_id = int(parsed.get("background_asset_id") or 0) or None
    visual_history_service.record_visual_history_event(
        conn,
        release_id=release_id,
        preview_scope=BACKGROUND_PREVIEW_SCOPE,
        history_stage="APPROVED",
        preview_id=preview_id,
        background_asset_id=int(parsed.get("background_asset_id") or 0) or None,
        cover_asset_id=None,
        template_ref=None,
        decision_mode=str(parsed.get("selection_mode") or "manual"),
        reuse_warning=None,
        actor=approved_by,
    )
    cover_asset_id = dbm.resolve_canonical_cover_asset_id_for_background_apply(conn, release_id=release_id)
    current_intent = {"background": {"asset_id": int(background_asset_id or 0)}, "cover": {"asset_id": int(cover_asset_id or 0)}}
    apply_tokens = build_apply_tokens(
        release_id=release_id,
        snapshot_row=dict(snapshot),
        current_intent_config_json=current_intent,
    )
    return {
        "release_id": release_id,
        "preview_id": preview_id,
        "approved": True,
        "stale_token": apply_tokens.stale_token,
        "conflict_token": apply_tokens.conflict_token,
    }


def apply_background_assignment(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    applied_by: str | None,
    reuse_override_confirmed: bool = False,
    stale_token: str | None = None,
    conflict_token: str | None = None,
) -> dict[str, Any]:
    approved = dbm.get_release_visual_approved_preview(
        conn,
        release_id=release_id,
        preview_scope=BACKGROUND_PREVIEW_SCOPE,
    )
    if not approved:
        raise BackgroundAssignmentError(code="VBG_APPROVAL_REQUIRED", message="Approved background preview is required before apply")

    snapshot = conn.execute(
        "SELECT id, intent_snapshot_json, preview_package_json FROM release_visual_preview_snapshots WHERE id = ?",
        (str(approved["preview_id"]),),
    ).fetchone()
    if not snapshot:
        raise BackgroundAssignmentError(code="VBG_PREVIEW_NOT_FOUND", message="Approved background preview not found")

    parsed = json.loads(str(snapshot["preview_package_json"]) or "{}")
    background_asset_id = int(parsed.get("background_asset_id") or 0)
    if background_asset_id <= 0:
        raise BackgroundAssignmentError(code="VBG_INVALID_PREVIEW", message="Approved preview has no background asset")

    cover_asset_id = dbm.resolve_canonical_cover_asset_id_for_background_apply(conn, release_id=release_id)
    if cover_asset_id is None:
        raise BackgroundAssignmentError(
            code="VBG_CANONICAL_COVER_REQUIRED",
            message="Background apply requires canonical cover asset",
        )
    applied_package = conn.execute(
        "SELECT source_preview_id FROM release_visual_applied_packages WHERE release_id = ?",
        (release_id,),
    ).fetchone()
    current_intent = {"background": {"asset_id": background_asset_id}, "cover": {"asset_id": int(cover_asset_id)}}
    if not stale_token:
        raise BackgroundAssignmentError(code="VBG_PREVIEW_STALE", message="stale_token is required before apply")
    if not conflict_token:
        raise BackgroundAssignmentError(code="VBG_APPLY_CONFLICT", message="conflict_token is required before apply")
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
            "VISUAL_PREVIEW_STALE": "VBG_PREVIEW_STALE",
            "VISUAL_APPLY_CONFLICT": "VBG_APPLY_CONFLICT",
            "VISUAL_ALREADY_APPLIED": "VBG_ALREADY_APPLIED",
            "VISUAL_APPROVAL_REQUIRED": "VBG_APPROVAL_REQUIRED",
        }
        raise BackgroundAssignmentError(
            code=code_map.get(lifecycle_exc.code, "VBG_APPLY_CONFLICT"),
            message=lifecycle_exc.message,
        ) from lifecycle_exc

    reuse = visual_history_service.lookup_exact_reuse_warnings(
        conn,
        release_id=release_id,
        background_asset_id=background_asset_id,
        cover_asset_id=int(cover_asset_id),
    )
    if reuse["requires_override"] and not reuse_override_confirmed:
        prior = reuse["prior_usage"][0] if reuse["prior_usage"] else {}
        raise BackgroundAssignmentError(
            code="VBG_REUSE_OVERRIDE_REQUIRED",
            message=(
                "Exact reuse warning requires explicit override; "
                f"prior_release_id={prior.get('release_id')}"
            ),
        )

    now_iso = _now_iso()
    dbm.upsert_release_visual_background_decision(
        conn,
        release_id=release_id,
        background_asset_id=background_asset_id,
        source_family=str(parsed.get("source_family") or "known_resolved"),
        source_reference=(str(parsed["source_reference"]) if parsed.get("source_reference") is not None else None),
        selection_mode=str(parsed.get("selection_mode") or "manual"),
        template_assisted=bool(parsed.get("template_assisted", False)),
        created_at=now_iso,
        updated_at=now_iso,
    )

    resolved = apply_release_visual_package(
        conn,
        release_id=release_id,
        background_asset_id=background_asset_id,
        cover_asset_id=int(cover_asset_id),
        source_preview_id=str(snapshot["id"]),
        applied_by=applied_by,
    )
    summary = build_visual_package_summary(
        release_id=release_id,
        package={
            "background_asset_id": background_asset_id,
            "cover_asset_id": int(cover_asset_id),
            "source_family": str(parsed.get("source_family") or "known_resolved"),
            "source_reference": parsed.get("source_reference"),
            "selection_mode": str(parsed.get("selection_mode") or "manual"),
            "template_assisted": bool(parsed.get("template_assisted", False)),
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
        preview_scope=BACKGROUND_PREVIEW_SCOPE,
        history_stage="APPLIED",
        preview_id=str(snapshot["id"]),
        background_asset_id=background_asset_id,
        cover_asset_id=int(cover_asset_id),
        template_ref=None,
        decision_mode=str(parsed.get("selection_mode") or "manual"),
        reuse_warning=reuse_audit,
        actor=applied_by,
    )
    return {
        "release_id": release_id,
        "preview_id": str(snapshot["id"]),
        "background_asset_id": background_asset_id,
        "cover_asset_id": int(cover_asset_id),
        "runtime": {
            "runtime_bound": bool(resolved.runtime_bound),
            "deferred": bool(resolved.deferred),
            "job_id": resolved.job_id,
        },
        "summary": summary,
        "reuse": reuse,
    }


def _select_candidate_or_error(
    *,
    candidates_payload: dict[str, Any],
    requested_asset_id: int | None,
    requested_source_family: str | None,
    requested_source_reference: str | None,
) -> dict[str, Any]:
    candidates = list(candidates_payload.get("candidates") or [])
    if requested_source_family == "generation":
        raise BackgroundAssignmentError(code="VBG_GENERATION_NOT_SUPPORTED", message="Background generation is not supported")
    if requested_source_family and requested_source_family not in ALLOWED_BACKGROUND_SOURCE_FAMILIES:
        raise BackgroundAssignmentError(code="VBG_UNSUPPORTED_SOURCE_FAMILY", message="Unsupported source family")

    if requested_asset_id is not None:
        for item in candidates:
            if int(item["asset_id"]) == int(requested_asset_id):
                if requested_source_family and str(item["source_family"]) != requested_source_family:
                    raise BackgroundAssignmentError(code="VBG_CANDIDATE_MISMATCH", message="Requested source family does not match candidate")
                if requested_source_reference and str(item.get("source_reference") or "") != requested_source_reference:
                    raise BackgroundAssignmentError(code="VBG_CANDIDATE_MISMATCH", message="Requested source reference does not match candidate")
                return item
        raise BackgroundAssignmentError(code="VBG_CANDIDATE_NOT_FOUND", message="Requested background candidate not found")

    prefill_asset_id = candidates_payload.get("prefill", {}).get("background_asset_id")
    if prefill_asset_id is not None:
        for item in candidates:
            if int(item["asset_id"]) == int(prefill_asset_id):
                return item

    if candidates:
        return candidates[0]

    raise BackgroundAssignmentError(code="VBG_NO_CANDIDATES", message="No background candidates available")


def _get_release(conn: sqlite3.Connection, *, release_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT id, channel_id FROM releases WHERE id = ?", (release_id,)).fetchone()
    if not row:
        raise BackgroundAssignmentError(code="VBG_RELEASE_NOT_FOUND", message="Release not found")
    return dict(row)


def _resolve_prefill_background_asset_id(conn: sqlite3.Connection, *, release_id: int) -> tuple[int | None, bool]:
    decision = dbm.get_release_visual_background_decision_by_release_id(conn, release_id=release_id)
    if decision and decision.get("background_asset_id") is not None:
        return int(decision["background_asset_id"]), bool(int(decision.get("template_assisted") or 0))

    applied = conn.execute(
        "SELECT background_asset_id FROM release_visual_applied_packages WHERE release_id = ?",
        (release_id,),
    ).fetchone()
    if applied and applied["background_asset_id"] is not None:
        return int(applied["background_asset_id"]), False

    assisted_asset_id = _resolve_assisted_prefill_asset_id(conn, release_id=release_id)
    if assisted_asset_id is None:
        return None, False
    return assisted_asset_id, True


def _resolve_assisted_prefill_asset_id(conn: sqlite3.Connection, *, release_id: int) -> int | None:
    resolved = channel_visual_style_template_service.resolve_effective_channel_visual_style_template_for_release(
        conn,
        release_id=release_id,
    )
    template = resolved.get("effective_template") or {}
    payload = template.get("template_payload") if isinstance(template, dict) else None
    if not isinstance(payload, dict):
        return None
    raw_asset_id = payload.get("default_background_asset_id")
    if raw_asset_id is None:
        return None
    try:
        asset_id = int(raw_asset_id)
    except (TypeError, ValueError):
        return None

    release = conn.execute("SELECT channel_id FROM releases WHERE id = ?", (release_id,)).fetchone()
    if not release:
        return None
    exists = conn.execute(
        "SELECT id FROM assets WHERE id = ? AND channel_id = ?",
        (asset_id, int(release["channel_id"])),
    ).fetchone()
    if not exists:
        return None
    return asset_id


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
