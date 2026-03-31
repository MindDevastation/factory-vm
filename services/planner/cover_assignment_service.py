from __future__ import annotations

from datetime import datetime, timezone
import json
import secrets
import sqlite3
from typing import Any


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
