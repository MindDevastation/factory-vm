from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


def record_visual_history_event(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    preview_scope: str,
    history_stage: str,
    preview_id: str | None,
    background_asset_id: int | None,
    cover_asset_id: int | None,
    template_ref: dict[str, Any] | None,
    decision_mode: str | None,
    reuse_warning: dict[str, Any] | None,
    actor: str | None,
) -> dict[str, Any]:
    release = conn.execute("SELECT id, channel_id FROM releases WHERE id = ?", (release_id,)).fetchone()
    if not release:
        raise ValueError("release not found")
    created_at = _now_iso()
    event_id = int(
        conn.execute(
            """
            INSERT INTO release_visual_history_events(
                release_id, channel_id, preview_scope, history_stage, preview_id,
                background_asset_id, cover_asset_id, template_ref_json, decision_mode,
                reuse_warning_json, actor, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                release_id,
                int(release["channel_id"]),
                preview_scope,
                history_stage,
                preview_id,
                background_asset_id,
                cover_asset_id,
                json.dumps(template_ref, sort_keys=True) if template_ref is not None else None,
                decision_mode,
                json.dumps(reuse_warning, sort_keys=True) if reuse_warning is not None else None,
                actor,
                created_at,
            ),
        ).lastrowid
    )
    return {
        "event_id": event_id,
        "release_id": release_id,
        "channel_id": int(release["channel_id"]),
        "preview_scope": preview_scope,
        "history_stage": history_stage,
    }


def lookup_exact_reuse_warnings(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    background_asset_id: int,
    cover_asset_id: int,
) -> dict[str, Any]:
    release = conn.execute("SELECT channel_id FROM releases WHERE id = ?", (release_id,)).fetchone()
    if not release:
        raise ValueError("release not found")
    channel_id = int(release["channel_id"])
    rows = conn.execute(
        """
        SELECT p.release_id, p.background_asset_id, p.cover_asset_id, p.applied_at
        FROM release_visual_applied_packages p
        JOIN releases r ON r.id = p.release_id
        WHERE r.channel_id = ? AND p.release_id != ?
          AND (
            p.background_asset_id = ?
            OR p.cover_asset_id = ?
            OR (p.background_asset_id = ? AND p.cover_asset_id = ?)
          )
        ORDER BY p.applied_at DESC
        LIMIT 10
        """,
        (channel_id, release_id, background_asset_id, cover_asset_id, background_asset_id, cover_asset_id),
    ).fetchall()
    prior_usage = [dict(row) for row in rows]
    same_background = [row for row in prior_usage if int(row["background_asset_id"]) == int(background_asset_id)]
    same_cover = [row for row in prior_usage if int(row["cover_asset_id"]) == int(cover_asset_id)]
    same_package = [
        row
        for row in prior_usage
        if int(row["background_asset_id"]) == int(background_asset_id) and int(row["cover_asset_id"]) == int(cover_asset_id)
    ]
    warnings: list[str] = []
    if same_background:
        warnings.append("same background asset already applied in channel")
    if same_cover:
        warnings.append("same cover asset already applied in channel")
    if same_package:
        warnings.append("same visual package identity already applied in channel")
    return {
        "requires_override": len(warnings) > 0,
        "warnings": warnings,
        "prior_usage": prior_usage,
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
