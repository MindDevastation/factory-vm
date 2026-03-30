from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any

from services.common import db as dbm


class RuntimeVisualResolverError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class RuntimeVisualResolutionResult:
    release_id: int
    release_decision_written: bool
    runtime_bound: bool
    deferred: bool
    job_id: int | None


def apply_release_visual_package(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    background_asset_id: int,
    cover_asset_id: int,
    source_preview_id: str | None,
    applied_by: str | None,
) -> RuntimeVisualResolutionResult:
    release = _get_release(conn, release_id=release_id)
    _validate_asset(conn, channel_id=int(release["channel_id"]), asset_id=background_asset_id)
    _validate_asset(conn, channel_id=int(release["channel_id"]), asset_id=cover_asset_id)

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    conn.execute(
        """
        INSERT INTO release_visual_applied_packages(
            release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at
        ) VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(release_id) DO UPDATE SET
            background_asset_id = excluded.background_asset_id,
            cover_asset_id = excluded.cover_asset_id,
            source_preview_id = excluded.source_preview_id,
            applied_by = excluded.applied_by,
            applied_at = excluded.applied_at
        """,
        (release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, now_iso),
    )
    outcome = resolve_runtime_visual_bindings_for_release(conn, release_id=release_id)
    return RuntimeVisualResolutionResult(
        release_id=release_id,
        release_decision_written=True,
        runtime_bound=outcome.runtime_bound,
        deferred=outcome.deferred,
        job_id=outcome.job_id,
    )


def resolve_runtime_visual_bindings_for_release(
    conn: sqlite3.Connection,
    *,
    release_id: int,
) -> RuntimeVisualResolutionResult:
    _get_release(conn, release_id=release_id)
    applied = conn.execute(
        """
        SELECT release_id, background_asset_id, cover_asset_id
        FROM release_visual_applied_packages
        WHERE release_id = ?
        """,
        (release_id,),
    ).fetchone()
    if not applied:
        return RuntimeVisualResolutionResult(
            release_id=release_id,
            release_decision_written=False,
            runtime_bound=False,
            deferred=False,
            job_id=None,
        )

    release_row = conn.execute("SELECT current_open_job_id FROM releases WHERE id = ?", (release_id,)).fetchone()
    assert release_row is not None
    open_job_id = release_row["current_open_job_id"]
    if open_job_id is None:
        return RuntimeVisualResolutionResult(
            release_id=release_id,
            release_decision_written=True,
            runtime_bound=False,
            deferred=True,
            job_id=None,
        )

    job = conn.execute("SELECT id, release_id FROM jobs WHERE id = ?", (int(open_job_id),)).fetchone()
    if not job or int(job["release_id"]) != int(release_id):
        return RuntimeVisualResolutionResult(
            release_id=release_id,
            release_decision_written=True,
            runtime_bound=False,
            deferred=True,
            job_id=None,
        )

    job_id = int(job["id"])
    conn.execute("DELETE FROM job_inputs WHERE job_id = ? AND role IN ('BACKGROUND', 'COVER')", (job_id,))
    dbm.link_job_input(conn, job_id, int(applied["background_asset_id"]), "BACKGROUND", 0)
    dbm.link_job_input(conn, job_id, int(applied["cover_asset_id"]), "COVER", 0)
    _sync_ui_job_draft_visual_names(conn, job_id=job_id, background_asset_id=int(applied["background_asset_id"]), cover_asset_id=int(applied["cover_asset_id"]))
    return RuntimeVisualResolutionResult(
        release_id=release_id,
        release_decision_written=True,
        runtime_bound=True,
        deferred=False,
        job_id=job_id,
    )


def _sync_ui_job_draft_visual_names(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    background_asset_id: int,
    cover_asset_id: int,
) -> None:
    draft = dbm.get_ui_job_draft(conn, job_id)
    if not draft:
        return
    background_asset = conn.execute("SELECT name FROM assets WHERE id = ?", (background_asset_id,)).fetchone()
    cover_asset = conn.execute("SELECT name FROM assets WHERE id = ?", (cover_asset_id,)).fetchone()
    if not background_asset or not cover_asset:
        return
    background_name = str(background_asset["name"] or "")
    cover_name = str(cover_asset["name"] or "")
    dbm.update_ui_job_draft(
        conn,
        job_id=job_id,
        title=str(draft["title"]),
        description=str(draft["description"]),
        tags_csv=str(draft["tags_csv"]),
        cover_name=cover_name,
        cover_ext=_asset_ext(cover_name),
        background_name=background_name,
        background_ext=_asset_ext(background_name),
        audio_ids_text=str(draft["audio_ids_text"]),
    )


def _asset_ext(name: str) -> str:
    suffix = Path(name).suffix.lstrip(".").strip().lower()
    return suffix or "png"


def _get_release(conn: sqlite3.Connection, *, release_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT id, channel_id FROM releases WHERE id = ?", (release_id,)).fetchone()
    if not row:
        raise RuntimeVisualResolverError(code="VISUAL_RELEASE_NOT_FOUND", message="Release not found")
    return row


def _validate_asset(conn: sqlite3.Connection, *, channel_id: int, asset_id: int) -> None:
    row = conn.execute(
        "SELECT id, channel_id FROM assets WHERE id = ?",
        (asset_id,),
    ).fetchone()
    if not row:
        raise RuntimeVisualResolverError(code="VISUAL_ASSET_NOT_FOUND", message="Visual asset not found")
    if int(row["channel_id"]) != int(channel_id):
        raise RuntimeVisualResolverError(code="VISUAL_ASSET_CHANNEL_MISMATCH", message="Visual asset channel mismatch")
