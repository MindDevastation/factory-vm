from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from services.common import db as dbm


@dataclass(frozen=True)
class UiRenderEnqueueResult:
    enqueued: bool
    reason: str


@dataclass(frozen=True)
class UiRenderGuardResult:
    eligible: bool
    reason: str

    @property
    def not_found(self) -> bool:
        return self.reason == "not_found"

    @property
    def not_allowed(self) -> bool:
        return self.reason == "not_allowed"

    @property
    def already_in_progress(self) -> bool:
        return self.reason == "already_in_progress"


def check_ui_render_guard(
    conn: sqlite3.Connection,
    *,
    job_id: int,
) -> UiRenderGuardResult:
    job = conn.execute("SELECT job_type, state FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not job or str(job.get("job_type") or "") != "UI":
        return UiRenderGuardResult(eligible=False, reason="not_found")

    if str(job.get("state") or "") != "DRAFT":
        return UiRenderGuardResult(eligible=False, reason="not_allowed")

    has_inputs = conn.execute(
        "SELECT 1 FROM job_inputs WHERE job_id=? LIMIT 1",
        (job_id,),
    ).fetchone()
    if has_inputs:
        return UiRenderGuardResult(eligible=False, reason="already_in_progress")

    return UiRenderGuardResult(eligible=True, reason="eligible")


def enqueue_ui_render_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    channel_id: int,
    tracks: list[dict[str, Any]],
    background_file_id: str,
    background_filename: str,
    cover_file_id: str,
    cover_filename: str,
) -> UiRenderEnqueueResult:
    tx_started = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        tx_started = True

        guard = check_ui_render_guard(conn, job_id=job_id)
        if guard.not_found:
            conn.execute("ROLLBACK")
            tx_started = False
            return UiRenderEnqueueResult(enqueued=False, reason="not_found")

        if guard.not_allowed:
            conn.execute("ROLLBACK")
            tx_started = False
            return UiRenderEnqueueResult(enqueued=False, reason="not_allowed")

        if guard.already_in_progress:
            conn.execute("ROLLBACK")
            tx_started = False
            return UiRenderEnqueueResult(enqueued=False, reason="already_in_progress")

        for idx, track in enumerate(tracks):
            file_id = str(track.get("file_id") or "")
            filename = str(track.get("filename") or "")
            aid = dbm.create_asset(
                conn,
                channel_id=channel_id,
                kind="AUDIO",
                origin="GDRIVE",
                origin_id=file_id,
                name=filename,
                path=f"gdrive:{file_id}",
            )
            dbm.link_job_input(conn, job_id, aid, "TRACK", idx)

        bg_aid = dbm.create_asset(
            conn,
            channel_id=channel_id,
            kind="IMAGE",
            origin="GDRIVE",
            origin_id=background_file_id,
            name=background_filename,
            path=f"gdrive:{background_file_id}",
        )
        dbm.link_job_input(conn, job_id, bg_aid, "BACKGROUND", 0)

        if cover_file_id:
            cover_aid = dbm.create_asset(
                conn,
                channel_id=channel_id,
                kind="IMAGE",
                origin="GDRIVE",
                origin_id=cover_file_id,
                name=cover_filename,
                path=f"gdrive:{cover_file_id}",
            )
            dbm.link_job_input(conn, job_id, cover_aid, "COVER", 0)

        dbm.update_job_state(conn, job_id, state="READY_FOR_RENDER", stage="FETCH", error_reason="")

        conn.execute("COMMIT")
        tx_started = False
        return UiRenderEnqueueResult(enqueued=True, reason="enqueued")
    except Exception:
        if tx_started:
            conn.execute("ROLLBACK")
        raise
