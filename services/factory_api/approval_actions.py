from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from services.common import db as dbm
from services.playlist_builder.workflow import write_committed_history_for_published


def approve_job(conn: Any, *, job_id: int, comment: str) -> dict[str, Any]:
    job = dbm.get_job(conn, job_id)
    if not job:
        raise HTTPException(404)
    if str(job.get("state")) != "WAIT_APPROVAL":
        raise HTTPException(409, "job is not in WAIT_APPROVAL")
    normalized_comment = (comment or "approved").strip() or "approved"
    dbm.set_approval(conn, job_id, "APPROVE", normalized_comment)
    dbm.update_job_state(conn, job_id, state="APPROVED", stage="APPROVAL")
    return {"ok": True}


def reject_job(conn: Any, *, job_id: int, comment: str) -> dict[str, Any]:
    job = dbm.get_job(conn, job_id)
    if not job:
        raise HTTPException(404)
    if str(job.get("state")) != "WAIT_APPROVAL":
        raise HTTPException(409, "job is not in WAIT_APPROVAL")
    normalized_comment = str(comment or "").strip()
    dbm.set_approval(conn, job_id, "REJECT", normalized_comment)
    dbm.update_job_state(conn, job_id, state="REJECTED", stage="APPROVAL")
    return {"ok": True}


def mark_job_published(conn: Any, *, job_id: int) -> dict[str, Any]:
    job = dbm.get_job(conn, job_id)
    if not job:
        raise HTTPException(404)
    if str(job.get("state")) not in ("APPROVED", "WAIT_APPROVAL"):
        raise HTTPException(409, "job is not in APPROVED/WAIT_APPROVAL")

    ts = dbm.now_ts()
    delete_at = ts + 48 * 3600
    conn.execute("BEGIN IMMEDIATE")
    try:
        dbm.update_job_state(conn, job_id, state="PUBLISHED", stage="APPROVAL", published_at=ts, delete_mp4_at=delete_at)
        history_id = write_committed_history_for_published(conn, job_id=job_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {"ok": True, "delete_mp4_at": delete_at, "history_id": history_id}


__all__ = ["approve_job", "reject_job", "mark_job_published"]
