from __future__ import annotations

import shutil
import os
import socket
from pathlib import Path

from services.common.env import Env
from services.common import db as dbm
from services.common.paths import outbox_dir, workspace_dir, preview_path
from services.common.logging_setup import get_logger


log = get_logger("cleanup")


def cleanup_published_artifacts(conn, *, storage_root: str, job_id: int) -> dict[str, object]:
    job = dbm.get_job(conn, job_id)
    if not job:
        return {"ok": False, "reason": "job_not_found"}

    state = str(job.get("state") or "")
    if state not in {"PUBLISHED", "CLEANED"}:
        return {"ok": False, "reason": f"state_not_cleanup_eligible:{state}"}

    rows = conn.execute(
        """
        SELECT a.id, jo.role, a.path
        FROM job_outputs jo
        JOIN assets a ON a.id = jo.asset_id
        WHERE jo.job_id = ? AND jo.role IN ('MP4', 'PREVIEW_60S')
        ORDER BY a.id ASC
        """,
        (int(job_id),),
    ).fetchall()
    if not rows:
        return {"ok": False, "reason": "no_cleanup_target"}

    removed = 0
    for row in rows:
        raw = str(row.get("path") or "").strip()
        if not raw:
            continue
        p = Path(raw)
        if p.exists():
            p.unlink(missing_ok=True)
            removed += 1

    dbm.update_job_state(conn, int(job_id), state="CLEANED", stage="CLEANUP", progress_text="mp4 deleted (forced)")
    return {"ok": True, "cleaned": True, "removed_files": removed}


def cleanup_cycle(*, env: Env, worker_id: str) -> None:
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)

        dbm.touch_worker(
            conn,
            worker_id=worker_id,
            role="cleanup",
            pid=os.getpid(),
            hostname=socket.gethostname(),
            details={"state": "running"},
        )
        ts = dbm.now_ts()

        # Always remove leftover workspaces for non-rendering jobs
        rows = conn.execute(
            "SELECT id, state FROM jobs WHERE state NOT IN ('RENDERING','FETCHING_INPUTS')"
        ).fetchall()
        for r in rows:
            ws = workspace_dir(env, int(r["id"]))
            if ws.exists():
                shutil.rmtree(ws, ignore_errors=True)

        # Delete MP4 after delete_mp4_at
        due = conn.execute(
            "SELECT id FROM jobs WHERE state = 'PUBLISHED' AND delete_mp4_at IS NOT NULL AND delete_mp4_at <= ?",
            (ts,),
        ).fetchall()
        for r in due:
            job_id = int(r["id"])
            ob = outbox_dir(env, job_id)
            mp4 = ob / "render.mp4"
            if mp4.exists():
                mp4.unlink(missing_ok=True)
            pv = preview_path(env, job_id)
            if pv.exists():
                pv.unlink(missing_ok=True)
            # keep QA/logs/youtube links; mark cleaned
            dbm.update_job_state(conn, job_id, state="CLEANED", stage="CLEANUP", progress_text="mp4 deleted")
        log.info("cleanup_cycle done")

    finally:
        conn.close()
