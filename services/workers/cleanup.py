from __future__ import annotations

import shutil
import os
import socket

from services.common.env import Env
from services.common import db as dbm
from services.common.paths import outbox_dir, workspace_dir, preview_path
from services.common.logging_setup import get_logger


log = get_logger("cleanup")


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
