from __future__ import annotations

import os
import socket

from services.common import db as dbm
from services.common.env import Env
from services.common.logging_setup import get_logger
from services.integrations.gdrive import DriveClient
from services.track_analyzer.discover import discover_channel_tracks
from services.track_analyzer import track_jobs_db as tjdb


log = get_logger("track_jobs")


def track_jobs_cycle(*, env: Env, worker_id: str) -> None:
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        dbm.touch_worker(
            conn,
            worker_id=worker_id,
            role="track_jobs",
            pid=os.getpid(),
            hostname=socket.gethostname(),
            details={"library_root": env.gdrive_library_root_id},
        )

        job = tjdb.claim_queued_job(conn)
        if job is None:
            return

        job_id = int(job["id"])
        job_type = str(job.get("job_type") or "")
        channel_slug = str(job.get("channel_slug") or "").strip()

        tjdb.update_progress(conn, job_id=job_id, processed_count=0, total_count=1, last_message="job started")
        tjdb.append_log(conn, job_id=job_id, level="INFO", message=f"job claimed type={job_type}")

        try:
            if job_type in {"TRACK_DISCOVER", "SCAN_TRACKS"}:
                if not channel_slug:
                    raise ValueError("channel_slug is required")
                _run_track_discover(conn, env=env, job_id=job_id, channel_slug=channel_slug)
            else:
                raise ValueError(f"unsupported track job type: {job_type}")

            tjdb.update_progress(conn, job_id=job_id, processed_count=1, total_count=1, last_message="job completed")
            tjdb.finish_job(conn, job_id=job_id, status="DONE", last_message="DONE")
            tjdb.append_log(conn, job_id=job_id, level="INFO", message="job finished status=DONE")
        except Exception as e:
            msg = f"job failed: {e}"
            tjdb.update_progress(conn, job_id=job_id, processed_count=0, total_count=1, last_message=msg)
            tjdb.finish_job(conn, job_id=job_id, status="FAILED", last_message=msg)
            tjdb.append_log(conn, job_id=job_id, level="ERROR", message=msg)
            log.exception("track_jobs_cycle failed: job_id=%s type=%s channel=%s err=%s", job_id, job_type, channel_slug, e)
    finally:
        conn.close()


def _run_track_discover(conn, *, env: Env, job_id: int, channel_slug: str) -> None:
    tjdb.append_log(conn, job_id=job_id, level="INFO", message=f"discover started channel={channel_slug}")

    drive = DriveClient(
        service_account_json=env.gdrive_sa_json,
        oauth_client_json=env.gdrive_oauth_client_json,
        oauth_token_json=env.gdrive_oauth_token_json,
    )

    stats = discover_channel_tracks(
        conn,
        drive,
        gdrive_library_root_id=env.gdrive_library_root_id,
        channel_slug=channel_slug,
    )
    msg = (
        f"discover done channel={channel_slug} seen_wav={stats.seen_wav} "
        f"renamed={stats.renamed} inserted={stats.inserted} updated={stats.updated}"
    )
    level = "WARN" if stats.seen_wav == 0 else "INFO"
    tjdb.append_log(conn, job_id=job_id, level=level, message=msg)
    tjdb.update_progress(conn, job_id=job_id, processed_count=1, total_count=1, last_message=msg)
