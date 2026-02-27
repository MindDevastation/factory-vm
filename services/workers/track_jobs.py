from __future__ import annotations

import os
import socket

from services.common import db as dbm
from services.common.env import Env
from services.common.logging_setup import get_logger
from services.integrations.gdrive import DriveClient
from services.track_analyzer.analyze import analyze_tracks
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
            elif job_type in {"TRACK_ANALYZE", "ANALYZE_TRACKS"}:
                if not channel_slug:
                    raise ValueError("channel_slug is required")
                _run_track_analyze(conn, env=env, job=job, job_id=job_id, channel_slug=channel_slug)
            else:
                raise ValueError(f"unsupported track job type: {job_type}")

            tjdb.update_progress(conn, job_id=job_id, processed_count=1, total_count=1, last_message="job completed")
            tjdb.finish_job(conn, job_id=job_id, status="DONE", last_message="DONE")
            tjdb.append_log(conn, job_id=job_id, level="INFO", message="job finished status=DONE")
        except Exception as e:
            safe_error = _sanitize_error_message(env, e)
            msg = f"job failed: {safe_error}"
            tjdb.update_progress(conn, job_id=job_id, processed_count=0, total_count=1, last_message=msg)
            tjdb.finish_job(conn, job_id=job_id, status="FAILED", last_message=msg)
            tjdb.append_log(conn, job_id=job_id, level="ERROR", message=msg)
            log.error("track_jobs_cycle failed: job_id=%s type=%s channel=%s err=%s", job_id, job_type, channel_slug, safe_error)
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


def _run_track_analyze(conn, *, env: Env, job: dict, job_id: int, channel_slug: str) -> None:
    payload = dbm.json_loads(job.get("payload_json") or "{}") if isinstance(job.get("payload_json"), str) else {}
    raw_scope = payload.get("scope")
    raw_force = payload.get("force")
    raw_max_tracks = payload.get("max_tracks")

    scope = str(raw_scope or "pending")
    force = bool(raw_force)
    try:
        max_tracks = int(raw_max_tracks)
    except Exception:
        max_tracks = 200
    if max_tracks <= 0:
        max_tracks = 200

    total_count = _count_analyze_candidates(conn, channel_slug=channel_slug, scope=scope, force=force, max_tracks=max_tracks)
    tjdb.update_progress(conn, job_id=job_id, processed_count=0, total_count=total_count, last_message="analyze started")
    tjdb.append_log(
        conn,
        job_id=job_id,
        level="INFO",
        message=f"analyze started channel={channel_slug} scope={scope} force={force} max_tracks={max_tracks}",
    )

    drive = DriveClient(
        service_account_json=env.gdrive_sa_json,
        oauth_client_json=env.gdrive_oauth_client_json,
        oauth_token_json=env.gdrive_oauth_token_json,
    )
    stats = analyze_tracks(
        conn,
        drive,
        channel_slug=channel_slug,
        storage_root=env.storage_root,
        job_id=job_id,
        scope=scope,
        force=force,
        max_tracks=max_tracks,
    )

    for processed in range(1, stats.processed + 1):
        tjdb.update_progress(
            conn,
            job_id=job_id,
            processed_count=processed,
            total_count=stats.selected,
            last_message=f"analyze progress {processed}/{stats.selected}",
        )

    msg = (
        f"analyze done channel={channel_slug} selected={stats.selected} "
        f"processed={stats.processed} failed={stats.failed}"
    )
    level = "WARN" if stats.failed else "INFO"
    tjdb.append_log(conn, job_id=job_id, level=level, message=msg)
    tjdb.update_progress(conn, job_id=job_id, processed_count=stats.processed, total_count=stats.selected, last_message=msg)


def _count_analyze_candidates(conn, *, channel_slug: str, scope: str, force: bool, max_tracks: int) -> int:
    normalized_scope = scope.strip().lower()
    where = ["channel_slug = ?"]
    args: list[object] = [channel_slug]
    if normalized_scope == "pending" and not force:
        where.append("analyzed_at IS NULL")

    args.append(max_tracks)
    row = conn.execute(
        f"""
        SELECT COUNT(1) AS cnt
        FROM (
          SELECT id
          FROM tracks
          WHERE {' AND '.join(where)}
          ORDER BY id ASC
          LIMIT ?
        )
        """,
        tuple(args),
    ).fetchone()
    return int((row or {}).get("cnt") or 0)


def _sanitize_error_message(env: Env, err: Exception) -> str:
    message = " ".join(str(err).split())
    if not message:
        message = err.__class__.__name__

    secret_values = [
        env.basic_pass,
        env.oauth_state_secret,
        env.gdrive_sa_json,
        env.gdrive_client_secret_json,
        env.gdrive_oauth_client_json,
        env.gdrive_oauth_token_json,
        env.yt_client_secret_json,
    ]
    for value in secret_values:
        if value:
            message = message.replace(value, "***")

    if len(message) > 300:
        message = f"{message[:300]}..."
    return message
