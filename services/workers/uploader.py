from __future__ import annotations

import json
import os
import socket

from services.common.env import Env
from services.common import db as dbm
from services.common.paths import outbox_dir, cancel_flag_path
from services.common.logging_setup import get_logger
from services.common.youtube_credentials import (
    YouTubeCredentialResolutionError,
    resolve_youtube_channel_credentials,
)
from services.integrations.youtube import YouTubeClient


log = get_logger("uploader")


def uploader_cycle(*, env: Env, worker_id: str) -> None:
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)

        dbm.touch_worker(
            conn,
            worker_id=worker_id,
            role="uploader",
            pid=os.getpid(),
            hostname=socket.gethostname(),
            details={"upload_backend": env.upload_backend},
        )

        job_id = dbm.claim_job(conn, want_state="UPLOADING", worker_id=worker_id, lock_ttl_sec=env.job_lock_ttl_sec)
        if not job_id:
            return

        job = dbm.get_job(conn, job_id)
        if not job:
            dbm.release_lock(conn, job_id, worker_id)
            return

        if str(job.get("state") or "") == "CANCELLED":
            # Job was cancelled before upload started.
            dbm.release_lock(conn, job_id, worker_id)
            return

        log.info(
            "uploader claimed job",
            extra={
                "job_id": int(job_id),
                "state": str(job.get("state") or ""),
                "channel_slug": str(job.get("channel_slug") or ""),
                "channel_id": int(job.get("channel_id") or 0),
            },
        )

        try:
            if cancel_flag_path(env, job_id).exists():
                dbm.cancel_job(conn, job_id, reason="cancelled by user")
                dbm.release_lock(conn, job_id, worker_id)
                return
        except Exception:
            pass

            dbm.release_lock(conn, job_id, worker_id)
            return

        # Idempotency: if already uploaded, do not re-upload.
        existing = conn.execute("SELECT video_id, url, studio_url FROM youtube_uploads WHERE job_id = ?", (job_id,)).fetchone()
        if existing and existing.get("video_id"):
            dbm.update_job_state(conn, job_id, state="WAIT_APPROVAL", stage="APPROVAL", progress_text="already uploaded (private)")
            dbm.clear_retry(conn, job_id)
            dbm.release_lock(conn, job_id, worker_id)
            return

        mp4 = outbox_dir(env, job_id) / "render.mp4"
        if not mp4.exists():
            attempt = dbm.increment_attempt(conn, job_id)
            if attempt < env.max_upload_attempts:
                dbm.schedule_retry(conn, job_id, next_state="UPLOADING", stage="UPLOAD", error_reason="missing mp4", backoff_sec=env.retry_backoff_sec)
            else:
                dbm.update_job_state(conn, job_id, state="UPLOAD_FAILED", stage="UPLOAD", error_reason="missing mp4")
                dbm.clear_retry(conn, job_id)
                dbm.release_lock(conn, job_id, worker_id)
            return

        tags = json.loads(job["release_tags_json"] or "[]")

        if env.upload_backend == "mock":
            video_id = f"mock-{job_id}"
            url = f"file://{mp4.resolve()}"
            studio_url = ""
            dbm.set_youtube_upload(conn, job_id, video_id=video_id, url=url, studio_url=studio_url, privacy="private")
            dbm.update_job_state(conn, job_id, state="WAIT_APPROVAL", stage="APPROVAL", progress_text="mock uploaded")
            dbm.clear_retry(conn, job_id)
            dbm.release_lock(conn, job_id, worker_id)
            return

        # Real YouTube upload
        channel_slug = str(job.get("channel_slug") or "")
        try:
            client_secret_json, token_json, source_label = resolve_youtube_channel_credentials(
                channel_slug,
                conn=conn,
                global_client_secret_path=env.yt_client_secret_json,
                global_token_path=env.yt_token_json,
            )
            log.info(
                "resolved youtube credentials",
                extra={"job_id": int(job_id), "channel_slug": channel_slug, "source_label": source_label},
            )
            yt = YouTubeClient(client_secret_json=client_secret_json, token_json=token_json)
        except YouTubeCredentialResolutionError as e:
            msg = f"youtube credentials not configured for channel={channel_slug}: {e}"
            dbm.increment_attempt(conn, job_id)
            dbm.set_youtube_error(conn, job_id, msg)
            dbm.update_job_state(conn, job_id, state="UPLOAD_FAILED", stage="UPLOAD", error_reason=msg)
            dbm.clear_retry(conn, job_id)
            dbm.release_lock(conn, job_id, worker_id)
            return
        except Exception as e:
            msg = f"youtube client init failed for channel={channel_slug}: {e}"
            dbm.increment_attempt(conn, job_id)
            dbm.set_youtube_error(conn, job_id, msg)
            dbm.update_job_state(conn, job_id, state="UPLOAD_FAILED", stage="UPLOAD", error_reason=msg)
            dbm.clear_retry(conn, job_id)
            dbm.release_lock(conn, job_id, worker_id)
            return

        dbm.update_job_state(conn, job_id, state="UPLOADING", stage="UPLOAD", progress_text="uploading")
    finally:
        conn.close()

    # pre_upload_cancel_check
    conn2 = dbm.connect(env)
    try:
        j2 = dbm.get_job(conn2, job_id)
        if j2 and str(j2.get("state") or "") == "CANCELLED":
            dbm.release_lock(conn2, job_id, worker_id)
            return
    finally:
        conn2.close()

    try:
        res = yt.upload_private(video_path=mp4, title=job["release_title"], description=job["release_description"], tags=tags)
        video_id = res.video_id
        url = f"https://www.youtube.com/watch?v={video_id}"
        studio_url = f"https://studio.youtube.com/video/{video_id}/edit"
    except Exception as e:
        conn = dbm.connect(env)
        try:
            attempt = dbm.increment_attempt(conn, job_id)
            dbm.set_youtube_error(conn, job_id, str(e))
            if attempt < env.max_upload_attempts:
                dbm.schedule_retry(
                    conn,
                    job_id,
                    next_state="UPLOADING",
                    stage="UPLOAD",
                    error_reason=f"attempt={attempt} retry: {e}",
                    backoff_sec=env.retry_backoff_sec,
                )
            else:
                dbm.update_job_state(conn, job_id, state="UPLOAD_FAILED", stage="UPLOAD", error_reason=str(e))
                dbm.clear_retry(conn, job_id)
                dbm.release_lock(conn, job_id, worker_id)
        finally:
            conn.close()
        return

    # Optional thumbnail
    cover_dir = outbox_dir(env, job_id) / "cover"
    cover_files = list(cover_dir.glob("*"))
    if cover_files:
        try:
            yt.set_thumbnail(video_id=video_id, image_path=cover_files[0])
        except Exception:
            pass

    conn = dbm.connect(env)
    try:
        dbm.set_youtube_upload(conn, job_id, video_id=video_id, url=url, studio_url=studio_url, privacy="private")
        dbm.update_job_state(conn, job_id, state="WAIT_APPROVAL", stage="APPROVAL", progress_text="uploaded (private)")
        dbm.clear_retry(conn, job_id)
        dbm.release_lock(conn, job_id, worker_id)
    finally:
        conn.close()
