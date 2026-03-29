from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone
from typing import Any

from services.common import db as dbm
from services.common.env import Env
from services.common.logging_setup import get_logger
from services.common.paths import cancel_flag_path, outbox_dir
from services.common.youtube_token_resolver import (
    YouTubeTokenResolutionError,
    resolve_channel_token_path,
)
from services.factory_api.publish_audit_status import resolve_effective_audit_status
from services.factory_api.publish_policy import _resolve_effective_policy
from services.integrations.youtube import YouTubeClient
from services.publish_runtime.schedule import evaluate_publish_schedule


log = get_logger("uploader")


def _load_global_controls(conn: Any) -> dict[str, Any]:
    row = conn.execute(
        "SELECT auto_publish_paused, reason FROM publish_global_controls WHERE singleton_key = 1"
    ).fetchone()
    if not row:
        return {"auto_publish_paused": False, "reason": None}
    return {
        "auto_publish_paused": bool(row["auto_publish_paused"]),
        "reason": (str(row["reason"]) if row["reason"] is not None else None),
    }


def _initialize_publish_runtime_after_private_upload(conn: Any, *, job_id: int) -> None:
    job = dbm.get_job(conn, job_id)
    if not job:
        return

    now_ts = datetime.now(timezone.utc).timestamp()
    conn.execute(
        "UPDATE jobs SET publish_state = 'private_uploaded', publish_last_transition_at = ?, updated_at = ? WHERE id = ?",
        (now_ts, now_ts, job_id),
    )

    policy = _resolve_effective_policy(conn, release_id=int(job["release_id"]), channel_slug=str(job["channel_slug"]))
    audit = resolve_effective_audit_status(conn, channel_slug=str(job["channel_slug"]))
    controls = _load_global_controls(conn)

    rel = conn.execute("SELECT planned_at FROM releases WHERE id = ?", (int(job["release_id"]),)).fetchone()
    schedule = evaluate_publish_schedule(planned_at=(str(rel["planned_at"]) if rel and rel["planned_at"] is not None else None))

    effective_reason_code = policy["effective_reason_code"]
    if bool(job.get("publish_hold_active") or 0):
        effective_reason_code = str(job.get("publish_hold_reason_code") or "policy_requires_manual")
    elif controls["auto_publish_paused"]:
        effective_reason_code = "global_pause_active"

    decision_mode = str(policy["effective_publish_mode"])
    if bool(job.get("publish_hold_active") or 0) or controls["auto_publish_paused"]:
        decision_mode = "hold"
    if str(audit.get("effective_status") or "unknown") != "approved":
        decision_mode = "hold"
        effective_reason_code = "audit_not_approved"

    if decision_mode == "hold":
        final_publish_state = "policy_blocked"
    elif schedule.eligibility == "future":
        final_publish_state = "waiting_for_schedule"
    elif decision_mode == "manual_only":
        final_publish_state = "manual_handoff_pending"
    else:
        final_publish_state = "ready_to_publish"

    conn.execute(
        """
        UPDATE jobs
        SET publish_state = ?,
            publish_target_visibility = ?,
            publish_delivery_mode_effective = ?,
            publish_resolved_scope = ?,
            publish_reason_code = ?,
            publish_scheduled_at = ?,
            publish_last_transition_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            final_publish_state,
            policy["effective_target_visibility"],
            "automatic" if str(policy["effective_publish_mode"]) == "auto" else "manual",
            str(policy["resolved_scope"]),
            effective_reason_code,
            schedule.publish_scheduled_at_ts,
            now_ts,
            now_ts,
            job_id,
        ),
    )


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
            _initialize_publish_runtime_after_private_upload(conn, job_id=job_id)
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
            _initialize_publish_runtime_after_private_upload(conn, job_id=job_id)
            dbm.clear_retry(conn, job_id)
            dbm.release_lock(conn, job_id, worker_id)
            return

        # Real YouTube upload
        channel_slug = str(job.get("channel_slug") or "").strip()
        try:
            token_json = resolve_channel_token_path(channel_slug=channel_slug, tokens_dir=env.yt_tokens_dir)
            if not env.yt_client_secret_json:
                raise YouTubeTokenResolutionError("YT_CLIENT_SECRET_JSON is required for YouTube uploads")
            log.info(
                "resolved youtube credentials",
                extra={"job_id": int(job_id), "channel_slug": channel_slug, "token_path": token_json},
            )
            yt = YouTubeClient(client_secret_json=env.yt_client_secret_json, token_json=token_json)
        except YouTubeTokenResolutionError as e:
            msg = str(e)
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
        _initialize_publish_runtime_after_private_upload(conn, job_id=job_id)
        dbm.clear_retry(conn, job_id)
        dbm.release_lock(conn, job_id, worker_id)
    finally:
        conn.close()
