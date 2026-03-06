from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable

from services.common import db as dbm


class UiJobRetryError(Exception):
    """Base error for UI job retry service."""


class UiJobRetryNotFoundError(UiJobRetryError):
    """Raised when source UI job cannot be found."""


class UiJobRetryStatusError(UiJobRetryError):
    """Raised when source UI job is not FAILED."""


@dataclass(frozen=True)
class UiJobRetryResult:
    retry_job_id: int
    created: bool


EnqueueRetryChild = Callable[[sqlite3.Connection, int], None]


def retry_failed_ui_job(
    conn: sqlite3.Connection,
    *,
    source_job_id: int,
    enqueue_retry_child: EnqueueRetryChild,
) -> UiJobRetryResult:
    tx_started = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        tx_started = True

        source_job = conn.execute(
            """
            SELECT id, release_id, job_type, state, stage, priority, attempt, root_job_id, attempt_no
            FROM jobs
            WHERE id = ?
            """,
            (source_job_id,),
        ).fetchone()
        if not source_job or str(source_job.get("job_type") or "") != "UI":
            raise UiJobRetryNotFoundError(f"ui source job {source_job_id} not found")

        if str(source_job.get("state") or "") != "FAILED":
            raise UiJobRetryStatusError(f"ui source job {source_job_id} is not FAILED")

        ts = dbm.now_ts()
        root_job_id = int(source_job.get("root_job_id") or source_job_id)
        attempt_no = int(source_job.get("attempt_no") or 1) + 1
        retry_job_id: int
        created = False

        try:
            cur = conn.execute(
                """
                INSERT INTO jobs(
                    release_id, job_type, state, stage, priority, attempt,
                    retry_of_job_id, root_job_id, attempt_no, force_refetch_inputs,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    int(source_job["release_id"]),
                    "UI",
                    "DRAFT",
                    "DRAFT",
                    int(source_job["priority"]),
                    int(source_job["attempt"]),
                    source_job_id,
                    root_job_id,
                    attempt_no,
                    ts,
                    ts,
                ),
            )
            retry_job_id = int(cur.lastrowid)
            created = True
        except sqlite3.IntegrityError:
            existing = conn.execute(
                "SELECT id FROM jobs WHERE retry_of_job_id = ?",
                (source_job_id,),
            ).fetchone()
            if not existing:
                raise
            retry_job_id = int(existing["id"])

        if not created:
            conn.execute("COMMIT")
            tx_started = False
            return UiJobRetryResult(retry_job_id=retry_job_id, created=False)

        source_draft = conn.execute(
            """
            SELECT channel_id, title, description, tags_csv, cover_name, cover_ext,
                   background_name, background_ext, audio_ids_text
            FROM ui_job_drafts
            WHERE job_id = ?
            """,
            (source_job_id,),
        ).fetchone()
        if not source_draft:
            raise UiJobRetryNotFoundError(f"ui source draft {source_job_id} not found")

        conn.execute(
            """
            INSERT INTO ui_job_drafts(
                job_id, channel_id, title, description, tags_csv,
                cover_name, cover_ext, background_name, background_ext,
                audio_ids_text, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                retry_job_id,
                int(source_draft["channel_id"]),
                str(source_draft["title"]),
                str(source_draft["description"]),
                str(source_draft["tags_csv"]),
                source_draft["cover_name"],
                source_draft["cover_ext"],
                str(source_draft["background_name"]),
                str(source_draft["background_ext"]),
                str(source_draft["audio_ids_text"]),
                ts,
                ts,
            ),
        )

        enqueue_retry_child(conn, retry_job_id)

        conn.execute("COMMIT")
        tx_started = False
        return UiJobRetryResult(retry_job_id=retry_job_id, created=True)
    except Exception:
        if tx_started:
            conn.execute("ROLLBACK")
        raise
