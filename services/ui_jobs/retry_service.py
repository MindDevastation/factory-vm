from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable

from services.common import db as dbm
from services.factory_api.ui_jobs_enqueue import _enqueue_ui_render_job_in_tx


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


def _load_source_enqueue_payload(conn: sqlite3.Connection, *, source_job_id: int) -> dict[str, object]:
    source_draft = conn.execute(
        """
        SELECT channel_id
        FROM ui_job_drafts
        WHERE job_id = ?
        """,
        (source_job_id,),
    ).fetchone()
    if not source_draft:
        raise UiJobRetryNotFoundError(f"ui source draft {source_job_id} not found")

    rows = conn.execute(
        """
        SELECT ji.role, ji.order_index, a.origin_id, a.name
        FROM job_inputs ji
        JOIN assets a ON a.id = ji.asset_id
        WHERE ji.job_id = ?
        ORDER BY ji.role ASC, ji.order_index ASC
        """,
        (source_job_id,),
    ).fetchall()
    if not rows:
        raise RuntimeError(f"ui source job {source_job_id} has no persisted inputs")

    tracks: list[dict[str, str]] = []
    background_file_id = ""
    background_filename = ""
    cover_file_id = ""
    cover_filename = ""

    for row in rows:
        role = str(row["role"])
        origin_id = str(row["origin_id"] or "")
        name = str(row["name"] or "")
        if role == "TRACK":
            tracks.append({"file_id": origin_id, "filename": name})
        elif role == "BACKGROUND":
            background_file_id = origin_id
            background_filename = name
        elif role == "COVER":
            cover_file_id = origin_id
            cover_filename = name

    if not tracks or not background_file_id:
        raise RuntimeError(f"ui source job {source_job_id} missing required persisted inputs")

    return {
        "channel_id": int(source_draft["channel_id"]),
        "tracks": tracks,
        "background_file_id": background_file_id,
        "background_filename": background_filename,
        "cover_file_id": cover_file_id,
        "cover_filename": cover_filename,
    }


def retry_failed_ui_job(
    conn: sqlite3.Connection,
    *,
    source_job_id: int,
    enqueue_retry_child: EnqueueRetryChild | None = None,
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
        enqueue_payload: dict[str, object] | None = None
        if enqueue_retry_child is None:
            enqueue_payload = _load_source_enqueue_payload(conn, source_job_id=source_job_id)

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

        if enqueue_retry_child is None:
            assert enqueue_payload is not None
            enqueue_result = _enqueue_ui_render_job_in_tx(
                conn,
                job_id=retry_job_id,
                channel_id=int(enqueue_payload["channel_id"]),
                tracks=list(enqueue_payload["tracks"]),
                background_file_id=str(enqueue_payload["background_file_id"]),
                background_filename=str(enqueue_payload["background_filename"]),
                cover_file_id=str(enqueue_payload["cover_file_id"]),
                cover_filename=str(enqueue_payload["cover_filename"]),
            )
            if not enqueue_result.enqueued:
                raise RuntimeError(
                    f"retry enqueue integration failed for child {retry_job_id}: {enqueue_result.reason}"
                )
        else:
            enqueue_retry_child(conn, retry_job_id)

        conn.execute("COMMIT")
        tx_started = False
        return UiJobRetryResult(retry_job_id=retry_job_id, created=True)
    except Exception:
        if tx_started:
            conn.execute("ROLLBACK")
        raise
