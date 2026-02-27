from __future__ import annotations

from typing import Any, Dict, List, Optional
import sqlite3

from services.common import db as dbm

RUNNING_STATUSES = ("QUEUED", "RUNNING")
TERMINAL_STATUSES = ("DONE", "FAILED", "CANCELLED")


def _decode_payload(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, str) and raw:
        loaded = dbm.json_loads(raw)
        if isinstance(loaded, dict):
            return loaded
    return {}


def enqueue_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    channel_slug: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> int:
    ts = dbm.now_ts()
    payload_json = dbm.json_dumps(payload or {})
    cur = conn.execute(
        """
        INSERT INTO track_jobs(job_type, channel_slug, status, payload_json, created_at, updated_at)
        VALUES(?, ?, 'QUEUED', ?, ?, ?)
        """,
        (job_type, channel_slug, payload_json, ts, ts),
    )
    return int(cur.lastrowid)


def has_already_running(conn: sqlite3.Connection, *, job_type: str, channel_slug: Optional[str] = None) -> bool:
    if channel_slug is None:
        row = conn.execute(
            """
            SELECT 1 FROM track_jobs
            WHERE job_type = ?
              AND channel_slug IS NULL
              AND status IN ('QUEUED', 'RUNNING')
            LIMIT 1
            """,
            (job_type,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT 1 FROM track_jobs
            WHERE job_type = ?
              AND channel_slug = ?
              AND status IN ('QUEUED', 'RUNNING')
            LIMIT 1
            """,
            (job_type, channel_slug),
        ).fetchone()
    return row is not None


def get_job(conn: sqlite3.Connection, job_id: int) -> Optional[Dict[str, Any]]:
    return conn.execute("SELECT * FROM track_jobs WHERE id = ?", (job_id,)).fetchone()


def append_log(conn: sqlite3.Connection, *, job_id: int, message: str, level: Optional[str] = None) -> int:
    ts = dbm.now_ts()
    cur = conn.execute(
        "INSERT INTO track_job_logs(job_id, level, message, ts) VALUES(?, ?, ?, ?)",
        (job_id, level, message, ts),
    )
    return int(cur.lastrowid)


def list_logs(conn: sqlite3.Connection, *, job_id: int, tail: int = 200) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM track_job_logs
        WHERE job_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (job_id, tail),
    ).fetchall()
    return list(reversed(rows))


def claim_queued_job(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    conn.execute("BEGIN IMMEDIATE;")
    row = conn.execute(
        """
        SELECT id FROM track_jobs
        WHERE status = 'QUEUED'
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        conn.execute("COMMIT;")
        return None

    job_id = int(row["id"])
    ts = dbm.now_ts()
    conn.execute(
        "UPDATE track_jobs SET status = 'RUNNING', updated_at = ? WHERE id = ? AND status = 'QUEUED'",
        (ts, job_id),
    )
    claimed = get_job(conn, job_id)
    conn.execute("COMMIT;")
    return claimed


def update_progress(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    processed_count: Optional[int] = None,
    total_count: Optional[int] = None,
    last_message: Optional[str] = None,
) -> None:
    row = get_job(conn, job_id)
    if row is None:
        return

    payload = _decode_payload(row.get("payload_json"))
    if processed_count is not None:
        payload["processed_count"] = int(processed_count)
    if total_count is not None:
        payload["total_count"] = int(total_count)
    if last_message is not None:
        payload["last_message"] = last_message

    ts = dbm.now_ts()
    conn.execute(
        "UPDATE track_jobs SET payload_json = ?, updated_at = ? WHERE id = ?",
        (dbm.json_dumps(payload), ts, job_id),
    )


def finish_job(conn: sqlite3.Connection, *, job_id: int, status: str, last_message: Optional[str] = None) -> None:
    if status not in TERMINAL_STATUSES:
        raise ValueError("status must be one of DONE/FAILED/CANCELLED")

    row = get_job(conn, job_id)
    if row is None:
        return

    payload = _decode_payload(row.get("payload_json"))
    if last_message is not None:
        payload["last_message"] = last_message

    ts = dbm.now_ts()
    conn.execute(
        "UPDATE track_jobs SET status = ?, payload_json = ?, updated_at = ? WHERE id = ?",
        (status, dbm.json_dumps(payload), ts, job_id),
    )
