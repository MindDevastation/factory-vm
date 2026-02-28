from __future__ import annotations

import json
import sqlite3
import time
import re
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.common.env import Env


def _dict_factory(cursor: sqlite3.Cursor, row: Tuple[Any, ...]) -> Dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def connect(env: Env) -> sqlite3.Connection:
    Path(env.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(env.db_path, timeout=30, isolation_level=None)
    conn.row_factory = _dict_factory
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    _ensure_track_analyzer_schema_tables(conn)

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            youtube_channel_id TEXT UNIQUE,
            kind TEXT NOT NULL,
            weight REAL NOT NULL DEFAULT 1.0,
            render_profile TEXT NOT NULL,
            autopublish_enabled INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS render_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            video_w INTEGER NOT NULL,
            video_h INTEGER NOT NULL,
            fps REAL NOT NULL,
            vcodec_required TEXT NOT NULL,
            audio_sr INTEGER NOT NULL,
            audio_ch INTEGER NOT NULL,
            acodec_required TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            tags_json TEXT NOT NULL,
            planned_at TEXT,
            origin_release_folder_id TEXT,
            origin_meta_file_id TEXT UNIQUE,
            created_at REAL NOT NULL,
            FOREIGN KEY(channel_id) REFERENCES channels(id)
        );

        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            origin TEXT NOT NULL,
            origin_id TEXT,
            name TEXT,
            path TEXT,
            sha256 TEXT,
            duration_sec REAL,
            created_at REAL NOT NULL,
            FOREIGN KEY(channel_id) REFERENCES channels(id)
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            state TEXT NOT NULL,
            stage TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            attempt INTEGER NOT NULL DEFAULT 0,
            locked_by TEXT,
            locked_at REAL,
            retry_at REAL,
            progress_pct REAL NOT NULL DEFAULT 0.0,
            progress_text TEXT,
            progress_updated_at REAL,
            error_reason TEXT,
            approval_notified_at REAL,
            published_at REAL,
            delete_mp4_at REAL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY(release_id) REFERENCES releases(id)
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_state_priority ON jobs(state, priority, created_at);

        CREATE TABLE IF NOT EXISTS job_inputs (
            job_id INTEGER NOT NULL,
            asset_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            order_index INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(asset_id) REFERENCES assets(id)
        );

        CREATE TABLE IF NOT EXISTS job_outputs (
            job_id INTEGER NOT NULL,
            asset_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(asset_id) REFERENCES assets(id)
        );

        CREATE TABLE IF NOT EXISTS qa_reports (
            job_id INTEGER PRIMARY KEY,
            hard_ok INTEGER NOT NULL,
            warnings_json TEXT NOT NULL,
            info_json TEXT NOT NULL,
            duration_expected REAL,
            duration_actual REAL,
            vcodec TEXT,
            acodec TEXT,
            fps REAL,
            width INTEGER,
            height INTEGER,
            sr INTEGER,
            ch INTEGER,
            mean_volume_db REAL,
            max_volume_db REAL,
            created_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS approvals (
            job_id INTEGER PRIMARY KEY,
            decision TEXT NOT NULL,
            comment TEXT NOT NULL,
            decided_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS youtube_uploads (
            job_id INTEGER PRIMARY KEY,
            video_id TEXT NOT NULL,
            url TEXT NOT NULL,
            studio_url TEXT NOT NULL,
            privacy TEXT NOT NULL,
            uploaded_at REAL NOT NULL,
            error TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS tg_messages (
            job_id INTEGER PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            created_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS tg_pending (
            user_id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS worker_heartbeats (
            worker_id TEXT PRIMARY KEY,
            role TEXT NOT NULL,
            pid INTEGER NOT NULL,
            hostname TEXT NOT NULL,
            details_json TEXT NOT NULL,
            last_seen REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_last_seen ON worker_heartbeats(last_seen);

        CREATE TABLE IF NOT EXISTS ui_job_drafts (
            job_id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            tags_csv TEXT NOT NULL,
            cover_name TEXT,
            cover_ext TEXT,
            background_name TEXT NOT NULL,
            background_ext TEXT NOT NULL,
            audio_ids_text TEXT NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(channel_id) REFERENCES channels(id)
        );

        CREATE TABLE IF NOT EXISTS canon_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canon_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canon_forbidden (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canon_palettes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canon_thresholds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_slug TEXT NOT NULL,
            track_id TEXT NOT NULL,
            gdrive_file_id TEXT NOT NULL UNIQUE,
            source TEXT,
            filename TEXT,
            title TEXT,
            artist TEXT,
            duration_sec REAL,
            discovered_at REAL NOT NULL,
            analyzed_at REAL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_tracks_channel_slug_track_id
            ON tracks(channel_slug, track_id);

        CREATE TABLE IF NOT EXISTS track_features (
            track_pk INTEGER PRIMARY KEY,
            payload_json TEXT NOT NULL,
            computed_at REAL NOT NULL,
            FOREIGN KEY(track_pk) REFERENCES tracks(id)
        );

        CREATE TABLE IF NOT EXISTS track_tags (
            track_pk INTEGER PRIMARY KEY,
            payload_json TEXT NOT NULL,
            computed_at REAL NOT NULL,
            FOREIGN KEY(track_pk) REFERENCES tracks(id)
        );

        CREATE TABLE IF NOT EXISTS track_scores (
            track_pk INTEGER PRIMARY KEY,
            payload_json TEXT NOT NULL,
            computed_at REAL NOT NULL,
            FOREIGN KEY(track_pk) REFERENCES tracks(id)
        );

        CREATE TABLE IF NOT EXISTS track_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            channel_slug TEXT,
            status TEXT NOT NULL,
            payload_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_track_jobs_type_status
            ON track_jobs(job_type, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_track_jobs_channel
            ON track_jobs(job_type, channel_slug, status, created_at);

        CREATE TABLE IF NOT EXISTS track_job_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            level TEXT,
            message TEXT NOT NULL,
            ts REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES track_jobs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_track_job_logs
            ON track_job_logs(job_id, ts);
        """
    )

    # Backward-compatible additive migrations for older DBs (SQLite doesn't support IF NOT EXISTS for ADD COLUMN).
    _ensure_jobs_columns(conn)
    _ensure_channels_columns(conn)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return {str(r.get("name")) for r in rows if isinstance(r, dict) and r.get("name")}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _next_legacy_table_name(conn: sqlite3.Connection, table: str) -> str:
    base = f"{table}__legacy"
    if not _table_exists(conn, base):
        return base

    ts = int(time.time())
    name = f"{base}_{ts}"
    while _table_exists(conn, name):
        ts += 1
        name = f"{base}_{ts}"
    return name


def _rename_table_to_legacy(conn: sqlite3.Connection, table: str) -> None:
    new_name = _next_legacy_table_name(conn, table)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"invalid table name: {table}")
    conn.execute(f"ALTER TABLE {table} RENAME TO {new_name}")


def _ensure_track_analyzer_schema_tables(conn: sqlite3.Connection) -> None:
    expected = {
        "canon_channels": {"id", "value"},
        "canon_tags": {"id", "value"},
        "canon_forbidden": {"id", "value"},
        "canon_palettes": {"id", "value"},
        "canon_thresholds": {"id", "value"},
    }
    for table, expected_cols in expected.items():
        if not _table_exists(conn, table):
            continue
        if _table_columns(conn, table) != expected_cols:
            _rename_table_to_legacy(conn, table)


def _ensure_jobs_columns(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "jobs")
    if "retry_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN retry_at REAL;")

    # These columns were added after the initial MVP schema. For older DBs created before
    # progress/error/approval fields existed, we add them additively.
    if "progress_pct" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN progress_pct REAL NOT NULL DEFAULT 0.0;")
    if "progress_text" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN progress_text TEXT;")
    if "progress_updated_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN progress_updated_at REAL;")
    if "error_reason" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN error_reason TEXT;")
    if "approval_notified_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN approval_notified_at REAL;")
    if "published_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN published_at REAL;")
    if "delete_mp4_at" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE jobs ADD COLUMN delete_mp4_at REAL;")

    # Create index only after ensuring the column exists.
    with suppress(Exception):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_state_retry ON jobs(state, retry_at, priority, created_at);")


def _ensure_channels_columns(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "channels")
    if "youtube_channel_id" not in cols:
        with suppress(Exception):
            conn.execute("ALTER TABLE channels ADD COLUMN youtube_channel_id TEXT;")

    with suppress(Exception):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_youtube_channel_id_unique
            ON channels(youtube_channel_id)
            WHERE youtube_channel_id IS NOT NULL;
            """
        )


def now_ts() -> float:
    return time.time()


def json_dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def get_channel_by_slug(conn: sqlite3.Connection, slug: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM channels WHERE slug = ?", (slug,))
    return cur.fetchone()


def get_channel_by_id(conn: sqlite3.Connection, channel_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM channels WHERE id = ?", (channel_id,))
    return cur.fetchone()


def get_channel_by_youtube_channel_id(conn: sqlite3.Connection, youtube_channel_id: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM channels WHERE youtube_channel_id = ?", (youtube_channel_id,))
    return cur.fetchone()


def create_channel(
    conn: sqlite3.Connection,
    *,
    slug: str,
    display_name: str,
    kind: str = "LONG",
    weight: float = 1.0,
    render_profile: str = "long_1080p24",
    autopublish_enabled: int = 0,
    youtube_channel_id: str | None = None,
) -> Dict[str, Any]:
    cur = conn.execute(
        """
        INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled, youtube_channel_id)
        VALUES(?,?,?,?,?,?,?)
        """,
        (slug, display_name, kind, weight, render_profile, autopublish_enabled, youtube_channel_id),
    )
    channel_id = int(cur.lastrowid)
    row = conn.execute(
        "SELECT id, slug, display_name, youtube_channel_id FROM channels WHERE id = ?",
        (channel_id,),
    ).fetchone()
    assert row is not None
    return row


def update_channel_display_name(
    conn: sqlite3.Connection,
    *,
    slug: str,
    display_name: str,
) -> Optional[Dict[str, Any]]:
    cols = _table_columns(conn, "channels")
    if "updated_at" in cols:
        conn.execute(
            "UPDATE channels SET display_name = ?, updated_at = ? WHERE slug = ?",
            (display_name, now_ts(), slug),
        )
    else:
        conn.execute(
            "UPDATE channels SET display_name = ? WHERE slug = ?",
            (display_name, slug),
        )
    return conn.execute(
        "SELECT id, slug, display_name FROM channels WHERE slug = ?",
        (slug,),
    ).fetchone()


def channel_has_jobs(conn: sqlite3.Connection, channel_id: int) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM jobs j
        JOIN releases r ON r.id = j.release_id
        WHERE r.channel_id = ?
        LIMIT 1
        """,
        (channel_id,),
    ).fetchone()
    return row is not None


def delete_channel_by_slug(conn: sqlite3.Connection, slug: str) -> int:
    cur = conn.execute("DELETE FROM channels WHERE slug = ?", (slug,))
    return int(cur.rowcount or 0)


def enable_track_catalog_for_channel(conn: sqlite3.Connection, channel_slug: str) -> None:
    conn.execute("INSERT OR IGNORE INTO canon_channels(value) VALUES(?)", (channel_slug,))
    conn.execute("INSERT OR IGNORE INTO canon_thresholds(value) VALUES(?)", (channel_slug,))


def disable_track_catalog_for_channel(conn: sqlite3.Connection, channel_slug: str) -> None:
    conn.execute("DELETE FROM canon_channels WHERE value = ?", (channel_slug,))
    conn.execute("DELETE FROM canon_thresholds WHERE value = ?", (channel_slug,))


def list_jobs(conn: sqlite3.Connection, state: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    if state:
        cur = conn.execute(
            """
            SELECT j.*, r.title AS release_title, c.slug AS channel_slug, c.display_name AS channel_name
            FROM jobs j
            JOIN releases r ON r.id = j.release_id
            JOIN channels c ON c.id = r.channel_id
            WHERE j.state = ?
            ORDER BY j.priority DESC, j.created_at ASC
            LIMIT ?
            """,
            (state, limit),
        )
    else:
        cur = conn.execute(
            """
            SELECT j.*, r.title AS release_title, c.slug AS channel_slug, c.display_name AS channel_name
            FROM jobs j
            JOIN releases r ON r.id = j.release_id
            JOIN channels c ON c.id = r.channel_id
            ORDER BY j.updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
    return cur.fetchall()


def get_job(conn: sqlite3.Connection, job_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT j.*, r.title AS release_title, r.description AS release_description, r.tags_json AS release_tags_json,
               r.channel_id AS channel_id,
               c.slug AS channel_slug, c.display_name AS channel_name, c.kind AS channel_kind, c.autopublish_enabled
        FROM jobs j
        JOIN releases r ON r.id = j.release_id
        JOIN channels c ON c.id = r.channel_id
        WHERE j.id = ?
        """,
        (job_id,),
    )
    return cur.fetchone()


def get_ui_job_draft(conn: sqlite3.Connection, job_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT d.*, c.display_name AS channel_name
        FROM ui_job_drafts d
        JOIN channels c ON c.id = d.channel_id
        WHERE d.job_id = ?
        """,
        (job_id,),
    )
    return cur.fetchone()


def create_ui_job_draft(
    conn: sqlite3.Connection,
    *,
    channel_id: int,
    title: str,
    description: str,
    tags_csv: str,
    cover_name: Optional[str],
    cover_ext: Optional[str],
    background_name: str,
    background_ext: str,
    audio_ids_text: str,
    job_type: str = "UI",
) -> int:
    ts = now_ts()
    tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
    cur = conn.execute(
        """
        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
        VALUES(?, ?, ?, ?, NULL, NULL, NULL, ?)
        """,
        (channel_id, title, description, json_dumps(tags), ts),
    )
    release_id = int(cur.lastrowid)
    cur2 = conn.execute(
        """
        INSERT INTO jobs(release_id, job_type, state, stage, priority, attempt, created_at, updated_at)
        VALUES(?, ?, 'DRAFT', 'DRAFT', 0, 0, ?, ?)
        """,
        (release_id, job_type, ts, ts),
    )
    job_id = int(cur2.lastrowid)
    conn.execute(
        """
        INSERT INTO ui_job_drafts(
            job_id, channel_id, title, description, tags_csv,
            cover_name, cover_ext, background_name, background_ext,
            audio_ids_text, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            channel_id,
            title,
            description,
            tags_csv,
            cover_name,
            cover_ext,
            background_name,
            background_ext,
            audio_ids_text,
            ts,
            ts,
        ),
    )
    return job_id


def update_ui_job_draft(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    title: str,
    description: str,
    tags_csv: str,
    cover_name: Optional[str],
    cover_ext: Optional[str],
    background_name: str,
    background_ext: str,
    audio_ids_text: str,
) -> None:
    ts = now_ts()
    tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
    conn.execute(
        """
        UPDATE ui_job_drafts
        SET title=?, description=?, tags_csv=?, cover_name=?, cover_ext=?,
            background_name=?, background_ext=?, audio_ids_text=?, updated_at=?
        WHERE job_id = ?
        """,
        (title, description, tags_csv, cover_name, cover_ext, background_name, background_ext, audio_ids_text, ts, job_id),
    )
    conn.execute(
        """
        UPDATE releases
        SET title=?, description=?, tags_json=?
        WHERE id = (SELECT release_id FROM jobs WHERE id = ?)
        """,
        (title, description, json_dumps(tags), job_id),
    )


def claim_job(
    conn: sqlite3.Connection,
    *,
    want_state: str,
    worker_id: str,
    lock_ttl_sec: int,
) -> Optional[int]:
    """Claim one job atomically.

    Rules:
      - only jobs in want_state
      - skip jobs scheduled for retry in the future (retry_at)
      - reclaim expired locks (locked_at older than lock_ttl_sec)
    """

    ts = now_ts()
    expiry = ts - float(lock_ttl_sec)

    conn.execute("BEGIN IMMEDIATE;")

    # Release expired locks in this state.
    conn.execute(
        """
        UPDATE jobs
        SET locked_by = NULL, locked_at = NULL, updated_at = ?
        WHERE state = ?
          AND locked_by IS NOT NULL
          AND locked_at IS NOT NULL
          AND locked_at < ?
        """,
        (ts, want_state, expiry),
    )

    row = conn.execute(
        """
        SELECT id FROM jobs
        WHERE state = ?
          AND locked_by IS NULL
          AND (retry_at IS NULL OR retry_at <= ?)
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        """,
        (want_state, ts),
    ).fetchone()
    if not row:
        conn.execute("COMMIT;")
        return None

    job_id = int(row["id"])
    cur = conn.execute(
        """
        UPDATE jobs
        SET locked_by = ?, locked_at = ?, updated_at = ?
        WHERE id = ? AND locked_by IS NULL
        """,
        (worker_id, ts, ts, job_id),
    )
    conn.execute("COMMIT;")
    if cur.rowcount != 1:
        return None
    return job_id


def touch_worker(
    conn: sqlite3.Connection,
    *,
    worker_id: str,
    role: str,
    pid: int,
    hostname: str,
    details: Dict[str, Any],
) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO worker_heartbeats(worker_id, role, pid, hostname, details_json, last_seen)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(worker_id) DO UPDATE SET
            role=excluded.role,
            pid=excluded.pid,
            hostname=excluded.hostname,
            details_json=excluded.details_json,
            last_seen=excluded.last_seen
        """,
        (worker_id, role, pid, hostname, json_dumps(details), ts),
    )


def list_workers(conn: sqlite3.Connection, limit: int = 200) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT worker_id, role, pid, hostname, details_json, last_seen
        FROM worker_heartbeats
        ORDER BY last_seen DESC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def increment_attempt(conn: sqlite3.Connection, job_id: int) -> int:
    ts = now_ts()
    conn.execute("UPDATE jobs SET attempt = attempt + 1, updated_at = ? WHERE id = ?", (ts, job_id))
    row = conn.execute("SELECT attempt FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return int(row["attempt"]) if row else 0


def schedule_retry(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    next_state: str,
    stage: str,
    error_reason: str,
    backoff_sec: int,
) -> None:
    ts = now_ts()
    retry_at = ts + float(backoff_sec)
    conn.execute(
        """
        UPDATE jobs
        SET state = ?, stage = ?, error_reason = ?, retry_at = ?, updated_at = ?, locked_by = NULL, locked_at = NULL
        WHERE id = ? AND state != 'CANCELLED'
        """,
        (next_state, stage, error_reason, retry_at, ts, job_id),
    )


def clear_retry(conn: sqlite3.Connection, job_id: int) -> None:
    ts = now_ts()
    conn.execute("UPDATE jobs SET retry_at = NULL, updated_at = ? WHERE id = ?", (ts, job_id))


def reclaim_stale_render_jobs(
    conn: sqlite3.Connection,
    *,
    lock_ttl_sec: int,
    backoff_sec: int,
    max_attempts: int,
) -> int:
    """Recover jobs that were in-progress inside orchestrator (FETCHING_INPUTS/RENDERING)
    and got stuck due to worker crash.

    Returns number of reclaimed jobs.
    """

    ts = now_ts()
    expiry = ts - float(lock_ttl_sec)
    rows = conn.execute(
        """
        SELECT id, state, locked_by FROM jobs
        WHERE state IN ('FETCHING_INPUTS','RENDERING')
          AND locked_by IS NOT NULL
          AND locked_at IS NOT NULL
          AND locked_at < ?
        """,
        (expiry,),
    ).fetchall()

    reclaimed = 0
    for r in rows:
        job_id = int(r["id"])
        prev_state = str(r.get("state") or "")
        attempt = increment_attempt(conn, job_id)
        reason = f"reclaimed stale lock from {prev_state}"
        if attempt < max_attempts:
            schedule_retry(
                conn,
                job_id,
                next_state="READY_FOR_RENDER",
                stage="FETCH",
                error_reason=f"attempt={attempt} retry: {reason}",
                backoff_sec=backoff_sec,
            )
        else:
            update_job_state(
                conn,
                job_id,
                state="RENDER_FAILED",
                stage="RENDER",
                error_reason=f"attempt={attempt} terminal: {reason}",
            )
            clear_retry(conn, job_id)
            conn.execute(
                "UPDATE jobs SET locked_by=NULL, locked_at=NULL, updated_at=? WHERE id=?",
                (now_ts(), job_id),
            )
        reclaimed += 1

    return reclaimed




def force_unlock(conn: sqlite3.Connection, job_id: int) -> None:
    # Force unlock a job regardless of who holds the lock (admin action).
    ts = now_ts()
    conn.execute(
        "UPDATE jobs SET locked_by=NULL, locked_at=NULL, updated_at=? WHERE id=?",
        (ts, job_id),
    )


def cancel_job(conn: sqlite3.Connection, job_id: int, *, reason: str = 'cancelled by user') -> None:
    # Mark job as CANCELLED and clear lock/retry. Safe to call multiple times.
    ts = now_ts()
    conn.execute(
        '''
        UPDATE jobs
        SET state='CANCELLED', stage='CANCELLED',
            progress_text='cancelled',
            error_reason=?,
            retry_at=NULL,
            locked_by=NULL,
            locked_at=NULL,
            updated_at=?
        WHERE id=?
        ''',
        (reason, ts, job_id),
    )
def release_lock(conn: sqlite3.Connection, job_id: int, worker_id: str) -> None:
    ts = now_ts()
    conn.execute(
        """
        UPDATE jobs SET locked_by = NULL, locked_at = NULL, updated_at = ?
        WHERE id = ? AND locked_by = ?
        """,
        (ts, job_id, worker_id),
    )


def update_job_state(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    state: str,
    stage: Optional[str] = None,
    error_reason: Optional[str] = None,
    progress_pct: Optional[float] = None,
    progress_text: Optional[str] = None,
    approval_notified_at: Optional[float] = None,
    published_at: Optional[float] = None,
    delete_mp4_at: Optional[float] = None,
) -> None:
    ts = now_ts()
    fields: List[str] = ["state = ?", "updated_at = ?"]
    vals: List[Any] = [state, ts]

    if stage is not None:
        fields.append("stage = ?")
        vals.append(stage)
    if error_reason is not None:
        fields.append("error_reason = ?")
        vals.append(error_reason)
    if progress_pct is not None:
        fields.append("progress_pct = ?")
        vals.append(progress_pct)
        fields.append("progress_updated_at = ?")
        vals.append(ts)
    if progress_text is not None:
        fields.append("progress_text = ?")
        vals.append(progress_text)
    if approval_notified_at is not None:
        fields.append("approval_notified_at = ?")
        vals.append(approval_notified_at)
    if published_at is not None:
        fields.append("published_at = ?")
        vals.append(published_at)
    if delete_mp4_at is not None:
        fields.append("delete_mp4_at = ?")
        vals.append(delete_mp4_at)

    where = " WHERE id = ?"
    if state != 'CANCELLED':
        where = " WHERE id = ? AND state != 'CANCELLED'"
    q = "UPDATE jobs SET " + ", ".join(fields) + where
    vals.append(job_id)
    conn.execute(q, tuple(vals))


def create_asset(
    conn: sqlite3.Connection,
    *,
    channel_id: int,
    kind: str,
    origin: str,
    origin_id: Optional[str],
    name: Optional[str],
    path: Optional[str],
) -> int:
    ts = now_ts()
    cur = conn.execute(
        """
        INSERT INTO assets(channel_id, kind, origin, origin_id, name, path, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (channel_id, kind, origin, origin_id, name, path, ts),
    )
    return int(cur.lastrowid)


def link_job_input(conn: sqlite3.Connection, job_id: int, asset_id: int, role: str, order_index: int) -> None:
    conn.execute(
        "INSERT INTO job_inputs(job_id, asset_id, role, order_index) VALUES(?, ?, ?, ?)",
        (job_id, asset_id, role, order_index),
    )


def link_job_output(conn: sqlite3.Connection, job_id: int, asset_id: int, role: str) -> None:
    conn.execute(
        "INSERT INTO job_outputs(job_id, asset_id, role) VALUES(?, ?, ?)",
        (job_id, asset_id, role),
    )


def set_qa_report(conn: sqlite3.Connection, job_id: int, report: Dict[str, Any]) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO qa_reports(
            job_id, hard_ok, warnings_json, info_json, duration_expected, duration_actual,
            vcodec, acodec, fps, width, height, sr, ch, mean_volume_db, max_volume_db, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            hard_ok=excluded.hard_ok,
            warnings_json=excluded.warnings_json,
            info_json=excluded.info_json,
            duration_expected=excluded.duration_expected,
            duration_actual=excluded.duration_actual,
            vcodec=excluded.vcodec,
            acodec=excluded.acodec,
            fps=excluded.fps,
            width=excluded.width,
            height=excluded.height,
            sr=excluded.sr,
            ch=excluded.ch,
            mean_volume_db=excluded.mean_volume_db,
            max_volume_db=excluded.max_volume_db,
            created_at=excluded.created_at
        """,
        (
            job_id,
            1 if report.get("hard_ok") else 0,
            json_dumps(report.get("warnings", [])),
            json_dumps(report.get("info", [])),
            report.get("duration_expected"),
            report.get("duration_actual"),
            report.get("vcodec"),
            report.get("acodec"),
            report.get("fps"),
            report.get("width"),
            report.get("height"),
            report.get("sr"),
            report.get("ch"),
            report.get("mean_volume_db"),
            report.get("max_volume_db"),
            ts,
        ),
    )


def set_approval(conn: sqlite3.Connection, job_id: int, decision: str, comment: str) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO approvals(job_id, decision, comment, decided_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            decision=excluded.decision,
            comment=excluded.comment,
            decided_at=excluded.decided_at
        """,
        (job_id, decision, comment, ts),
    )


def set_youtube_upload(conn: sqlite3.Connection, job_id: int, *, video_id: str, url: str, studio_url: str, privacy: str) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO youtube_uploads(job_id, video_id, url, studio_url, privacy, uploaded_at)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            video_id=excluded.video_id,
            url=excluded.url,
            studio_url=excluded.studio_url,
            privacy=excluded.privacy,
            uploaded_at=excluded.uploaded_at,
            error=NULL
        """,
        (job_id, video_id, url, studio_url, privacy, ts),
    )


def set_youtube_error(conn: sqlite3.Connection, job_id: int, error: str) -> None:
    conn.execute(
        """
        INSERT INTO youtube_uploads(job_id, video_id, url, studio_url, privacy, uploaded_at, error)
        VALUES(?, '', '', '', '', ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET error=excluded.error
        """,
        (job_id, now_ts(), error),
    )


def upsert_tg_message(conn: sqlite3.Connection, job_id: int, chat_id: int, message_id: int) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO tg_messages(job_id, chat_id, message_id, created_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            chat_id=excluded.chat_id,
            message_id=excluded.message_id,
            created_at=excluded.created_at
        """,
        (job_id, chat_id, message_id, ts),
    )


def set_pending_reply(conn: sqlite3.Connection, user_id: int, job_id: int, kind: str) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO tg_pending(user_id, job_id, kind, created_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            job_id=excluded.job_id,
            kind=excluded.kind,
            created_at=excluded.created_at
        """,
        (user_id, job_id, kind, ts),
    )


def pop_pending_reply(conn: sqlite3.Connection, user_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM tg_pending WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        return None
    conn.execute("DELETE FROM tg_pending WHERE user_id = ?", (user_id,))
    return row
