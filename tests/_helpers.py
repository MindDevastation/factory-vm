from __future__ import annotations

import base64
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Tuple

from services.common.env import Env
from services.common import db as dbm
from services.common.config import load_render_profiles


def basic_auth_header(user: str, pwd: str) -> dict:
    token = base64.b64encode(f"{user}:{pwd}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


@contextmanager
def temp_env() -> Iterator[Tuple[tempfile.TemporaryDirectory, Env]]:
    """Create isolated temp DB/storage and return (tempdir, Env).

    Also sets minimal env vars required by the app.
    """
    td = tempfile.TemporaryDirectory()

    # Keep original env and restore on exit.
    old = os.environ.copy()
    try:
        os.environ["FACTORY_DB_PATH"] = str(Path(td.name) / "db.sqlite3")
        os.environ["FACTORY_STORAGE_ROOT"] = str(Path(td.name) / "storage")
        os.environ["FACTORY_BASIC_AUTH_USER"] = "admin"
        os.environ["FACTORY_BASIC_AUTH_PASS"] = "testpass"

        # disable external integrations by default
        os.environ["ORIGIN_BACKEND"] = "local"
        os.environ["UPLOAD_BACKEND"] = "mock"
        os.environ["TELEGRAM_ENABLED"] = "0"
        os.environ["TG_ADMIN_CHAT_ID"] = "0"

        env = Env.load()
        yield td, env
    finally:
        os.environ.clear()
        os.environ.update(old)
        td.cleanup()


def seed_minimal_db(env: Env) -> None:
    """Create schema + seed channels and render profiles from configs/*.yaml."""
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        # seed render profiles
        for rp in load_render_profiles("configs/render_profiles.yaml"):
            conn.execute(
                "INSERT OR IGNORE INTO render_profiles(name, video_w, video_h, fps, vcodec_required, audio_sr, audio_ch, acodec_required) VALUES(?,?,?,?,?,?,?,?)",
                (rp.name, rp.video_w, rp.video_h, rp.fps, rp.vcodec_required, rp.audio_sr, rp.audio_ch, rp.acodec_required),
            )

        # seed channels (runtime source of truth is DB; channels.yaml stays seed-only)
        channels_seed = [
            ("darkwood-reverie", "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
            ("channel-b", "Channel B", "LONG", 1.0, "long_1080p24", 0),
            ("channel-c", "Channel C", "LONG", 1.0, "long_1080p24", 0),
            ("channel-d", "Channel D", "LONG", 1.0, "long_1080p24", 0),
            ("titanwave-sonic", "TitanWave Sonic", "TITANWAVE", 0.0, "titanwave_1080p24", 0),
        ]
        for ch in channels_seed:
            conn.execute(
                "INSERT OR IGNORE INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                ch,
            )
    finally:
        conn.close()


def insert_release_and_job(
    env: Env,
    *,
    channel_slug: str = "darkwood-reverie",
    title: str = "Test Release",
    state: str = "READY_FOR_RENDER",
    stage: str = "FETCH",
    job_type: str = "RENDER_LONG",
) -> int:
    """Insert a release+job and return job_id."""
    conn = dbm.connect(env)
    try:
        ch = dbm.get_channel_by_slug(conn, channel_slug)
        assert ch, f"channel {channel_slug} not seeded"
        ts = dbm.now_ts()
        # origin_meta_file_id must be unique; use high-resolution timestamp.
        meta_id = f"meta_{time.time_ns()}"
        cur = conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(ch["id"]), title, "desc", "[]", None, None, meta_id, ts),
        )
        release_id = int(cur.lastrowid)
        cur2 = conn.execute(
            "INSERT INTO jobs(release_id, job_type, state, stage, priority, attempt, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (release_id, job_type, state, stage, 1, 0, ts, ts),
        )
        return int(cur2.lastrowid)
    finally:
        conn.close()


def add_local_inputs_for_job(env: Env, job_id: int, *, tracks: int = 1) -> Path:
    """Create local files (wav + cover) and attach them as job_inputs.

    Returns: folder containing created files.
    """
    base = Path(env.storage_root) / "test_inputs" / f"job_{job_id}"
    base.mkdir(parents=True, exist_ok=True)

    wavs = []
    for i in range(1, tracks + 1):
        p = base / f"track_{i}.wav"
        p.write_bytes(b"RIFF0000WAVEfmt ")  # minimal placeholder
        wavs.append(p)

    cover = base / "cover.png"
    cover.write_bytes(b"\x89PNG\r\n\x1a\n")

    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        assert job
        ch_slug = str(job["channel_slug"])
        ch = dbm.get_channel_by_slug(conn, ch_slug)
        assert ch
        ch_id = int(ch["id"])

        order = 0
        for w in wavs:
            aid = dbm.create_asset(conn, channel_id=ch_id, kind="AUDIO", origin="LOCAL", origin_id=str(w), name=w.name, path=str(w))
            dbm.link_job_input(conn, job_id, aid, "TRACK", order)
            order += 1

        cid = dbm.create_asset(conn, channel_id=ch_id, kind="IMAGE", origin="LOCAL", origin_id=str(cover), name=cover.name, path=str(cover))
        dbm.link_job_input(conn, job_id, cid, "COVER", 0)
    finally:
        conn.close()

    return base
