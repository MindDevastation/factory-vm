from __future__ import annotations

import os
import subprocess
from pathlib import Path

from services.common.env import Env
from services.common import db as dbm
from services.common.config import load_channels, load_render_profiles
from services.common.paths import outbox_dir
from services.workers.qa import qa_cycle


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"command failed ({p.returncode}): {' '.join(cmd)}\n{p.stdout}")


def _ensure_ffmpeg() -> None:
    _run(["ffmpeg", "-version"])


def _seed(conn) -> None:
    # channels
    for c in load_channels("configs/channels.yaml"):
        conn.execute(
            """
            INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET
                display_name=excluded.display_name,
                kind=excluded.kind,
                weight=excluded.weight,
                render_profile=excluded.render_profile,
                autopublish_enabled=excluded.autopublish_enabled
            """,
            (c.slug, c.display_name, c.kind, c.weight, c.render_profile, 1 if c.autopublish_enabled else 0),
        )

    # render profiles
    for p in load_render_profiles("configs/render_profiles.yaml"):
        conn.execute(
            """
            INSERT INTO render_profiles(name, video_w, video_h, fps, vcodec_required, audio_sr, audio_ch, acodec_required)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                video_w=excluded.video_w,
                video_h=excluded.video_h,
                fps=excluded.fps,
                vcodec_required=excluded.vcodec_required,
                audio_sr=excluded.audio_sr,
                audio_ch=excluded.audio_ch,
                acodec_required=excluded.acodec_required
            """,
            (p.name, p.video_w, p.video_h, p.fps, p.vcodec_required, p.audio_sr, p.audio_ch, p.acodec_required),
        )


def main() -> None:
    env = Env.load()

    if env.basic_pass == "change_me":
        raise RuntimeError("FACTORY_BASIC_AUTH_PASS is not set (default 'change_me' is insecure).")

    _ensure_ffmpeg()

    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        _seed(conn)

        ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
        if not ch:
            raise RuntimeError("channel 'darkwood-reverie' not found after seeding")

        ts = dbm.now_ts()
        cur = conn.execute(
            """INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
            (int(ch["id"]), "Selftest Release", "desc", dbm.json_dumps(["a", "b"]), None, None, f"selftest_meta_{int(ts)}", ts),
        )
        release_id = int(cur.lastrowid)

        cur2 = conn.execute(
            """INSERT INTO jobs(release_id, job_type, state, stage, priority, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)""",
            (release_id, "RENDER_LONG", "QA_RUNNING", "QA", 1, ts, ts),
        )
        job_id = int(cur2.lastrowid)
    finally:
        conn.close()

    # generate a small compliant mp4
    mp4_dir = outbox_dir(env, job_id)
    mp4_dir.mkdir(parents=True, exist_ok=True)
    mp4 = mp4_dir / "render.mp4"

    _run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=1920x1080:r=24",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "10",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-profile:v",
            "high",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "384k",
            "-ac",
            "2",
            "-ar",
            "48000",
            "-movflags",
            "+faststart",
            str(mp4),
        ]
    )

    # run QA cycle
    qa_cycle(env=env, worker_id="selftest")

    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        if not job:
            raise RuntimeError("job disappeared")
        state = job["state"]
    finally:
        conn.close()

    if state != "UPLOADING":
        raise RuntimeError(f"selftest failed: expected state=UPLOADING, got {state}")

    print(f"OK: job {job_id} -> UPLOADING")


if __name__ == "__main__":
    # Run as: PYTHONPATH=. FACTORY_DB_PATH=... FACTORY_STORAGE_ROOT=... FACTORY_BASIC_AUTH_PASS=... python scripts/selftest_smoke.py
    main()
