from __future__ import annotations

import os
from services.common.profile import load_profile_env

from services.common.env import Env
from services.common import db as dbm
from services.common.config import load_channels, load_render_profiles


def main() -> None:
    # configs/channels.yaml is seed-only. Runtime workers read channels from DB.
    load_profile_env()
    env = Env.load()
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)

        # channels
        channels = load_channels("configs/channels.yaml")
        for c in channels:
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
        profiles = load_render_profiles("configs/render_profiles.yaml")
        for p in profiles:
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

        print("Seeded channels + render_profiles.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
