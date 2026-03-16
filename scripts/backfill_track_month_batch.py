from __future__ import annotations

import argparse

from services.common import db as dbm
from services.common.env import Env
from services.common.profile import load_profile_env
from services.track_analyzer.discover import discover_channel_tracks
from services.workers.track_jobs import _build_track_catalog_drive_client


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill tracks.month_batch by re-running discovery against Audio/<Batch>/ folders.")
    parser.add_argument("--channel-slug", required=True)
    parser.add_argument("--gdrive-root-id", default=None, help="Override library root id. Defaults to env GDRIVE_LIBRARY_ROOT_ID")
    args = parser.parse_args(argv)

    load_profile_env()
    env = Env.load()
    root_id = str(args.gdrive_root_id or env.gdrive_library_root_id or "").strip()
    if not root_id:
        raise RuntimeError("gdrive root id is required (set GDRIVE_LIBRARY_ROOT_ID or pass --gdrive-root-id)")

    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        drive = _build_track_catalog_drive_client(env=env, channel_slug=str(args.channel_slug))
        stats = discover_channel_tracks(conn, drive, gdrive_library_root_id=root_id, channel_slug=str(args.channel_slug))
        conn.commit()
    finally:
        conn.close()

    print(
        f"backfill complete channel={args.channel_slug} seen_wav={stats.seen_wav} inserted={stats.inserted} updated={stats.updated} renamed={stats.renamed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
