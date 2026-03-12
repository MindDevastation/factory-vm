from __future__ import annotations

import argparse
import json

from services.common import db as dbm
from services.common.env import Env
from services.common.profile import load_profile_env
from services.playlist_builder.core import PlaylistBuilder, resolve_effective_brief_for_job


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="playlist-builder", description="Playlist Builder preview CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("preview", help="Build preview for a job")
    p.add_argument("--job-id", type=int, required=True)
    p.add_argument("--override-json", type=str, default="{}")
    return parser.parse_args(argv)


def run_preview(*, job_id: int, override_json: str) -> dict:
    load_profile_env()
    env = Env.load()
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        overrides = json.loads(override_json or "{}")
        if not isinstance(overrides, dict):
            raise ValueError("override-json must be a JSON object")
        brief = resolve_effective_brief_for_job(conn, job_id=job_id, request_override=overrides)
        preview = PlaylistBuilder().generate_preview(conn, brief)
        return {"brief": brief.to_api_dict(), "preview": preview.model_dump()}
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.command != "preview":
        raise ValueError("unsupported command")
    payload = run_preview(job_id=int(args.job_id), override_json=str(args.override_json))
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
