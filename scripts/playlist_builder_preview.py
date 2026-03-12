from __future__ import annotations

import argparse
import json

from services.common import db as dbm
from services.common.env import Env
from services.common.profile import load_profile_env
from services.playlist_builder.workflow import PlaylistBuilderApiError, apply_preview, create_preview


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="playlist-builder", description="Playlist Builder preview CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("preview", help="Build preview for a job")
    p.add_argument("--job-id", type=int, required=True)
    p.add_argument("--override-json", type=str, default="{}")

    a = sub.add_parser("apply", help="Apply preview to a job")
    a.add_argument("--job-id", type=int, required=True)
    a.add_argument("--preview-id", type=str, required=True)
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
        envelope = create_preview(conn, job_id=job_id, override=overrides, created_by="cli")
        conn.commit()
        return {
            "ok": True,
            "job_id": int(job_id),
            "preview_id": envelope.preview_id,
            "brief": envelope.brief.to_api_dict(),
            "preview": envelope.preview_result.model_dump(),
        }
    finally:
        conn.close()


def run_apply(*, job_id: int, preview_id: str) -> dict:
    load_profile_env()
    env = Env.load()
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        preview_id_text = str(preview_id or "").strip()
        if not preview_id_text:
            raise ValueError("preview-id is required")
        applied = apply_preview(conn, job_id=int(job_id), preview_id=preview_id_text)
        return {"ok": True, **applied}
    except PlaylistBuilderApiError as exc:
        return {
            "ok": False,
            "job_id": int(job_id),
            "preview_id": str(preview_id),
            "error": {"code": exc.code, "message": exc.message},
        }
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.command == "preview":
        payload = run_preview(job_id=int(args.job_id), override_json=str(args.override_json))
    elif args.command == "apply":
        payload = run_apply(job_id=int(args.job_id), preview_id=str(args.preview_id))
    else:
        raise ValueError("unsupported command")
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
