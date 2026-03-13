from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

from services.common.env import Env
from services.common.profile import load_profile_env
from services.ops_health_smoke import render_human_report, run_checks_with_error_capture


def _ok(msg: str) -> None:
    print(f"[OK] {msg}")


def _warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def _fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    raise SystemExit(2)


def _parse_checks(raw_checks: str | None) -> set[str] | None:
    if not raw_checks:
        return None
    selected = {item.strip() for item in raw_checks.split(",") if item.strip()}
    return selected or None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", nargs="?", default="doctor", choices=["doctor", "production-smoke"])
    parser.add_argument("--profile", default="local", choices=["local", "prod"])
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--json-out", default="")
    parser.add_argument("--checks", default="")
    args = parser.parse_args()
    is_smoke_json = args.command == "production-smoke" and args.as_json

    os.environ["FACTORY_PROFILE"] = args.profile
    loaded = load_profile_env()
    if not is_smoke_json:
        if loaded:
            _ok(f"Loaded env file: {loaded}")
        else:
            _warn("No env file loaded. Create deploy/env.local or deploy/env.prod (or deploy/env).")

    env = Env.load()

    if args.command == "production-smoke":
        selected_checks = _parse_checks(args.checks)
        report = run_checks_with_error_capture(profile=args.profile, selected_check_ids=selected_checks)

        if args.json_out:
            Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

        if args.as_json:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            print(render_human_report(report))
        raise SystemExit(int(report["exit_code"]))

    if shutil.which("ffmpeg") and shutil.which("ffprobe"):
        _ok("ffmpeg/ffprobe found")
    else:
        _fail("ffmpeg/ffprobe not found. Install ffmpeg.")

    storage = Path(env.storage_root)
    storage.mkdir(parents=True, exist_ok=True)
    for sub in ["workspace", "outbox", "logs", "qa", "previews"]:
        (storage / sub).mkdir(parents=True, exist_ok=True)
    _ok(f"Storage OK: {storage.resolve()}")

    if args.profile == "local":
        if env.origin_backend != "local":
            _warn("Local profile but ORIGIN_BACKEND is not 'local'.")
        origin = Path(env.origin_local_root)
        if not origin.exists():
            _warn(f"Local origin does not exist yet: {origin.resolve()}")
        else:
            _ok(f"Local origin: {origin.resolve()}")
    else:
        if env.origin_backend != "gdrive":
            _warn("Prod profile but ORIGIN_BACKEND is not 'gdrive'.")
        if not env.gdrive_root_id:
            _warn("GDRIVE_ROOT_ID is empty.")
        if env.gdrive_sa_json and not Path(env.gdrive_sa_json).exists():
            _warn(f"GDRIVE_SERVICE_ACCOUNT_JSON not found: {env.gdrive_sa_json}")

    if args.profile == "prod":
        if env.upload_backend != "youtube":
            _warn("Prod profile but UPLOAD_BACKEND is not 'youtube'.")
        if env.yt_client_secret_json and not Path(env.yt_client_secret_json).exists():
            _warn(f"YT client secret not found: {env.yt_client_secret_json}")
        if env.yt_token_json and not Path(env.yt_token_json).exists():
            _warn(f"YT token not found: {env.yt_token_json}")
        if env.telegram_enabled == 1:
            if not env.tg_bot_token or not env.tg_admin_chat_id:
                _warn("Telegram enabled but TG_BOT_TOKEN/TG_ADMIN_CHAT_ID not set.")
            else:
                _ok("Telegram config present")

    _ok("Doctor finished.")


if __name__ == "__main__":
    main()
