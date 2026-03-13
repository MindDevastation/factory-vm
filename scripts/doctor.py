from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

from services.common.profile import load_profile_env
from services.common.env import Env
from scripts.run_stack import _worker_roles


def _ok(msg: str) -> None:
    print(f"[OK] {msg}")


def _warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def _fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    raise SystemExit(2)



def _http_json(url: str, *, timeout_sec: float, headers: Dict[str, str] | None = None) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def _make_check(name: str, severity: str, result: str, message: str, details: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "name": name,
        "severity": severity,
        "result": result,
        "message": message,
        "details": details or {},
    }


def _compute_overall(checks: List[Dict[str, Any]]) -> tuple[str, int]:
    if any(c["severity"] == "critical" and c["result"] == "FAIL" for c in checks):
        return ("FAIL", 2)
    if any(
        (c["severity"] == "warning" and c["result"] in {"FAIL", "WARN"})
        or (c["severity"] == "info" and c["result"] == "FAIL")
        for c in checks
    ):
        return ("WARNING", 1)
    return ("OK", 0)


def _run_production_smoke(env: Env, *, timeout_sec: float = 3.0) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    start = time.monotonic()
    base_url = f"http://127.0.0.1:{env.port}"

    storage = Path(env.storage_root)
    storage.mkdir(parents=True, exist_ok=True)

    # critical: storage + disk usage
    usage = shutil.disk_usage(storage)
    pct_free = (usage.free / usage.total) * 100 if usage.total else 0.0
    gib_free = usage.free / (1024 ** 3)
    if pct_free < 8 or gib_free < 10:
        checks.append(
            _make_check(
                "disk_free_space",
                "critical",
                "FAIL",
                f"Low free space ({pct_free:.1f}% / {gib_free:.1f} GiB free)",
                {"pct_free": round(pct_free, 2), "gib_free": round(gib_free, 2)},
            )
        )
    elif pct_free < 15 or gib_free < 20:
        checks.append(
            _make_check(
                "disk_free_space",
                "warning",
                "WARN",
                f"Free space below warning threshold ({pct_free:.1f}% / {gib_free:.1f} GiB free)",
                {"pct_free": round(pct_free, 2), "gib_free": round(gib_free, 2)},
            )
        )
    else:
        checks.append(
            _make_check(
                "disk_free_space",
                "warning",
                "PASS",
                f"Disk free space healthy ({pct_free:.1f}% / {gib_free:.1f} GiB free)",
                {"pct_free": round(pct_free, 2), "gib_free": round(gib_free, 2)},
            )
        )

    # critical: API /health
    try:
        health = _http_json(f"{base_url}/health", timeout_sec=timeout_sec)
        if health.get("ok") is True:
            checks.append(_make_check("api_health", "critical", "PASS", "API /health reachable and healthy", health))
        else:
            checks.append(_make_check("api_health", "critical", "FAIL", "API /health returned unhealthy payload", health))
    except Exception as exc:
        checks.append(_make_check("api_health", "critical", "FAIL", f"API /health check failed: {exc}"))

    # critical: expected workers via run_stack role logic + /v1/workers
    expected_roles = _worker_roles(no_importer_flag=False)
    try:
        auth = base64.b64encode(f"{env.basic_user}:{env.basic_pass}".encode("utf-8")).decode("ascii")
        workers_payload = _http_json(
            f"{base_url}/v1/workers",
            timeout_sec=timeout_sec,
            headers={"Authorization": f"Basic {auth}"},
        )
        workers = workers_payload.get("workers") or []
        present_roles = {str(w.get("role") or "") for w in workers}
        missing = sorted([r for r in expected_roles if r not in present_roles])
        if missing:
            checks.append(
                _make_check(
                    "workers_heartbeat",
                    "critical",
                    "FAIL",
                    f"Missing required worker heartbeats: {', '.join(missing)}",
                    {"expected_roles": expected_roles, "present_roles": sorted(present_roles)},
                )
            )
        else:
            checks.append(
                _make_check(
                    "workers_heartbeat",
                    "critical",
                    "PASS",
                    "All required worker roles reported heartbeats",
                    {"expected_roles": expected_roles, "present_roles": sorted(present_roles)},
                )
            )
    except Exception as exc:
        checks.append(_make_check("workers_heartbeat", "critical", "FAIL", f"/v1/workers check failed: {exc}"))

    # warning: gdrive readiness (no external action)
    if env.origin_backend != "gdrive":
        checks.append(_make_check("gdrive_readiness", "info", "SKIP", "Origin backend is not gdrive"))
    else:
        try:
            from services.integrations import gdrive

            if gdrive._GOOGLE_IMPORT_ERROR is not None:  # type: ignore[attr-defined]
                raise RuntimeError(gdrive._GOOGLE_IMPORT_ERROR)  # type: ignore[attr-defined]
            if env.gdrive_sa_json:
                from google.oauth2 import service_account

                service_account.Credentials.from_service_account_file(env.gdrive_sa_json, scopes=gdrive.SCOPES)
            elif env.gdrive_oauth_client_json and env.gdrive_oauth_token_json:
                from google.oauth2.credentials import Credentials

                Credentials.from_authorized_user_file(env.gdrive_oauth_token_json, gdrive.SCOPES)
            else:
                raise RuntimeError("GDrive auth is not configured")
            checks.append(_make_check("gdrive_readiness", "warning", "PASS", "Google Drive credentials loaded"))
        except Exception as exc:
            checks.append(_make_check("gdrive_readiness", "warning", "FAIL", f"Google Drive readiness failed: {exc}"))

    # warning: youtube readiness (no upload)
    if env.upload_backend != "youtube":
        checks.append(_make_check("youtube_readiness", "info", "SKIP", "Upload backend is not youtube"))
    else:
        try:
            from services.integrations import youtube

            if youtube._GOOGLE_IMPORT_ERROR is not None:  # type: ignore[attr-defined]
                raise RuntimeError(youtube._GOOGLE_IMPORT_ERROR)  # type: ignore[attr-defined]
            if not env.yt_client_secret_json or not env.yt_tokens_dir:
                raise RuntimeError("YouTube config missing YT_CLIENT_SECRET_JSON or YT_TOKENS_DIR")
            token_path = Path(env.yt_tokens_dir) / "default.json"
            if not token_path.exists():
                raise RuntimeError(f"YouTube token not found: {token_path}")
            from google.oauth2.credentials import Credentials

            Credentials.from_authorized_user_file(str(token_path), youtube.SCOPES)
            checks.append(_make_check("youtube_readiness", "warning", "PASS", "YouTube credentials loaded"))
        except Exception as exc:
            checks.append(_make_check("youtube_readiness", "warning", "FAIL", f"YouTube readiness failed: {exc}"))

    # warning: telegram readiness (no message send)
    if env.telegram_enabled != 1:
        checks.append(_make_check("telegram_readiness", "info", "SKIP", "Telegram is disabled"))
    else:
        try:
            from aiogram import Bot

            if not env.tg_bot_token or not env.tg_admin_chat_id:
                raise RuntimeError("TG_BOT_TOKEN or TG_ADMIN_CHAT_ID missing")
            _ = Bot(token=env.tg_bot_token)
            checks.append(_make_check("telegram_readiness", "warning", "PASS", "Telegram client initialized"))
        except Exception as exc:
            checks.append(_make_check("telegram_readiness", "warning", "FAIL", f"Telegram readiness failed: {exc}"))

    elapsed_ms = int((time.monotonic() - start) * 1000)
    overall_status, exit_code = _compute_overall(checks)
    return {
        "status": overall_status,
        "exit_code": exit_code,
        "duration_ms": elapsed_ms,
        "checks": checks,
    }


def _print_smoke_human(report: Dict[str, Any]) -> None:
    print("Production smoke report")
    for check in report["checks"]:
        print(f"[{check['result']}] ({check['severity']}) {check['name']}: {check['message']}")
    print(f"Overall: {report['status']} (exit_code={report['exit_code']}, duration_ms={report['duration_ms']})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", nargs="?", default="doctor", choices=["doctor", "production-smoke"])
    parser.add_argument("--profile", default="local", choices=["local", "prod"])
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--timeout-sec", type=float, default=3.0)
    args = parser.parse_args()

    os.environ["FACTORY_PROFILE"] = args.profile
    loaded = load_profile_env()
    if loaded:
        _ok(f"Loaded env file: {loaded}")
    else:
        _warn("No env file loaded. Create deploy/env.local or deploy/env.prod (or deploy/env).")

    env = Env.load()

    if args.command == "production-smoke":
        try:
            report = _run_production_smoke(env, timeout_sec=max(0.5, args.timeout_sec))
        except Exception as exc:
            payload = {
                "status": "RUNNER_ERROR",
                "exit_code": 3,
                "duration_ms": 0,
                "checks": [],
                "error": str(exc),
            }
            if args.as_json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(f"[FAIL] production-smoke runner error: {exc}")
            raise SystemExit(3)

        if args.as_json:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            _print_smoke_human(report)
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
