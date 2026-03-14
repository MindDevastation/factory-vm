from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from services.common.env import Env
from services.ops_retention.log_policy import CANONICAL_LOG_POLICIES, LogClass
from services.ops_retention.config import resolve_log_dir
from services.common.paths import logs_path


_CONFIGURED_FOR: set[str] = set()


_LOG_CLASS_FILE_NAMES: dict[LogClass, str] = {
    LogClass.APPLICATION: "app.log",
    LogClass.WORKER_RUNTIME: "workers.log",
    LogClass.BOT: "bot.log",
    LogClass.UPLOADER_RENDER: "pipeline.log",
    LogClass.RECOVERY_AUDIT: "recovery.log",
    LogClass.SMOKE_OPS: "ops.log",
}


def resolve_log_class(service: str) -> LogClass:
    normalized = (service or "").strip().lower()
    if normalized in {"factory_api", "api", "app", "application"}:
        return LogClass.APPLICATION
    if normalized in {"bot", "telegram_bot"}:
        return LogClass.BOT
    if normalized.startswith(("worker-uploader", "worker-orchestrator", "worker-track_jobs", "pipeline", "render", "uploader")):
        return LogClass.UPLOADER_RENDER
    if normalized.startswith(("worker-cleanup", "recovery", "audit")):
        return LogClass.RECOVERY_AUDIT
    if normalized.startswith(("smoke", "ops")):
        return LogClass.SMOKE_OPS
    if normalized.startswith("worker-") or normalized in {"workers", "worker"}:
        return LogClass.WORKER_RUNTIME
    return LogClass.APPLICATION


def resolve_log_file_policy(service: str) -> tuple[Path, int, int]:
    log_class = resolve_log_class(service)
    policy = CANONICAL_LOG_POLICIES[log_class]
    filename = _LOG_CLASS_FILE_NAMES[log_class]
    return Path(filename), policy.rotate_mib * 1024 * 1024, policy.keep_files


class _ServiceFilter(logging.Filter):
    def __init__(self, service: str):
        super().__init__()
        self._service = service

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        if not hasattr(record, "service"):
            record.service = self._service
        return True


def setup_logging(env: Env, *, service: str) -> None:
    """Configure root logging once per service.

    Handlers:
      - stdout (for systemd/journald)
      - rotating file: storage/logs/<service>.log
    """

    if service in _CONFIGURED_FOR:
        return

    level = os.environ.get("LOG_LEVEL", "INFO").upper().strip() or "INFO"
    root = logging.getLogger()
    root.setLevel(level)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(service)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.addFilter(_ServiceFilter(service))
    root.addHandler(sh)

    log_dir = resolve_log_dir(env)
    log_dir.mkdir(parents=True, exist_ok=True)
    file_name, max_bytes, keep_files = resolve_log_file_policy(service)
    fh = RotatingFileHandler(
        filename=str(log_dir / file_name),
        maxBytes=max_bytes,
        backupCount=keep_files,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    fh.addFilter(_ServiceFilter(service))
    root.addHandler(fh)

    _CONFIGURED_FOR.add(service)


def get_logger(service: str) -> logging.Logger:
    return logging.getLogger(service)


def append_job_log(env: Env, job_id: int, line: str) -> None:
    """Append line to per-job log file (storage/logs/job_<id>.log)."""
    p = logs_path(env, job_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


def safe_path_basename(value: str, *, fallback: str) -> str:
    """Drop any path components (path traversal hardening)."""
    name = Path(str(value)).name
    return name or fallback
