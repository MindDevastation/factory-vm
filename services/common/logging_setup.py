from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from services.common.env import Env
from services.common.paths import logs_path, storage_root


_CONFIGURED_FOR: set[str] = set()


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

    log_dir = storage_root(env) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    fh = RotatingFileHandler(
        filename=str(log_dir / f"{service}.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
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
