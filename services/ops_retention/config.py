from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from services.common.disk_thresholds import DiskThresholds, load_disk_thresholds
from services.common.env import Env


@dataclass(frozen=True)
class RetentionWindows:
    preview_hours: int = 24
    export_days: int = 14
    transient_report_days: int = 7
    terminal_workspace_hours: int = 12
    failed_workspace_days: int = 3


@dataclass(frozen=True)
class OpsRetentionConfig:
    log_dir: Path
    disk_thresholds: DiskThresholds
    windows: RetentionWindows


def resolve_log_dir(env: Env) -> Path:
    explicit = os.environ.get("FACTORY_LOG_DIR", "").strip()
    if explicit:
        return Path(explicit)
    return Path(env.storage_root) / "logs"


def load_retention_windows() -> RetentionWindows:
    defaults = RetentionWindows()
    return RetentionWindows(
        preview_hours=int(os.environ.get("FACTORY_RETENTION_PREVIEW_HOURS", str(defaults.preview_hours))),
        export_days=int(os.environ.get("FACTORY_RETENTION_EXPORT_DAYS", str(defaults.export_days))),
        transient_report_days=int(
            os.environ.get("FACTORY_RETENTION_TRANSIENT_REPORT_DAYS", str(defaults.transient_report_days))
        ),
        terminal_workspace_hours=int(
            os.environ.get("FACTORY_RETENTION_TERMINAL_WORKSPACE_HOURS", str(defaults.terminal_workspace_hours))
        ),
        failed_workspace_days=int(
            os.environ.get("FACTORY_RETENTION_FAILED_WORKSPACE_DAYS", str(defaults.failed_workspace_days))
        ),
    )


def load_ops_retention_config(env: Env) -> OpsRetentionConfig:
    return OpsRetentionConfig(
        log_dir=resolve_log_dir(env),
        disk_thresholds=load_disk_thresholds(),
        windows=load_retention_windows(),
    )
