from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict


class LogStorageTier(str, Enum):
    PROJECT_FILE = "tier_a_project_file"
    JOURNALD = "tier_b_journald"


class LogClass(str, Enum):
    APPLICATION = "application_logs"
    WORKER_RUNTIME = "worker_runtime_logs"
    BOT = "bot_logs"
    UPLOADER_RENDER = "uploader_render_logs"
    RECOVERY_AUDIT = "recovery_audit_logs"
    SMOKE_OPS = "smoke_ops_logs"


@dataclass(frozen=True)
class LogRetentionPolicy:
    log_class: LogClass
    storage_tier: LogStorageTier
    rotate_mib: int
    keep_files: int


CANONICAL_LOG_POLICIES: Dict[LogClass, LogRetentionPolicy] = {
    LogClass.APPLICATION: LogRetentionPolicy(
        log_class=LogClass.APPLICATION,
        storage_tier=LogStorageTier.PROJECT_FILE,
        rotate_mib=20,
        keep_files=10,
    ),
    LogClass.WORKER_RUNTIME: LogRetentionPolicy(
        log_class=LogClass.WORKER_RUNTIME,
        storage_tier=LogStorageTier.PROJECT_FILE,
        rotate_mib=20,
        keep_files=10,
    ),
    LogClass.BOT: LogRetentionPolicy(
        log_class=LogClass.BOT,
        storage_tier=LogStorageTier.PROJECT_FILE,
        rotate_mib=10,
        keep_files=7,
    ),
    LogClass.UPLOADER_RENDER: LogRetentionPolicy(
        log_class=LogClass.UPLOADER_RENDER,
        storage_tier=LogStorageTier.PROJECT_FILE,
        rotate_mib=25,
        keep_files=8,
    ),
    LogClass.RECOVERY_AUDIT: LogRetentionPolicy(
        log_class=LogClass.RECOVERY_AUDIT,
        storage_tier=LogStorageTier.PROJECT_FILE,
        rotate_mib=10,
        keep_files=12,
    ),
    LogClass.SMOKE_OPS: LogRetentionPolicy(
        log_class=LogClass.SMOKE_OPS,
        storage_tier=LogStorageTier.PROJECT_FILE,
        rotate_mib=5,
        keep_files=12,
    ),
}


def journald_log_classes() -> Dict[LogClass, LogStorageTier]:
    return {log_class: LogStorageTier.JOURNALD for log_class in LogClass}
