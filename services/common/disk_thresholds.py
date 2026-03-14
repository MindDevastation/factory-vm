from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DiskThresholds:
    warn_percent: float = 15.0
    warn_gib: float = 20.0
    fail_percent: float = 8.0
    fail_gib: float = 10.0


def load_disk_thresholds(
    *,
    warn_percent_env: str = "FACTORY_SMOKE_DISK_WARN_PERCENT",
    warn_gib_env: str = "FACTORY_SMOKE_DISK_WARN_GIB",
    fail_percent_env: str = "FACTORY_SMOKE_DISK_FAIL_PERCENT",
    fail_gib_env: str = "FACTORY_SMOKE_DISK_FAIL_GIB",
) -> DiskThresholds:
    defaults = DiskThresholds()
    return DiskThresholds(
        warn_percent=float(os.environ.get(warn_percent_env, str(defaults.warn_percent))),
        warn_gib=float(os.environ.get(warn_gib_env, str(defaults.warn_gib))),
        fail_percent=float(os.environ.get(fail_percent_env, str(defaults.fail_percent))),
        fail_gib=float(os.environ.get(fail_gib_env, str(defaults.fail_gib))),
    )


def evaluate_disk_status(*, free_percent: float, free_gib: float, thresholds: DiskThresholds) -> str:
    if free_percent < thresholds.fail_percent or free_gib < thresholds.fail_gib:
        return "FAIL"
    if free_percent < thresholds.warn_percent or free_gib < thresholds.warn_gib:
        return "WARN"
    return "PASS"
