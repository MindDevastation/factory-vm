from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from services.common.disk_thresholds import DiskPressureLevel, DiskThresholds, evaluate_disk_pressure, load_disk_thresholds
from services.common.env import Env


@dataclass(frozen=True)
class DiskPressureSnapshot:
    pressure: DiskPressureLevel
    free_percent: float
    free_gib: float
    total_bytes: int
    used_bytes: int
    free_bytes: int
    checked_path: str
    resolved_mount_or_anchor: str
    thresholds: DiskThresholds


@dataclass(frozen=True)
class DiskBlockDecision:
    blocked: bool
    reason: str




def _nearest_existing_ancestor(path: Path) -> Path:
    current = path
    while not current.exists():
        parent = current.parent
        if parent == current:
            return Path("/")
        current = parent
    return current


def _resolve_mount_or_anchor(path: Path) -> Path:
    current = path.resolve()
    while True:
        parent = current.parent
        if parent == current:
            return current
        if os.stat(parent).st_dev != os.stat(current).st_dev:
            return current
        current = parent


def classify_write_block(snapshot: DiskPressureSnapshot) -> DiskBlockDecision:
    low_percent = snapshot.free_percent < snapshot.thresholds.fail_percent
    low_bytes = snapshot.free_gib < snapshot.thresholds.fail_gib
    if low_percent and low_bytes:
        return DiskBlockDecision(blocked=True, reason="free_percent_and_free_bytes_below_critical_threshold")
    if low_bytes:
        return DiskBlockDecision(blocked=False, reason="free_bytes_below_critical_threshold_only")
    if low_percent:
        return DiskBlockDecision(blocked=False, reason="free_percent_below_critical_threshold_only")
    return DiskBlockDecision(blocked=False, reason="critical_threshold_not_met")

def evaluate_disk_pressure_for_env(*, env: Env, target_path: Path | None = None) -> DiskPressureSnapshot:
    target = (target_path or Path(env.storage_root)).expanduser().resolve()
    checked_path = _nearest_existing_ancestor(target)
    resolved_mount_or_anchor = _resolve_mount_or_anchor(checked_path)
    usage = shutil.disk_usage(checked_path)
    free_percent = (usage.free / usage.total) * 100.0 if usage.total else 0.0
    free_gib = usage.free / (1024**3)
    thresholds = load_disk_thresholds()
    pressure = evaluate_disk_pressure(free_percent=free_percent, free_gib=free_gib, thresholds=thresholds)
    return DiskPressureSnapshot(
        pressure=pressure,
        free_percent=round(free_percent, 2),
        free_gib=round(free_gib, 2),
        total_bytes=int(usage.total),
        used_bytes=int(usage.used),
        free_bytes=int(usage.free),
        checked_path=str(checked_path),
        resolved_mount_or_anchor=str(resolved_mount_or_anchor),
        thresholds=thresholds,
    )


def emit_disk_pressure_event(*, logger: logging.Logger, snapshot: DiskPressureSnapshot, stage: str) -> None:
    if snapshot.pressure is DiskPressureLevel.OK:
        return
    event_name = "disk.warning" if snapshot.pressure is DiskPressureLevel.WARNING else "disk.critical"
    payload = {
        "event_name": event_name,
        "stage": stage,
        "pressure": snapshot.pressure.value,
        "checked_path": snapshot.checked_path,
        "resolved_mount_or_anchor": snapshot.resolved_mount_or_anchor,
        "total_bytes": snapshot.total_bytes,
        "used_bytes": snapshot.used_bytes,
        "free_bytes": snapshot.free_bytes,
        "free_percent": snapshot.free_percent,
        "free_gib": snapshot.free_gib,
        "thresholds": {
            "warn_percent": snapshot.thresholds.warn_percent,
            "warn_gib": snapshot.thresholds.warn_gib,
            "fail_percent": snapshot.thresholds.fail_percent,
            "fail_gib": snapshot.thresholds.fail_gib,
        },
    }
    logger.info(event_name, extra={"disk_event": payload})
