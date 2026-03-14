from __future__ import annotations

import logging
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
    target_path: str
    thresholds: DiskThresholds




def _nearest_existing_ancestor(path: Path) -> Path:
    current = path
    while not current.exists():
        parent = current.parent
        if parent == current:
            return Path("/")
        current = parent
    return current

def evaluate_disk_pressure_for_env(*, env: Env, target_path: Path | None = None) -> DiskPressureSnapshot:
    target = (target_path or Path(env.storage_root)).expanduser().resolve()
    usage_target = _nearest_existing_ancestor(target)
    usage = shutil.disk_usage(usage_target)
    free_percent = (usage.free / usage.total) * 100.0 if usage.total else 0.0
    free_gib = usage.free / (1024**3)
    thresholds = load_disk_thresholds()
    pressure = evaluate_disk_pressure(free_percent=free_percent, free_gib=free_gib, thresholds=thresholds)
    return DiskPressureSnapshot(
        pressure=pressure,
        free_percent=round(free_percent, 2),
        free_gib=round(free_gib, 2),
        target_path=str(usage_target),
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
        "target_path": snapshot.target_path,
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
