#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from services.common.disk_thresholds import evaluate_disk_status, load_disk_thresholds

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class RetentionConfig:
    log_dir: Path
    storage_root: Path
    preview_hours: float
    export_days: float
    transient_report_days: float
    terminal_workspace_hours: float
    failed_workspace_days: float


def _as_float(name: str, default: str) -> float:
    return float(os.environ.get(name, default))


def _load_config() -> RetentionConfig:
    storage_root = Path(os.environ.get("FACTORY_STORAGE_ROOT", "storage"))
    return RetentionConfig(
        log_dir=Path(os.environ.get("FACTORY_LOG_DIR", str(storage_root / "logs"))),
        storage_root=storage_root,
        preview_hours=_as_float("FACTORY_RETENTION_PREVIEW_HOURS", "48"),
        export_days=_as_float("FACTORY_RETENTION_EXPORT_DAYS", "14"),
        transient_report_days=_as_float("FACTORY_RETENTION_TRANSIENT_REPORT_DAYS", "14"),
        terminal_workspace_hours=_as_float("FACTORY_RETENTION_TERMINAL_WORKSPACE_HOURS", "24"),
        failed_workspace_days=_as_float("FACTORY_RETENTION_FAILED_WORKSPACE_DAYS", "7"),
    )


def _iter_files(root: Path):
    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def _is_older_than(path: Path, max_age_seconds: float, now: float) -> bool:
    try:
        age_seconds = now - path.stat().st_mtime
    except OSError:
        return False
    return age_seconds > max_age_seconds


def _emit(event: str, **fields: object) -> None:
    payload = {"event": event, **fields}
    print(json.dumps(payload, sort_keys=True))


def _resolve_targets(config: RetentionConfig, *, urgent: bool, critical_disk: bool) -> list[Path]:
    now = time.time()
    candidates: list[Path] = []

    for path in _iter_files(config.storage_root / "previews"):
        if _is_older_than(path, config.preview_hours * 3600, now):
            candidates.append(path)

    for root in (REPO_ROOT / "exports", REPO_ROOT / "data" / "exports"):
        for path in _iter_files(root):
            if _is_older_than(path, config.export_days * 86400, now):
                candidates.append(path)

    for root in (REPO_ROOT / "qa", REPO_ROOT / "data" / "qa"):
        for path in _iter_files(root):
            if _is_older_than(path, config.transient_report_days * 86400, now):
                candidates.append(path)

    for path in _iter_files(config.storage_root / "workspace" / "terminal"):
        if _is_older_than(path, config.terminal_workspace_hours * 3600, now):
            candidates.append(path)

    for path in _iter_files(config.storage_root / "workspace" / "failed"):
        if _is_older_than(path, config.failed_workspace_days * 86400, now):
            candidates.append(path)

    if urgent and critical_disk:
        for path in _iter_files(config.log_dir):
            if _is_older_than(path, 24 * 3600, now):
                candidates.append(path)

    # Stable dedup ordering.
    return sorted(set(candidates))


def _disk_status(storage_root: Path) -> str:
    thresholds = load_disk_thresholds()
    usage = shutil.disk_usage(storage_root if storage_root.exists() else REPO_ROOT)
    free_percent = (usage.free / usage.total) * 100.0 if usage.total else 0.0
    free_gib = usage.free / (1024**3)
    status = evaluate_disk_status(free_percent=free_percent, free_gib=free_gib, thresholds=thresholds)
    _emit(
        "retention.disk_status",
        free_percent=round(free_percent, 2),
        free_gib=round(free_gib, 2),
        status=status,
        warn_percent=thresholds.warn_percent,
        warn_gib=thresholds.warn_gib,
        fail_percent=thresholds.fail_percent,
        fail_gib=thresholds.fail_gib,
    )
    return status


def _scan(*, urgent: bool) -> int:
    config = _load_config()
    disk_state = _disk_status(config.storage_root)
    critical_disk = disk_state == "FAIL"
    candidates = _resolve_targets(config, urgent=urgent, critical_disk=critical_disk)
    _emit("retention.scan_complete", urgent=urgent, critical_disk=critical_disk, candidate_count=len(candidates))
    for path in candidates:
        _emit("retention.candidate", path=str(path.relative_to(REPO_ROOT)))
    return 0


def _run(*, urgent: bool) -> int:
    config = _load_config()
    disk_state = _disk_status(config.storage_root)
    critical_disk = disk_state == "FAIL"
    if urgent and not critical_disk:
        _emit("retention.urgent_skipped", reason="disk_not_critical")

    candidates = _resolve_targets(config, urgent=urgent, critical_disk=critical_disk)
    deleted = 0
    failed = 0
    for path in candidates:
        try:
            path.unlink()
            deleted += 1
            _emit("retention.delete", path=str(path.relative_to(REPO_ROOT)))
        except OSError as exc:
            failed += 1
            _emit("retention.delete_error", path=str(path), error=f"{exc.__class__.__name__}: {exc}")

    _emit("retention.run_complete", urgent=urgent, critical_disk=critical_disk, deleted=deleted, failed=failed)
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Factory retention policy runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Report retention candidates without deleting")
    scan_parser.add_argument("--urgent", action="store_true", help="Include urgent candidates when disk is critical")

    run_parser = subparsers.add_parser("run", help="Apply retention deletes")
    run_parser.add_argument("--urgent", action="store_true", help="Enable urgent deletes only when disk is critical")

    args = parser.parse_args()
    if args.command == "scan":
        return _scan(urgent=args.urgent)
    return _run(urgent=args.urgent)


if __name__ == "__main__":
    raise SystemExit(main())
