from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

SNAPSHOTS_DIRNAME = "snapshots"
INDEX_FILENAME = "index.json"
LATEST_SUCCESSFUL_FILENAME = "latest_successful"


def generate_backup_id(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def snapshots_root(backup_root: Path) -> Path:
    return backup_root / SNAPSHOTS_DIRNAME


def snapshot_dir(backup_root: Path, backup_id: str) -> Path:
    return snapshots_root(backup_root) / backup_id


def manifest_path(backup_root: Path, backup_id: str) -> Path:
    return snapshot_dir(backup_root, backup_id) / "manifest.json"


def checksums_path(backup_root: Path, backup_id: str) -> Path:
    return snapshot_dir(backup_root, backup_id) / "checksums.sha256"


def index_path(backup_root: Path) -> Path:
    return backup_root / INDEX_FILENAME


def latest_successful_path(backup_root: Path) -> Path:
    return backup_root / LATEST_SUCCESSFUL_FILENAME
