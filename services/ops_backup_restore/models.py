from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BackupScope:
    backup_dir: Path
    db_path: Path
    env_files: tuple[Path, ...]
    config_paths: tuple[Path, ...]
    export_paths: tuple[Path, ...]


@dataclass(frozen=True)
class ManifestItem:
    kind: str
    source_path: str
    stored_path: str
    size_bytes: int
    sha256: str
    contains_secrets: bool
