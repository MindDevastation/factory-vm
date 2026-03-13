from __future__ import annotations

import json
import os
import shutil
import sqlite3
import stat
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


def _split_paths(raw: str) -> tuple[Path, ...]:
    return tuple(Path(x.strip()) for x in raw.split(",") if x.strip())


def _chmod600(path: Path, *, generated: bool = False) -> None:
    mode = 0o600 if generated else (stat.S_IMODE(path.stat().st_mode) & 0o600) or 0o600
    os.chmod(path, mode)


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    _chmod600(dst)


def _copy_dir(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    for p in dst.rglob("*"):
        if p.is_file():
            _chmod600(p)


def _snapshot_name(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class BackupSettings:
    db_path: Path
    backup_dir: Path
    env_files: tuple[Path, ...]
    config_paths: tuple[Path, ...]
    export_dirs: tuple[Path, ...]

    @staticmethod
    def from_env(env: dict[str, str] | None = None) -> "BackupSettings":
        source = os.environ if env is None else env
        backup_dir = source.get("FACTORY_BACKUP_DIR", "").strip()
        if not backup_dir:
            raise ValueError("FACTORY_BACKUP_DIR is required")
        env_files_raw = source.get("FACTORY_ENV_FILES", "").strip()
        env_files = _split_paths(env_files_raw) if env_files_raw else tuple(
            p for p in (Path("deploy/env"), Path("deploy/env.local"), Path("deploy/env.prod")) if p.exists()
        )
        return BackupSettings(
            db_path=Path(source.get("FACTORY_DB_PATH", "").strip() or "data/factory.sqlite3"),
            backup_dir=Path(backup_dir),
            env_files=env_files,
            config_paths=_split_paths(source.get("FACTORY_BACKUP_CONFIG_PATHS", "")),
            export_dirs=_split_paths(source.get("FACTORY_BACKUP_EXPORT_DIRS", "")),
        )


def _manifest(snapshot: Path) -> dict:
    path = snapshot / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _snapshots(root: Path) -> list[Path]:
    return sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name) if root.exists() else []


def _successful(root: Path) -> list[Path]:
    return sorted([p for p in _snapshots(root) if _manifest(p).get("status") == "success"], key=lambda p: p.name, reverse=True)


def apply_retention(backup_dir: Path) -> list[Path]:
    keep: set[Path] = set()
    daily: set[str] = set()
    weekly: set[str] = set()
    monthly: set[str] = set()
    successful = _successful(backup_dir)
    if successful:
        keep.add(successful[0])
    for snap in successful:
        ts = datetime.strptime(snap.name, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        d, w, m = ts.strftime("%Y-%m-%d"), f"{ts.isocalendar().year}-W{ts.isocalendar().week:02d}", ts.strftime("%Y-%m")
        if len(daily) < 7 and d not in daily:
            daily.add(d)
            keep.add(snap)
        if len(weekly) < 4 and w not in weekly:
            weekly.add(w)
            keep.add(snap)
        if len(monthly) < 6 and m not in monthly:
            monthly.add(m)
            keep.add(snap)
    removed: list[Path] = []
    for snap in _snapshots(backup_dir):
        if _manifest(snap).get("status") == "success" and snap not in keep:
            shutil.rmtree(snap)
            removed.append(snap)
    return removed


def create_backup(settings: BackupSettings, *, now: datetime | None = None) -> Path:
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(settings.backup_dir, 0o700)
    snap = settings.backup_dir / _snapshot_name(now)
    snap.mkdir(parents=True, exist_ok=False)
    os.chmod(snap, 0o700)

    db_dst = snap / "db" / settings.db_path.name
    db_dst.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(settings.db_path) as src, sqlite3.connect(db_dst) as dst:
        src.backup(dst)
    _chmod600(db_dst, generated=True)

    copied = {"env": [], "config": [], "exports": []}
    for src in settings.env_files:
        if src.exists() and src.is_file():
            _copy_file(src, snap / "env" / src.name)
            copied["env"].append(str(src))
    for src in settings.config_paths:
        if src.exists():
            _copy_dir(src, snap / "config" / src.name) if src.is_dir() else _copy_file(src, snap / "config" / src.name)
            copied["config"].append(str(src))
    for src in settings.export_dirs:
        if src.exists() and src.is_dir():
            _copy_dir(src, snap / "exports" / src.name)
            copied["exports"].append(str(src))

    manifest = {
        "schema": "ops-backup-manifest-v1",
        "status": "success",
        "created_at": datetime.now(UTC).isoformat(),
        "snapshot": snap.name,
        "scope": {
            "FACTORY_DB_PATH": str(settings.db_path),
            "FACTORY_ENV_FILES": copied["env"],
            "FACTORY_BACKUP_CONFIG_PATHS": copied["config"],
            "FACTORY_BACKUP_EXPORT_DIRS": copied["exports"],
        },
        "artifacts": {
            "db": str(Path("db") / settings.db_path.name),
            "env": [str(Path("env") / Path(x).name) for x in copied["env"]],
            "config": [str(Path("config") / Path(x).name) for x in copied["config"]],
            "exports": [str(Path("exports") / Path(x).name) for x in copied["exports"]],
        },
    }
    m = snap / "manifest.json"
    m.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    _chmod600(m, generated=True)
    apply_retention(settings.backup_dir)
    return snap


def list_snapshots(settings: BackupSettings) -> list[Path]:
    return _snapshots(settings.backup_dir)


def restore_snapshot(settings: BackupSettings, snapshot: Path, *, services_stopped_file: Path) -> None:
    if not services_stopped_file.exists():
        raise RuntimeError("restore requires services to be stopped before file replacement")
    manifest = _manifest(snapshot)
    if manifest.get("status") != "success":
        raise RuntimeError("snapshot manifest is missing or not successful")

    db_src = snapshot / Path(manifest["artifacts"]["db"])
    if not db_src.exists():
        raise RuntimeError("snapshot DB artifact is missing")
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_src, settings.db_path)
    _chmod600(settings.db_path)

    for env_rel in manifest["artifacts"]["env"]:
        _copy_file(snapshot / env_rel, Path("deploy") / Path(env_rel).name)

    for rel, options in (("config", settings.config_paths), ("exports", settings.export_dirs)):
        for src_rel in manifest["artifacts"][rel]:
            src = snapshot / src_rel
            match = next((p for p in options if p.name == Path(src_rel).name), None)
            if not match:
                continue
            _copy_dir(src, match) if src.is_dir() else _copy_file(src, match)
