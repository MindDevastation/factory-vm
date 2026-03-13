from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import stat
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import quote

from services.ops_backup_restore.index import rebuild_index_from_snapshots, upsert_snapshot, write_index, write_latest_successful
from services.ops_backup_restore.manifest import build_manifest
from services.ops_backup_restore.models import ManifestItem
from services.ops_backup_restore.paths import generate_backup_id, snapshot_dir, snapshots_root
from services.ops_backup_restore.scope import resolve_backup_scope

LOGGER = logging.getLogger(__name__)


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


def _artifact_path(group: str, source: Path) -> Path:
    return Path(group) / quote(str(source), safe="")


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _checksummed_files(root: Path) -> list[Path]:
    return sorted(
        [
            node
            for node in root.rglob("*")
            if node.is_file() and node.name not in {"manifest.json", "checksums.sha256"}
        ]
    )


def _directory_artifact_metadata(root: Path, artifact_rel: str) -> tuple[int, str]:
    artifact_root = root / artifact_rel
    digest = sha256()
    total_size = 0
    for node in sorted([path for path in artifact_root.rglob("*") if path.is_file()]):
        rel = node.relative_to(root).as_posix()
        file_size = node.stat().st_size
        file_sha = _sha256_file(node)
        digest.update(f"{rel}\0{file_size}\0{file_sha}\n".encode("utf-8"))
        total_size += file_size
    return total_size, digest.hexdigest()


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
        scope = resolve_backup_scope(
            {
                "FACTORY_BACKUP_DIR": source.get("FACTORY_BACKUP_DIR", ""),
                "FACTORY_DB_PATH": source.get("FACTORY_DB_PATH", ""),
                "FACTORY_ENV_FILES": source.get("FACTORY_ENV_FILES", ""),
                "FACTORY_BACKUP_CONFIG_PATHS": source.get("FACTORY_BACKUP_CONFIG_PATHS", ""),
                "FACTORY_BACKUP_EXPORT_DIRS": source.get("FACTORY_BACKUP_EXPORT_DIRS", ""),
            }
        )
        env_files = scope.env_files or tuple(
            p for p in (Path("deploy/env"), Path("deploy/env.local"), Path("deploy/env.prod")) if p.exists()
        )
        return BackupSettings(
            db_path=scope.db_path,
            backup_dir=scope.backup_dir,
            env_files=env_files,
            config_paths=scope.config_paths,
            export_dirs=scope.export_paths,
        )


def _manifest(snapshot: Path) -> dict:
    path = snapshot / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _snapshots(root: Path) -> list[Path]:
    snapshots_dir = snapshots_root(root)
    return sorted([p for p in snapshots_dir.iterdir() if p.is_dir()], key=lambda p: p.name) if snapshots_dir.exists() else []


def _successful(root: Path) -> list[Path]:
    return sorted([p for p in _snapshots(root) if _manifest(p).get("status") == "SUCCESS"], key=lambda p: p.name, reverse=True)


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
        if _manifest(snap).get("status") == "SUCCESS" and snap not in keep:
            shutil.rmtree(snap)
            removed.append(snap)
    return removed


def create_backup(settings: BackupSettings, *, now: datetime | None = None) -> Path:
    started = time.monotonic()
    hostname = os.uname().nodename
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(settings.backup_dir, 0o700)

    backup_id = generate_backup_id(now)
    snapshots_dir = snapshots_root(settings.backup_dir)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    temp_snap = snapshots_dir / f"{backup_id}.tmp"
    snap = snapshot_dir(settings.backup_dir, backup_id)

    LOGGER.info(
        "ops.backup.create.start",
        extra={
            "backup_id": backup_id,
            "hostname": hostname,
            "items_count": 0,
            "total_size_bytes": 0,
            "duration_ms": 0,
            "result": "STARTED",
            "error_code": "",
        },
    )

    if temp_snap.exists():
        shutil.rmtree(temp_snap)

    try:
        temp_snap.mkdir(parents=True, exist_ok=False)
        os.chmod(temp_snap, 0o700)

        db_dst = temp_snap / "db" / "app.sqlite3"
        db_dst.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(settings.db_path) as src, sqlite3.connect(db_dst) as dst:
            src.backup(dst)
        if not db_dst.exists() or db_dst.stat().st_size <= 0:
            raise RuntimeError("sqlite_backup_output_invalid")
        _chmod600(db_dst, generated=True)

        copied = {"env": [], "config": [], "exports": []}
        for src in settings.env_files:
            if src.exists() and src.is_file():
                artifact_rel = _artifact_path("env", src)
                _copy_file(src, temp_snap / artifact_rel)
                copied["env"].append({"source": str(src), "artifact": str(artifact_rel)})
        for src in settings.config_paths:
            if src.exists():
                artifact_rel = _artifact_path("config", src)
                _copy_dir(src, temp_snap / artifact_rel) if src.is_dir() else _copy_file(src, temp_snap / artifact_rel)
                copied["config"].append({"source": str(src), "artifact": str(artifact_rel)})
        for src in settings.export_dirs:
            if src.exists():
                artifact_rel = _artifact_path("exports", src)
                _copy_dir(src, temp_snap / artifact_rel) if src.is_dir() else _copy_file(src, temp_snap / artifact_rel)
                copied["exports"].append({"source": str(src), "artifact": str(artifact_rel)})

        checksummed = _checksummed_files(temp_snap)
        sha_by_rel = {path.relative_to(temp_snap).as_posix(): _sha256_file(path) for path in checksummed}
        total_size_bytes = sum(path.stat().st_size for path in checksummed)

        items = [
            ManifestItem(
                kind="db",
                source_path=str(settings.db_path),
                stored_path="db/app.sqlite3",
                size_bytes=db_dst.stat().st_size,
                sha256=sha_by_rel["db/app.sqlite3"],
                contains_secrets=False,
            )
        ]
        for kind in ("env", "config", "exports"):
            for item in copied[kind]:
                path = temp_snap / item["artifact"]
                if path.exists() and path.is_dir():
                    size_bytes, artifact_sha = _directory_artifact_metadata(temp_snap, item["artifact"])
                else:
                    size_bytes = path.stat().st_size if path.exists() and path.is_file() else 0
                    artifact_sha = sha_by_rel.get(item["artifact"], "")
                items.append(
                    ManifestItem(
                        kind=kind,
                        source_path=item["source"],
                        stored_path=item["artifact"],
                        size_bytes=size_bytes,
                        sha256=artifact_sha,
                        contains_secrets=(kind == "env"),
                    )
                )

        manifest = build_manifest(
            backup_id=backup_id,
            scope=resolve_backup_scope(
                {
                    "FACTORY_BACKUP_DIR": str(settings.backup_dir),
                    "FACTORY_DB_PATH": str(settings.db_path),
                    "FACTORY_ENV_FILES": ":".join(str(item) for item in settings.env_files),
                    "FACTORY_BACKUP_CONFIG_PATHS": ":".join(str(item) for item in settings.config_paths),
                    "FACTORY_BACKUP_EXPORT_DIRS": ":".join(str(item) for item in settings.export_dirs),
                }
            ),
            items=items,
            created_at=datetime.now(UTC),
        )
        manifest["snapshot"] = backup_id
        manifest["restore_targets"] = {
            "FACTORY_DB_PATH": str(settings.db_path),
            "FACTORY_ENV_FILES": [item["source"] for item in copied["env"]],
            "FACTORY_BACKUP_CONFIG_PATHS": [item["source"] for item in copied["config"]],
            "FACTORY_BACKUP_EXPORT_DIRS": [item["source"] for item in copied["exports"]],
        }
        manifest["artifacts"] = {
            "db": "db/app.sqlite3",
            "env": copied["env"],
            "config": copied["config"],
            "exports": copied["exports"],
        }
        manifest_file = temp_snap / "manifest.json"
        manifest_file.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        _chmod600(manifest_file, generated=True)

        checksums_file = temp_snap / "checksums.sha256"
        checksums_file.write_text(
            "\n".join(f"{sha_by_rel[rel]}  {rel}" for rel in sorted(sha_by_rel)) + "\n",
            encoding="utf-8",
        )
        _chmod600(checksums_file, generated=True)

        _fsync_file(db_dst)
        _fsync_file(manifest_file)
        _fsync_file(checksums_file)
        _fsync_dir(db_dst.parent)
        _fsync_dir(temp_snap)

        os.replace(temp_snap, snap)
        _fsync_dir(snapshots_dir)

        index_payload = upsert_snapshot(
            backup_root=settings.backup_dir,
            backup_id=backup_id,
            created_at=manifest["created_at"],
        )
        write_index(settings.backup_dir, index_payload)
        write_latest_successful(settings.backup_dir, backup_id)

        try:
            apply_retention(settings.backup_dir)
            write_index(settings.backup_dir, rebuild_index_from_snapshots(settings.backup_dir))

            successful = _successful(settings.backup_dir)
            if successful:
                write_latest_successful(settings.backup_dir, successful[0].name)
        except Exception:
            LOGGER.exception("ops.backup.create.retention_failure", extra={"backup_id": backup_id})

        duration_ms = int((time.monotonic() - started) * 1000)
        LOGGER.info(
            "ops.backup.create.success",
            extra={
                "backup_id": backup_id,
                "hostname": hostname,
                "items_count": len(items),
                "total_size_bytes": total_size_bytes,
                "duration_ms": duration_ms,
                "result": "SUCCESS",
                "error_code": "",
            },
        )
        return snap
    except Exception as exc:
        duration_ms = int((time.monotonic() - started) * 1000)
        LOGGER.exception(
            "ops.backup.create.failure",
            extra={
                "backup_id": backup_id,
                "hostname": hostname,
                "items_count": 0,
                "total_size_bytes": 0,
                "duration_ms": duration_ms,
                "result": "FAILURE",
                "error_code": type(exc).__name__,
            },
        )
        raise


def list_snapshots(settings: BackupSettings) -> list[Path]:
    return _snapshots(settings.backup_dir)


def _legacy_or_exact_mappings(manifest: dict, *, artifact_key: str, scope_key: str) -> list[tuple[Path, Path]]:
    artifacts = manifest.get("artifacts", {}).get(artifact_key, [])
    if not artifacts:
        return []
    if isinstance(artifacts[0], dict):
        return [(Path(item["source"]), Path(item["artifact"])) for item in artifacts]

    scope_paths = [Path(item) for item in manifest.get("restore_targets", {}).get(scope_key, [])]
    if len(scope_paths) != len(artifacts):
        raise RuntimeError(f"snapshot manifest {artifact_key} mapping is ambiguous")
    return [(scope_paths[idx], Path(artifacts[idx])) for idx in range(len(artifacts))]


def _resolve_target(source: Path, configured: Iterable[Path], *, scope_key: str) -> Path:
    for candidate in configured:
        if candidate == source:
            return candidate
    raise RuntimeError(f"restore target for {scope_key} source '{source}' is not configured")


def restore_snapshot(settings: BackupSettings, snapshot: Path, *, services_stopped_file: Path) -> None:
    if not services_stopped_file.exists():
        raise RuntimeError("restore requires services to be stopped before file replacement")
    manifest = _manifest(snapshot)
    if manifest.get("status") != "SUCCESS":
        raise RuntimeError("snapshot manifest is missing or not successful")

    db_src = snapshot / Path(manifest["artifacts"]["db"])
    if not db_src.exists():
        raise RuntimeError("snapshot DB artifact is missing")
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_src, settings.db_path)
    _chmod600(settings.db_path)

    for source, artifact_rel in _legacy_or_exact_mappings(
        manifest,
        artifact_key="env",
        scope_key="FACTORY_ENV_FILES",
    ):
        target = _resolve_target(source, settings.env_files, scope_key="FACTORY_ENV_FILES")
        _copy_file(snapshot / artifact_rel, target)

    for rel, options, scope_key in (
        ("config", settings.config_paths, "FACTORY_BACKUP_CONFIG_PATHS"),
        ("exports", settings.export_dirs, "FACTORY_BACKUP_EXPORT_DIRS"),
    ):
        for source, src_rel in _legacy_or_exact_mappings(manifest, artifact_key=rel, scope_key=scope_key):
            target = _resolve_target(source, options, scope_key=scope_key)
            src = snapshot / src_rel
            _copy_dir(src, target) if src.is_dir() else _copy_file(src, target)
