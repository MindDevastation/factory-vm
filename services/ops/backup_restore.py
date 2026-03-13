from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import stat
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import quote

from services.ops_backup_restore.index import (
    load_index,
    rebuild_index_from_snapshots,
    upsert_snapshot,
    write_index,
    write_latest_successful,
)
from services.ops_backup_restore.manifest import build_manifest
from services.ops_backup_restore.models import ManifestItem
from services.ops_backup_restore.paths import generate_backup_id, snapshot_dir, snapshots_root
from services.ops_backup_restore.scope import resolve_backup_scope

LOGGER = logging.getLogger(__name__)


class OpsRestoreError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


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


def _checksummed_files(root: Path, *, include_manifest: bool = True) -> list[Path]:
    excluded_names = {"checksums.sha256"}
    if not include_manifest:
        excluded_names.add("manifest.json")
    return sorted(
        [
            node
            for node in root.rglob("*")
            if node.is_file() and node.name not in excluded_names
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

        checksummed = _checksummed_files(temp_snap, include_manifest=False)
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

        contract_checksummed = _checksummed_files(temp_snap)
        contract_sha_by_rel = {path.relative_to(temp_snap).as_posix(): _sha256_file(path) for path in contract_checksummed}

        checksums_file = temp_snap / "checksums.sha256"
        checksums_file.write_text(
            "\n".join(f"{contract_sha_by_rel[rel]}  {rel}" for rel in sorted(contract_sha_by_rel)) + "\n",
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


def list_backups(settings: BackupSettings) -> list[dict]:
    return load_index(settings.backup_dir).get("snapshots", [])


def resolve_snapshot_from_index(settings: BackupSettings, backup_id: str) -> Path:
    for item in list_backups(settings):
        if item.get("backup_id") != backup_id:
            continue
        if item.get("status") != "SUCCESS":
            raise OpsRestoreError("OPS_RESTORE_BACKUP_NOT_FOUND", f"backup_id '{backup_id}' is not SUCCESS")
        return snapshot_dir(settings.backup_dir, backup_id)
    raise OpsRestoreError("OPS_RESTORE_BACKUP_NOT_FOUND", f"backup_id '{backup_id}' not found")


def _parse_checksums(snapshot: Path) -> dict[str, str]:
    checksums_path = snapshot / "checksums.sha256"
    if not checksums_path.exists():
        raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", "snapshot checksums file is missing")
    mapping: dict[str, str] = {}
    for line in checksums_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split("  ", 1)
        if len(parts) != 2:
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", "invalid checksums format")
        mapping[parts[1]] = parts[0]
    return mapping


def verify_backup_snapshot(snapshot: Path) -> dict:
    manifest = _manifest(snapshot)
    if manifest.get("status") != "SUCCESS":
        raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", "snapshot manifest is missing or invalid")

    checksums = _parse_checksums(snapshot)
    expected = set(checksums.keys())
    actual = {path.relative_to(snapshot).as_posix() for path in _checksummed_files(snapshot)}
    if expected != actual:
        raise OpsRestoreError("OPS_RESTORE_CHECKSUM_FAILED", "snapshot checksum file does not match snapshot contents")

    for rel, digest in checksums.items():
        file_path = snapshot / rel
        if not file_path.exists() or _sha256_file(file_path) != digest:
            raise OpsRestoreError("OPS_RESTORE_CHECKSUM_FAILED", f"checksum mismatch for '{rel}'")

    _validate_restore_manifest_contract(snapshot, manifest, checksums)
    return manifest


def verify_backup_by_id(settings: BackupSettings, backup_id: str) -> Path:
    snapshot = resolve_snapshot_from_index(settings, backup_id)
    try:
        verify_backup_snapshot(snapshot)
        LOGGER.info("ops.backup.verify.success", extra={"backup_id": backup_id, "error_code": ""})
        return snapshot
    except Exception as exc:
        error_code = exc.code if isinstance(exc, OpsRestoreError) else type(exc).__name__
        LOGGER.exception("ops.backup.verify.failure", extra={"backup_id": backup_id, "error_code": error_code})
        raise


def _quarantine_path(quarantine_root: Path, target: Path) -> Path:
    return quarantine_root / quote(str(target.resolve()), safe="")


def _sqlite_integrity_check(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("PRAGMA integrity_check").fetchone()
    if not row or row[0] != "ok":
        raise OpsRestoreError("OPS_RESTORE_DB_INTEGRITY_FAILED", "sqlite integrity_check failed")


def _manifest_restore_mappings(manifest: dict, artifact_key: str) -> list[tuple[Path, Path]]:
    artifacts = manifest.get("artifacts", {}).get(artifact_key, [])
    mappings: list[tuple[Path, Path]] = []
    for item in artifacts:
        if not isinstance(item, dict) or "source" not in item or "artifact" not in item:
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"{artifact_key} manifest mappings are invalid")
        if not isinstance(item["source"], str) or not item["source"]:
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"{artifact_key} manifest source is invalid")
        if not isinstance(item["artifact"], str) or not item["artifact"]:
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"{artifact_key} manifest artifact is invalid")
        mappings.append((Path(item["source"]), Path(item["artifact"])))
    return mappings


def _validate_restore_manifest_contract(snapshot: Path, manifest: dict, checksums: dict[str, str]) -> None:
    restore_targets = manifest.get("restore_targets")
    if not isinstance(restore_targets, dict):
        raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", "manifest restore_targets are missing or invalid")

    db_target_value = restore_targets.get("FACTORY_DB_PATH")
    if not isinstance(db_target_value, str) or not db_target_value:
        raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", "manifest restore target FACTORY_DB_PATH is missing")

    targets_by_kind = {
        "env": "FACTORY_ENV_FILES",
        "config": "FACTORY_BACKUP_CONFIG_PATHS",
        "exports": "FACTORY_BACKUP_EXPORT_DIRS",
    }
    for target_key in targets_by_kind.values():
        values = restore_targets.get(target_key)
        if not isinstance(values, list) or not all(isinstance(item, str) and item for item in values):
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"manifest restore target {target_key} is invalid")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", "snapshot artifacts are missing or invalid")

    db_artifact = artifacts.get("db")
    if not isinstance(db_artifact, str) or not db_artifact:
        raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", "db artifact is missing from manifest")

    manifest_items = {
        item.get("stored_path"): item
        for item in manifest.get("items", [])
        if isinstance(item, dict) and isinstance(item.get("stored_path"), str)
    }

    all_artifacts: list[tuple[str, Path]] = [("db", Path(db_artifact))]
    for artifact_key, target_key in targets_by_kind.items():
        mappings = _manifest_restore_mappings(manifest, artifact_key)
        if len(mappings) != len(restore_targets[target_key]):
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"manifest {artifact_key} restore mapping count is invalid")
        mapped_sources = [str(source) for source, _ in mappings]
        if mapped_sources != restore_targets[target_key]:
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"manifest {artifact_key} restore mappings do not match restore targets")
        all_artifacts.extend((artifact_key, artifact) for _, artifact in mappings)

    for kind, artifact in all_artifacts:
        artifact_rel = artifact.as_posix()
        artifact_path = snapshot / artifact
        if not artifact_path.exists():
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"snapshot artifact missing: {artifact_path}")
        item = manifest_items.get(artifact_rel)
        if not item:
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"manifest item missing for artifact '{artifact_rel}'")

        if artifact_path.is_file():
            if checksums.get(artifact_rel) != item.get("sha256"):
                raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"manifest checksum mismatch for artifact '{artifact_rel}'")
        else:
            expected_size, expected_sha = _directory_artifact_metadata(snapshot, artifact_rel)
            if item.get("size_bytes") != expected_size or item.get("sha256") != expected_sha:
                raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"manifest directory metadata mismatch for artifact '{artifact_rel}'")


def restore_snapshot(settings: BackupSettings, snapshot: Path, *, services_stopped_file: Path) -> dict:
    if not services_stopped_file.exists():
        raise OpsRestoreError("OPS_RESTORE_SERVICES_RUNNING", "restore requires services to be stopped before file replacement")

    manifest = verify_backup_snapshot(snapshot)
    restore_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    quarantine_root = settings.backup_dir / "quarantine" / restore_id
    try:
        quarantine_root.mkdir(parents=True, exist_ok=False)
    except Exception as exc:
        raise OpsRestoreError("OPS_RESTORE_QUARANTINE_FAILED", str(exc)) from exc

    restore_targets = manifest.get("restore_targets", {})
    db_target_value = restore_targets.get("FACTORY_DB_PATH")
    if not isinstance(db_target_value, str) or not db_target_value:
        raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", "manifest restore target FACTORY_DB_PATH is missing")
    db_target = Path(db_target_value)
    db_src = snapshot / Path(manifest["artifacts"]["db"])
    restore_pairs = [(db_target, db_src)]
    restore_pairs.extend((source, snapshot / artifact) for source, artifact in _manifest_restore_mappings(manifest, "env"))
    restore_pairs.extend((source, snapshot / artifact) for source, artifact in _manifest_restore_mappings(manifest, "config"))
    restore_pairs.extend((source, snapshot / artifact) for source, artifact in _manifest_restore_mappings(manifest, "exports"))

    moved_targets: list[Path] = []
    for target, src in restore_pairs:
        if not src.exists():
            raise OpsRestoreError("OPS_RESTORE_MANIFEST_INVALID", f"snapshot artifact missing: {src}")
        if target.exists():
            qdst = _quarantine_path(quarantine_root, target)
            qdst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(target), str(qdst))
            moved_targets.append(target)

    db_target.parent.mkdir(parents=True, exist_ok=True)
    db_tmp = db_target.with_name(f".{db_target.name}.restore_tmp")
    shutil.copy2(db_src, db_tmp)
    _chmod600(db_tmp, generated=True)
    os.replace(db_tmp, db_target)
    _chmod600(db_target, generated=True)

    for target, src in restore_pairs[1:]:
        if src.is_dir():
            _copy_dir(src, target)
        else:
            _copy_file(src, target)

    _sqlite_integrity_check(db_target)
    return {"restore_id": restore_id, "quarantine_dir": quarantine_root, "restored": len(restore_pairs), "moved": len(moved_targets)}
