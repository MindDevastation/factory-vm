from __future__ import annotations

import json
import os
from pathlib import Path

from services.ops_backup_restore.paths import index_path, latest_successful_path, manifest_path, snapshots_root

INDEX_VERSION = "factory_backup_index/1"


def load_index(backup_root: Path) -> dict:
    path = index_path(backup_root)
    if not path.exists():
        return {"index_version": INDEX_VERSION, "snapshots": []}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("index_version") != INDEX_VERSION:
        payload["index_version"] = INDEX_VERSION
    payload.setdefault("snapshots", [])
    return payload


def upsert_snapshot(
    *,
    backup_root: Path,
    backup_id: str,
    created_at: str,
    retention_labels: list[str] | None = None,
) -> dict:
    idx = load_index(backup_root)
    entry = _snapshot_index_entry(
        backup_root=backup_root,
        backup_id=backup_id,
        created_at=created_at,
        retention_labels=retention_labels,
    )

    filtered = [item for item in idx["snapshots"] if item.get("backup_id") != backup_id]
    filtered.append(entry)
    filtered.sort(key=lambda item: item.get("backup_id", ""), reverse=True)
    idx["snapshots"] = filtered
    return idx


def _snapshot_index_entry(*, backup_root: Path, backup_id: str, created_at: str, retention_labels: list[str] | None) -> dict:
    rel_manifest = manifest_path(backup_root, backup_id).relative_to(backup_root).as_posix()
    labels = list(retention_labels or ["latest", "daily"])
    return {
        "backup_id": backup_id,
        "created_at": created_at,
        "status": "SUCCESS",
        "manifest_path": rel_manifest,
        "retention_labels": labels,
    }


def rebuild_index_from_snapshots(backup_root: Path) -> dict:
    payload = {"index_version": INDEX_VERSION, "snapshots": []}
    root = snapshots_root(backup_root)
    if not root.exists():
        return payload

    for snap in sorted([node for node in root.iterdir() if node.is_dir()], key=lambda node: node.name, reverse=True):
        manifest_file = snap / "manifest.json"
        if not manifest_file.exists():
            continue
        with manifest_file.open("r", encoding="utf-8") as handle:
            manifest = json.load(handle)
        if manifest.get("status") != "SUCCESS":
            continue
        payload["snapshots"].append(
            _snapshot_index_entry(
                backup_root=backup_root,
                backup_id=snap.name,
                created_at=manifest.get("created_at", ""),
                retention_labels=None,
            )
        )

    return payload


def write_index(backup_root: Path, payload: dict) -> Path:
    path = index_path(backup_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(path, 0o600)
    return path


def write_latest_successful(backup_root: Path, backup_id: str) -> Path:
    path = latest_successful_path(backup_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{backup_id}\n", encoding="utf-8")
    os.chmod(path, 0o600)
    return path
