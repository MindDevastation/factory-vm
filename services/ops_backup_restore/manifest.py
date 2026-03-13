from __future__ import annotations

import socket
import sqlite3
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from services.ops_backup_restore.models import BackupScope, ManifestItem

MANIFEST_VERSION = "factory_backup/1"


def resolve_app_version(*, env: dict[str, str] | None = None, repo_root: Path | None = None) -> str:
    source = {} if env is None else env
    app_version = source.get("FACTORY_APP_VERSION", "").strip()
    if app_version:
        return app_version

    root = repo_root or Path.cwd()
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        sha = ""
    return sha or "unknown"


def resolve_schema_version(db_path: Path) -> str:
    if not db_path.exists():
        return "unknown"
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute("PRAGMA user_version").fetchone()
    except sqlite3.Error:
        return "unknown"
    if not row:
        return "unknown"
    return str(row[0])


def build_manifest(
    *,
    backup_id: str,
    scope: BackupScope,
    items: list[ManifestItem],
    created_at: datetime | None = None,
    app_version: str | None = None,
    schema_version: str | None = None,
    hostname: str | None = None,
) -> dict:
    created = (created_at or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z")
    resolved_app_version = app_version or resolve_app_version()
    resolved_schema_version = schema_version or resolve_schema_version(scope.db_path)
    resolved_hostname = hostname or socket.gethostname()

    return {
        "manifest_version": MANIFEST_VERSION,
        "backup_id": backup_id,
        "created_at": created,
        "app_version": resolved_app_version,
        "schema_version": resolved_schema_version,
        "hostname": resolved_hostname,
        "scope": {
            "db_included": True,
            "env_files_count": len(scope.env_files),
            "config_paths_count": len(scope.config_paths),
            "export_paths_count": len(scope.export_paths),
        },
        "items": [
            {
                "kind": item.kind,
                "source_path": item.source_path,
                "stored_path": item.stored_path,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "contains_secrets": item.contains_secrets,
            }
            for item in items
        ],
        "status": "SUCCESS",
    }
