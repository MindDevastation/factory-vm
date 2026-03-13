from __future__ import annotations

import json
import os
import sqlite3
import stat
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

from services.ops_backup_restore.index import upsert_snapshot, write_index, write_latest_successful
from services.ops_backup_restore.manifest import build_manifest
from services.ops_backup_restore.models import BackupScope, ManifestItem
from services.ops_backup_restore.paths import (
    checksums_path,
    generate_backup_id,
    index_path,
    latest_successful_path,
    manifest_path,
    snapshot_dir,
)
from services.ops_backup_restore.scope import resolve_backup_scope


class OpsBackupRestoreP0S1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_scope_resolution_uses_required_and_colon_allowlists(self) -> None:
        scope = resolve_backup_scope(
            {
                "FACTORY_BACKUP_DIR": str(self.root / "backups"),
                "FACTORY_DB_PATH": str(self.root / "data" / "app.sqlite3"),
                "FACTORY_ENV_FILES": "/a/.env:/b/.env",
                "FACTORY_BACKUP_CONFIG_PATHS": "/configs/a.yaml:/configs/b",
                "FACTORY_BACKUP_EXPORT_DIRS": "/exports/one:/exports/two",
            }
        )
        self.assertEqual(scope.env_files, (Path("/a/.env"), Path("/b/.env")))
        self.assertEqual(scope.config_paths, (Path("/configs/a.yaml"), Path("/configs/b")))
        self.assertEqual(scope.export_paths, (Path("/exports/one"), Path("/exports/two")))

    def test_scope_resolution_requires_backup_and_db_path(self) -> None:
        with self.assertRaisesRegex(ValueError, "FACTORY_BACKUP_DIR"):
            resolve_backup_scope({"FACTORY_DB_PATH": "/tmp/db.sqlite3"})
        with self.assertRaisesRegex(ValueError, "FACTORY_DB_PATH"):
            resolve_backup_scope({"FACTORY_BACKUP_DIR": "/tmp/backups"})

    def test_backup_id_and_snapshot_layout(self) -> None:
        backup_root = self.root / "backups"
        backup_id = generate_backup_id(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        self.assertEqual(backup_id, "20260102T030405Z")
        self.assertEqual(snapshot_dir(backup_root, backup_id), backup_root / "snapshots" / backup_id)
        self.assertEqual(manifest_path(backup_root, backup_id), backup_root / "snapshots" / backup_id / "manifest.json")
        self.assertEqual(checksums_path(backup_root, backup_id), backup_root / "snapshots" / backup_id / "checksums.sha256")
        self.assertEqual(index_path(backup_root), backup_root / "index.json")
        self.assertEqual(latest_successful_path(backup_root), backup_root / "latest_successful")

    def test_manifest_builder_schema(self) -> None:
        db_path = self.root / "app.sqlite3"
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA user_version = 7")

        scope = BackupScope(
            backup_dir=self.root / "backups",
            db_path=db_path,
            env_files=(Path("/safe/.env"),),
            config_paths=(Path("/cfg/settings.yaml"),),
            export_paths=(Path("/exports/daily"),),
        )
        items = [
            ManifestItem(
                kind="env",
                source_path="/safe/.env",
                stored_path="env/safe.env",
                size_bytes=10,
                sha256="abc123",
                contains_secrets=True,
            )
        ]

        manifest = build_manifest(
            backup_id="20260102T030405Z",
            scope=scope,
            items=items,
            created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            app_version="1.2.3",
            hostname="host-a",
        )

        self.assertEqual(manifest["manifest_version"], "factory_backup/1")
        self.assertEqual(manifest["schema_version"], "7")
        self.assertEqual(manifest["scope"]["db_included"], True)
        self.assertEqual(manifest["scope"]["env_files_count"], 1)
        self.assertEqual(manifest["status"], "SUCCESS")
        self.assertNotIn("TOKEN=", json.dumps(manifest))

    def test_manifest_builder_uses_factory_app_version_from_environment(self) -> None:
        db_path = self.root / "app.sqlite3"
        scope = BackupScope(
            backup_dir=self.root / "backups",
            db_path=db_path,
            env_files=tuple(),
            config_paths=tuple(),
            export_paths=tuple(),
        )

        with mock.patch.dict(os.environ, {"FACTORY_APP_VERSION": "9.9.9"}, clear=False):
            manifest = build_manifest(
                backup_id="20260102T030405Z",
                scope=scope,
                items=[],
                created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            )

        self.assertEqual(manifest["app_version"], "9.9.9")

    def test_index_upsert_and_latest_successful(self) -> None:
        backup_root = self.root / "backups"
        payload = upsert_snapshot(
            backup_root=backup_root,
            backup_id="20260102T030405Z",
            created_at="2026-01-02T03:04:05Z",
        )
        payload = upsert_snapshot(
            backup_root=backup_root,
            backup_id="20260103T030405Z",
            created_at="2026-01-03T03:04:05Z",
            retention_labels=["latest", "daily"],
        )

        out = write_index(backup_root, payload)
        latest = write_latest_successful(backup_root, "20260103T030405Z")

        data = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(data["index_version"], "factory_backup_index/1")
        self.assertEqual(data["snapshots"][0]["backup_id"], "20260103T030405Z")
        self.assertEqual(
            data["snapshots"][0]["manifest_path"],
            "snapshots/20260103T030405Z/manifest.json",
        )
        self.assertEqual(latest.read_text(encoding="utf-8"), "20260103T030405Z\n")
        self.assertEqual(stat.S_IMODE(out.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(latest.stat().st_mode), 0o600)


if __name__ == "__main__":
    unittest.main()
