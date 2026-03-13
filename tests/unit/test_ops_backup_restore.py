from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from services.ops.backup_restore import BackupSettings, apply_retention, create_backup, restore_snapshot


class TestOpsBackupRestore(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        self.root = Path(self.td.name)
        self.db_path = self.root / "data" / "factory.sqlite3"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("create table jobs(id integer primary key, name text)")
            conn.execute("insert into jobs(name) values (?)", ("seed",))
            conn.commit()

        self.deploy = self.root / "deploy"
        self.deploy.mkdir(parents=True, exist_ok=True)
        (self.deploy / "env").write_text("FACTORY_BASIC_AUTH_PASS=super-secret\n", encoding="utf-8")

        self.backup_dir = self.root / "backups"

    def tearDown(self) -> None:
        self.td.cleanup()

    def _settings(self) -> BackupSettings:
        return BackupSettings.from_env(
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": str(self.deploy / "env"),
                "FACTORY_BACKUP_CONFIG_PATHS": "",
                "FACTORY_BACKUP_EXPORT_DIRS": "",
            }
        )

    def test_create_backup_uses_sqlite_online_backup_and_redacted_manifest(self) -> None:
        settings = self._settings()
        snapshot = create_backup(settings, now=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))

        manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "success")
        self.assertNotIn("super-secret", json.dumps(manifest))

        db_backup = snapshot / manifest["artifacts"]["db"]
        with sqlite3.connect(db_backup) as conn:
            row = conn.execute("select count(*) from jobs").fetchone()
        self.assertEqual(row[0], 1)

        backup_mode = (snapshot / "manifest.json").stat().st_mode & 0o777
        self.assertEqual(backup_mode, 0o600)
        root_mode = self.backup_dir.stat().st_mode & 0o777
        self.assertEqual(root_mode, 0o700)

    def test_retention_keeps_policies_and_latest(self) -> None:
        settings = self._settings()
        start = datetime(2026, 3, 31, 0, 0, 0, tzinfo=UTC)
        created: list[str] = []
        for idx in range(20):
            dt = start - timedelta(days=idx)
            snap = create_backup(settings, now=dt)
            created.append(snap.name)

        kept = sorted([p.name for p in self.backup_dir.iterdir() if p.is_dir()])
        self.assertTrue(created[0] in kept)
        self.assertLess(len(kept), len(created))
        self.assertGreaterEqual(len(kept), 7)

        # retention called after successful backup, can still be run idempotently
        removed = apply_retention(self.backup_dir)
        self.assertIsInstance(removed, list)

    def test_restore_requires_services_stopped_file(self) -> None:
        settings = self._settings()
        snapshot = create_backup(settings, now=datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC))

        with self.assertRaises(RuntimeError):
            restore_snapshot(settings, snapshot, services_stopped_file=self.root / "missing.stop")

        stopped = self.root / "services.stopped"
        stopped.write_text("ok", encoding="utf-8")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("delete from jobs")
            conn.commit()

        restore_snapshot(settings, snapshot, services_stopped_file=stopped)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("select count(*) from jobs").fetchone()
        self.assertEqual(row[0], 1)


if __name__ == "__main__":
    unittest.main()
