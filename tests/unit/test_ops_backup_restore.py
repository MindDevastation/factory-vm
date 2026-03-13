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


    def test_restore_uses_exact_configured_env_target_path(self) -> None:
        external_env = self.root / "runtime" / "secrets" / "factory.env"
        external_env.parent.mkdir(parents=True, exist_ok=True)
        external_env.write_text("TOKEN=before\n", encoding="utf-8")

        settings = BackupSettings.from_env(
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": str(external_env),
                "FACTORY_BACKUP_CONFIG_PATHS": "",
                "FACTORY_BACKUP_EXPORT_DIRS": "",
            }
        )
        snapshot = create_backup(settings, now=datetime(2026, 2, 2, 0, 0, 0, tzinfo=UTC))

        external_env.unlink()
        stopped = self.root / "services.stopped"
        stopped.write_text("ok", encoding="utf-8")

        restore_snapshot(settings, snapshot, services_stopped_file=stopped)
        self.assertTrue(external_env.exists())
        self.assertEqual(external_env.read_text(encoding="utf-8"), "TOKEN=before\n")

    def test_restore_handles_duplicate_basenames_without_collision(self) -> None:
        env_a = self.root / "envs" / "tenant-a" / "shared.env"
        env_b = self.root / "envs" / "tenant-b" / "shared.env"
        env_a.parent.mkdir(parents=True, exist_ok=True)
        env_b.parent.mkdir(parents=True, exist_ok=True)
        env_a.write_text("A=1\n", encoding="utf-8")
        env_b.write_text("B=2\n", encoding="utf-8")

        config_a = self.root / "config" / "alpha" / "shared"
        config_b = self.root / "config" / "beta" / "shared"
        config_a.mkdir(parents=True, exist_ok=True)
        config_b.mkdir(parents=True, exist_ok=True)
        (config_a / "settings.json").write_text('{"name":"alpha"}', encoding="utf-8")
        (config_b / "settings.json").write_text('{"name":"beta"}', encoding="utf-8")

        export_a = self.root / "exports" / "batch-a" / "shared"
        export_b = self.root / "exports" / "batch-b" / "shared"
        export_a.mkdir(parents=True, exist_ok=True)
        export_b.mkdir(parents=True, exist_ok=True)
        (export_a / "report.txt").write_text("export-a", encoding="utf-8")
        (export_b / "report.txt").write_text("export-b", encoding="utf-8")

        settings = BackupSettings.from_env(
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": f"{env_a},{env_b}",
                "FACTORY_BACKUP_CONFIG_PATHS": f"{config_a},{config_b}",
                "FACTORY_BACKUP_EXPORT_DIRS": f"{export_a},{export_b}",
            }
        )
        snapshot = create_backup(settings, now=datetime(2026, 2, 3, 0, 0, 0, tzinfo=UTC))

        env_a.write_text("A=mutated\n", encoding="utf-8")
        env_b.write_text("B=mutated\n", encoding="utf-8")
        (config_a / "settings.json").write_text('{"name":"mutated-a"}', encoding="utf-8")
        (config_b / "settings.json").write_text('{"name":"mutated-b"}', encoding="utf-8")
        (export_a / "report.txt").write_text("mutated-a", encoding="utf-8")
        (export_b / "report.txt").write_text("mutated-b", encoding="utf-8")

        stopped = self.root / "services.stopped"
        stopped.write_text("ok", encoding="utf-8")

        restore_snapshot(settings, snapshot, services_stopped_file=stopped)

        self.assertEqual(env_a.read_text(encoding="utf-8"), "A=1\n")
        self.assertEqual(env_b.read_text(encoding="utf-8"), "B=2\n")
        self.assertEqual((config_a / "settings.json").read_text(encoding="utf-8"), '{"name":"alpha"}')
        self.assertEqual((config_b / "settings.json").read_text(encoding="utf-8"), '{"name":"beta"}')
        self.assertEqual((export_a / "report.txt").read_text(encoding="utf-8"), "export-a")
        self.assertEqual((export_b / "report.txt").read_text(encoding="utf-8"), "export-b")

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
