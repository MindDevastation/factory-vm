from __future__ import annotations

import hashlib
import json
import io
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

from scripts.ops_backup_restore import main as backup_restore_main
from services.ops.backup_restore import (
    BackupSettings,
    OpsRestoreError,
    apply_retention,
    create_backup,
    resolve_snapshot_from_index,
    restore_snapshot,
    verify_backup_by_id,
)


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
        self.assertEqual(manifest["status"], "SUCCESS")
        self.assertNotIn("super-secret", json.dumps(manifest))

        db_backup = snapshot / manifest["artifacts"]["db"]
        with sqlite3.connect(db_backup) as conn:
            row = conn.execute("select count(*) from jobs").fetchone()
        self.assertEqual(row[0], 1)

        backup_mode = (snapshot / "manifest.json").stat().st_mode & 0o777
        self.assertEqual(backup_mode, 0o600)
        root_mode = self.backup_dir.stat().st_mode & 0o777
        self.assertEqual(root_mode, 0o700)

    def test_create_backup_uses_p0s1_canonical_layout_and_index_helpers(self) -> None:
        settings = self._settings()
        snapshot = create_backup(settings, now=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))

        self.assertEqual(snapshot, self.backup_dir / "snapshots" / "20260102T030405Z")

        manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["manifest_version"], "factory_backup/1")
        self.assertEqual(manifest["status"], "SUCCESS")
        self.assertIn("scope", manifest)
        self.assertIn("env_files_count", manifest["scope"])
        self.assertNotIn("FACTORY_DB_PATH", manifest["scope"])
        self.assertIn("restore_targets", manifest)

        index = json.loads((self.backup_dir / "index.json").read_text(encoding="utf-8"))
        self.assertEqual(index["index_version"], "factory_backup_index/1")
        self.assertEqual(index["snapshots"][0]["manifest_path"], "snapshots/20260102T030405Z/manifest.json")
        self.assertEqual((self.backup_dir / "latest_successful").read_text(encoding="utf-8"), "20260102T030405Z\n")


    def test_create_backup_writes_manifest_checksums_and_db_snapshot(self) -> None:
        cfg = self.root / "configs" / "app.yaml"
        cfg.parent.mkdir(parents=True, exist_ok=True)
        cfg.write_text("mode: prod\n", encoding="utf-8")

        export_dir = self.root / "exports" / "daily"
        export_dir.mkdir(parents=True, exist_ok=True)
        export_file = export_dir / "jobs.json"
        export_file.write_text('{"jobs":1}\n', encoding="utf-8")

        settings = BackupSettings.from_env(
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": str(self.deploy / "env"),
                "FACTORY_BACKUP_CONFIG_PATHS": str(cfg),
                "FACTORY_BACKUP_EXPORT_DIRS": str(export_dir),
            }
        )

        snapshot = create_backup(settings, now=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["artifacts"]["db"], "db/app.sqlite3")

        checksum_lines = (snapshot / "checksums.sha256").read_text(encoding="utf-8").strip().splitlines()
        self.assertGreaterEqual(len(checksum_lines), 3)

        checksum_map = {}
        for line in checksum_lines:
            digest, rel = line.split("  ", 1)
            checksum_map[rel] = digest

        for rel, digest in checksum_map.items():
            payload = (snapshot / rel).read_bytes()
            self.assertEqual(hashlib.sha256(payload).hexdigest(), digest)

        manifest_items = {item["stored_path"]: item["sha256"] for item in manifest["items"]}
        self.assertEqual(manifest_items["db/app.sqlite3"], checksum_map["db/app.sqlite3"])

    def test_create_backup_directory_artifact_manifest_metadata_is_non_empty_and_consistent(self) -> None:
        config_dir = self.root / "configs" / "bundle"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "a.yaml").write_text("a: 1\n", encoding="utf-8")
        (config_dir / "nested").mkdir(parents=True, exist_ok=True)
        (config_dir / "nested" / "b.yaml").write_text("b: 2\n", encoding="utf-8")

        settings = BackupSettings.from_env(
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": str(self.deploy / "env"),
                "FACTORY_BACKUP_CONFIG_PATHS": str(config_dir),
                "FACTORY_BACKUP_EXPORT_DIRS": "",
            }
        )

        snapshot = create_backup(settings, now=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
        checksum_lines = (snapshot / "checksums.sha256").read_text(encoding="utf-8").strip().splitlines()

        checksum_map = {}
        for line in checksum_lines:
            digest, rel = line.split("  ", 1)
            checksum_map[rel] = digest

        config_artifact = next(item for item in manifest["artifacts"]["config"] if item["source"] == str(config_dir))["artifact"]
        config_manifest_item = next(item for item in manifest["items"] if item["stored_path"] == config_artifact)
        self.assertGreater(config_manifest_item["size_bytes"], 0)
        self.assertTrue(config_manifest_item["sha256"])

        files = sorted(path for path in (snapshot / config_artifact).rglob("*") if path.is_file())
        expected_size = sum(path.stat().st_size for path in files)
        expected_digest = hashlib.sha256()
        for path in files:
            rel = path.relative_to(snapshot).as_posix()
            expected_digest.update(f"{rel}\0{path.stat().st_size}\0{checksum_map[rel]}\n".encode("utf-8"))

        self.assertEqual(config_manifest_item["size_bytes"], expected_size)
        self.assertEqual(config_manifest_item["sha256"], expected_digest.hexdigest())

    def test_create_backup_failure_does_not_update_index_or_latest(self) -> None:
        settings = self._settings()
        first = create_backup(settings, now=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC))
        first_index = (self.backup_dir / "index.json").read_text(encoding="utf-8")
        first_latest = (self.backup_dir / "latest_successful").read_text(encoding="utf-8")

        with mock.patch("services.ops.backup_restore._copy_file", side_effect=RuntimeError("copy_failed")):
            with self.assertRaises(RuntimeError):
                create_backup(settings, now=datetime(2026, 1, 2, 0, 0, 0, tzinfo=UTC))

        self.assertTrue(first.exists())
        self.assertEqual((self.backup_dir / "index.json").read_text(encoding="utf-8"), first_index)
        self.assertEqual((self.backup_dir / "latest_successful").read_text(encoding="utf-8"), first_latest)
        self.assertTrue((self.backup_dir / "snapshots" / "20260102T000000Z.tmp").exists())

    def test_repeated_backups_keep_previous_snapshot_content_intact(self) -> None:
        settings = self._settings()
        snap1 = create_backup(settings, now=datetime(2026, 1, 2, 0, 0, 0, tzinfo=UTC))

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("insert into jobs(name) values (?)", ("after-first",))
            conn.commit()

        snap2 = create_backup(settings, now=datetime(2026, 1, 3, 0, 0, 0, tzinfo=UTC))
        self.assertNotEqual(snap1, snap2)

        with sqlite3.connect(snap1 / "db" / "app.sqlite3") as conn:
            count1 = conn.execute("select count(*) from jobs").fetchone()[0]
        with sqlite3.connect(snap2 / "db" / "app.sqlite3") as conn:
            count2 = conn.execute("select count(*) from jobs").fetchone()[0]

        self.assertEqual(count1, 1)
        self.assertEqual(count2, 2)

    def test_retention_keeps_policies_and_latest(self) -> None:
        settings = self._settings()
        start = datetime(2026, 3, 31, 0, 0, 0, tzinfo=UTC)
        created: list[str] = []
        for idx in range(20):
            dt = start - timedelta(days=idx)
            snap = create_backup(settings, now=dt)
            created.append(snap.name)

        snapshots_root = self.backup_dir / "snapshots"
        kept = sorted([p.name for p in snapshots_root.iterdir() if p.is_dir()])
        self.assertTrue(created[0] in kept)
        self.assertLess(len(kept), len(created))
        self.assertGreaterEqual(len(kept), 7)

        # retention called after successful backup, can still be run idempotently
        removed = apply_retention(self.backup_dir)
        self.assertIsInstance(removed, list)

    def test_create_backup_retention_rebuilds_index_without_stale_entries(self) -> None:
        settings = self._settings()
        start = datetime(2026, 3, 31, 0, 0, 0, tzinfo=UTC)
        for idx in range(20):
            create_backup(settings, now=start - timedelta(days=idx))

        index = json.loads((self.backup_dir / "index.json").read_text(encoding="utf-8"))
        snapshot_dirs = {
            node.name for node in (self.backup_dir / "snapshots").iterdir() if node.is_dir() and not node.name.endswith(".tmp")
        }
        indexed_ids = {item["backup_id"] for item in index["snapshots"]}

        self.assertSetEqual(indexed_ids, snapshot_dirs)
        self.assertEqual((self.backup_dir / "latest_successful").read_text(encoding="utf-8"), f"{max(snapshot_dirs)}\n")



    def test_from_env_scope_contract_uses_canonical_colon_separator(self) -> None:
        settings = BackupSettings.from_env(
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": f"{self.root / 'a.env'}:{self.root / 'b.env'}",
                "FACTORY_BACKUP_CONFIG_PATHS": "",
                "FACTORY_BACKUP_EXPORT_DIRS": "",
            }
        )
        self.assertEqual(settings.env_files, (self.root / "a.env", self.root / "b.env"))

    def test_from_env_scope_contract_does_not_use_legacy_comma_separator(self) -> None:
        settings = BackupSettings.from_env(
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": f"{self.root / 'a.env'},{self.root / 'b.env'}",
                "FACTORY_BACKUP_CONFIG_PATHS": "",
                "FACTORY_BACKUP_EXPORT_DIRS": "",
            }
        )
        self.assertEqual(settings.env_files, (Path(f"{self.root / 'a.env'},{self.root / 'b.env'}"),))

    def test_from_env_requires_factory_db_path(self) -> None:
        with self.assertRaisesRegex(ValueError, "FACTORY_DB_PATH"):
            BackupSettings.from_env(
                {
                    "FACTORY_BACKUP_DIR": str(self.backup_dir),
                    "FACTORY_ENV_FILES": "",
                    "FACTORY_BACKUP_CONFIG_PATHS": "",
                    "FACTORY_BACKUP_EXPORT_DIRS": "",
                }
            )

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
                "FACTORY_ENV_FILES": f"{env_a}:{env_b}",
                "FACTORY_BACKUP_CONFIG_PATHS": f"{config_a}:{config_b}",
                "FACTORY_BACKUP_EXPORT_DIRS": f"{export_a}:{export_b}",
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

    def test_verify_command_succeeds_for_valid_snapshot(self) -> None:
        settings = self._settings()
        snapshot = create_backup(settings, now=datetime(2026, 2, 4, 0, 0, 0, tzinfo=UTC))

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout, mock.patch.dict(
            "os.environ",
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": str(self.deploy / "env"),
                "FACTORY_BACKUP_CONFIG_PATHS": "",
                "FACTORY_BACKUP_EXPORT_DIRS": "",
            },
            clear=False,
        ):
            code = backup_restore_main(["backup", "verify", "--backup-id", snapshot.name])

        self.assertEqual(code, 0)
        self.assertIn("verify_ok", stdout.getvalue())

    def test_verify_fails_for_tampered_snapshot(self) -> None:
        settings = self._settings()
        snapshot = create_backup(settings, now=datetime(2026, 2, 5, 0, 0, 0, tzinfo=UTC))
        (snapshot / "db" / "app.sqlite3").write_bytes(b"tampered")

        with self.assertRaises(OpsRestoreError) as exc:
            verify_backup_by_id(settings, snapshot.name)
        self.assertEqual(exc.exception.code, "OPS_RESTORE_CHECKSUM_FAILED")

    def test_restore_over_existing_state_moves_previous_files_to_quarantine(self) -> None:
        settings = self._settings()
        snapshot = create_backup(settings, now=datetime(2026, 2, 6, 0, 0, 0, tzinfo=UTC))
        stopped = self.backup_dir / ".services_stopped"
        stopped.parent.mkdir(parents=True, exist_ok=True)
        stopped.write_text("ok", encoding="utf-8")

        self.db_path.write_text("old", encoding="utf-8")
        (self.deploy / "env").write_text("OLD=1\n", encoding="utf-8")

        summary = restore_snapshot(settings, snapshot, services_stopped_file=stopped)
        quarantine_dir = Path(summary["quarantine_dir"])
        self.assertTrue(quarantine_dir.exists())
        self.assertTrue(any(path.is_file() for path in quarantine_dir.rglob("*")))

    def test_restore_failure_does_not_claim_success(self) -> None:
        settings = self._settings()
        snapshot = create_backup(settings, now=datetime(2026, 2, 7, 0, 0, 0, tzinfo=UTC))
        (snapshot / "db" / "app.sqlite3").write_bytes(b"not-a-sqlite-db")
        stopped = self.backup_dir / ".services_stopped"
        stopped.parent.mkdir(parents=True, exist_ok=True)
        stopped.write_text("ok", encoding="utf-8")

        with mock.patch("scripts.ops_backup_restore.LOGGER") as logger, mock.patch(
            "sys.stdout", new_callable=io.StringIO
        ) as stdout, mock.patch.dict(
            "os.environ",
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": str(self.deploy / "env"),
                "FACTORY_BACKUP_CONFIG_PATHS": "",
                "FACTORY_BACKUP_EXPORT_DIRS": "",
                "FACTORY_SERVICES_STOPPED_FILE": str(stopped),
            },
            clear=False,
        ):
            code = backup_restore_main(["restore", "--backup-id", snapshot.name])

        self.assertNotEqual(code, 0)
        self.assertNotIn("restore_ok", stdout.getvalue())
        logger.info.assert_any_call("ops.restore.start", extra={"backup_id": snapshot.name})
        failure_calls = [call for call in logger.exception.call_args_list if call.args[0] == "ops.restore.failure"]
        self.assertTrue(failure_calls)

    def test_quarantine_strategy_avoids_basename_collisions(self) -> None:
        settings = self._settings()
        env_a = self.root / "one" / "same.env"
        env_b = self.root / "two" / "same.env"
        env_a.parent.mkdir(parents=True, exist_ok=True)
        env_b.parent.mkdir(parents=True, exist_ok=True)
        env_a.write_text("A=1\n", encoding="utf-8")
        env_b.write_text("B=2\n", encoding="utf-8")
        settings = BackupSettings.from_env(
            {
                "FACTORY_DB_PATH": str(self.db_path),
                "FACTORY_BACKUP_DIR": str(self.backup_dir),
                "FACTORY_ENV_FILES": f"{env_a}:{env_b}",
                "FACTORY_BACKUP_CONFIG_PATHS": "",
                "FACTORY_BACKUP_EXPORT_DIRS": "",
            }
        )
        snapshot = create_backup(settings, now=datetime(2026, 2, 8, 0, 0, 0, tzinfo=UTC))
        stopped = self.backup_dir / ".services_stopped"
        stopped.parent.mkdir(parents=True, exist_ok=True)
        stopped.write_text("ok", encoding="utf-8")

        env_a.write_text("A=mutated\n", encoding="utf-8")
        env_b.write_text("B=mutated\n", encoding="utf-8")
        summary = restore_snapshot(settings, snapshot, services_stopped_file=stopped)

        quarantine_files = [p for p in Path(summary["quarantine_dir"]).rglob("*") if p.is_file()]
        self.assertGreaterEqual(len(quarantine_files), 2)


if __name__ == "__main__":
    unittest.main()
