from __future__ import annotations

import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.ops_backup_schedule import main as schedule_main


class OpsBackupScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.backup_dir = Path(self.tmpdir.name) / "backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    def test_run_create_and_verify_success(self) -> None:
        (self.backup_dir / "latest_successful").write_text("20260203T040506Z\n", encoding="utf-8")

        with (
            mock.patch.dict("os.environ", {"FACTORY_BACKUP_DIR": str(self.backup_dir)}, clear=False),
            mock.patch("scripts.ops_backup_schedule.backup_restore_main", side_effect=[0, 0]) as cli,
            mock.patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            code = schedule_main(["run"])

        self.assertEqual(code, 0)
        self.assertEqual(
            cli.call_args_list,
            [
                mock.call(["backup", "create"]),
                mock.call(["backup", "verify", "--backup-id", "20260203T040506Z"]),
            ],
        )
        self.assertIn("scheduled_backup_ok backup_id=20260203T040506Z", stdout.getvalue())

    def test_run_skip_verify_only_calls_create(self) -> None:
        with mock.patch("scripts.ops_backup_schedule.backup_restore_main", return_value=0) as cli:
            code = schedule_main(["run", "--skip-verify"])

        self.assertEqual(code, 0)
        cli.assert_called_once_with(["backup", "create"])

    def test_run_returns_create_failure(self) -> None:
        with mock.patch("scripts.ops_backup_schedule.backup_restore_main", return_value=2):
            code = schedule_main(["run"])

        self.assertEqual(code, 2)

    def test_run_missing_backup_dir_env(self) -> None:
        with (
            mock.patch.dict("os.environ", {}, clear=True),
            mock.patch("scripts.ops_backup_schedule.backup_restore_main", return_value=0),
            mock.patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            code = schedule_main(["run"])

        self.assertEqual(code, 2)
        self.assertIn("FACTORY_BACKUP_DIR is required", stdout.getvalue())

    def test_module_invocation_help(self) -> None:
        proc = subprocess.run(
            [sys.executable, "-m", "scripts.ops_backup_schedule", "--help"],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("Scheduled backup wrapper for systemd timers", proc.stdout)
        self.assertIn("run", proc.stdout)



if __name__ == "__main__":
    unittest.main()
