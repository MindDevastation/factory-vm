from __future__ import annotations

import logging
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.common.disk_guard import emit_disk_pressure_event, evaluate_disk_pressure_for_env
from services.common.disk_thresholds import DiskPressureLevel
from services.common.env import Env


class TestDiskGuard(unittest.TestCase):
    def test_threshold_parity_with_smoke_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_vars = {
                "FACTORY_STORAGE_ROOT": str(root),
                "FACTORY_SMOKE_DISK_WARN_PERCENT": "18",
                "FACTORY_SMOKE_DISK_WARN_GIB": "25",
                "FACTORY_SMOKE_DISK_FAIL_PERCENT": "7",
                "FACTORY_SMOKE_DISK_FAIL_GIB": "9",
            }
            with patch.dict("os.environ", env_vars, clear=False):
                env = Env.load()
                usage = shutil._ntuple_diskusage(total=100 * 1024**3, used=96 * 1024**3, free=4 * 1024**3)
                with patch("services.common.disk_guard.shutil.disk_usage", return_value=usage):
                    snapshot = evaluate_disk_pressure_for_env(env=env)

        self.assertEqual(snapshot.pressure, DiskPressureLevel.CRITICAL)
        self.assertEqual(snapshot.thresholds.warn_percent, 18.0)
        self.assertEqual(snapshot.thresholds.fail_gib, 9.0)

    def test_emit_disk_pressure_event_for_warning_and_critical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.dict("os.environ", {"FACTORY_STORAGE_ROOT": str(root)}, clear=False):
                env = Env.load()
                warning_usage = shutil._ntuple_diskusage(total=100 * 1024**3, used=86 * 1024**3, free=14 * 1024**3)
                critical_usage = shutil._ntuple_diskusage(total=100 * 1024**3, used=93 * 1024**3, free=7 * 1024**3)

                logger = logging.getLogger("test.disk.guard")
                with self.assertLogs(logger, level="INFO") as warning_logs:
                    with patch("services.common.disk_guard.shutil.disk_usage", return_value=warning_usage):
                        warning_snapshot = evaluate_disk_pressure_for_env(env=env)
                    emit_disk_pressure_event(logger=logger, snapshot=warning_snapshot, stage="test_warning")

                with self.assertLogs(logger, level="INFO") as critical_logs:
                    with patch("services.common.disk_guard.shutil.disk_usage", return_value=critical_usage):
                        critical_snapshot = evaluate_disk_pressure_for_env(env=env)
                    emit_disk_pressure_event(logger=logger, snapshot=critical_snapshot, stage="test_critical")

        self.assertIn("disk.warning", "\n".join(warning_logs.output))
        self.assertIn("disk.critical", "\n".join(critical_logs.output))


if __name__ == "__main__":
    unittest.main()
