from __future__ import annotations

import logging
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from services.common.disk_thresholds import DiskPressureLevel
from services.common.env import Env
from services.ops_retention.config import RetentionWindows
from services.ops_retention.runner import execute_retention


class TestOpsRetentionP0S4(unittest.TestCase):
    def test_urgent_mode_only_activates_for_critical_disk_pressure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previews = root / "previews"
            previews.mkdir(parents=True)
            preview = previews / "recent_preview.mp4"
            preview.write_text("x", encoding="utf-8")
            old_ts = time.time() - (2 * 3600)
            os.utime(preview, (old_ts, old_ts))

            events: list[dict[str, object]] = []
            with patch.dict("os.environ", {"FACTORY_STORAGE_ROOT": str(root)}, clear=False):
                env = Env.load()
                outcome_warn = execute_retention(
                    env=env,
                    windows=RetentionWindows(preview_hours=24),
                    execution_mode="run",
                    logger=logging.getLogger("test.retention.urgent.warn"),
                    event_sink=events.append,
                    disk_pressure=DiskPressureLevel.WARNING,
                    urgent_requested=True,
                )

            self.assertEqual(outcome_warn.deleted, 0)
            self.assertTrue(preview.exists())
            urgent_start = [evt for evt in events if evt.get("event_name") == "retention.urgent.start"]
            self.assertEqual(urgent_start[-1]["result"], "ignored")

            events = []
            with patch.dict("os.environ", {"FACTORY_STORAGE_ROOT": str(root)}, clear=False):
                env = Env.load()
                outcome_critical = execute_retention(
                    env=env,
                    windows=RetentionWindows(preview_hours=24),
                    execution_mode="run",
                    logger=logging.getLogger("test.retention.urgent.critical"),
                    event_sink=events.append,
                    disk_pressure=DiskPressureLevel.CRITICAL,
                    urgent_requested=True,
                )

            self.assertEqual(outcome_critical.deleted, 1)
            self.assertFalse(preview.exists())
            urgent_complete = [evt for evt in events if evt.get("event_name") == "retention.urgent.complete"]
            self.assertIn("urgent_mode=true", str(urgent_complete[-1]["result"]))

    def test_urgent_mode_keeps_protected_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previews = root / "previews"
            previews.mkdir(parents=True)
            protected_dir = previews / "backups"
            protected_dir.mkdir(parents=True)
            marker = protected_dir / "keep.txt"
            marker.write_text("keep", encoding="utf-8")
            old_ts = time.time() - (30 * 3600)
            os.utime(protected_dir, (old_ts, old_ts))

            with patch.dict("os.environ", {"FACTORY_STORAGE_ROOT": str(root)}, clear=False):
                env = Env.load()
                outcome = execute_retention(
                    env=env,
                    windows=RetentionWindows(preview_hours=24),
                    execution_mode="run",
                    logger=logging.getLogger("test.retention.urgent.protected"),
                    disk_pressure=DiskPressureLevel.CRITICAL,
                    urgent_requested=True,
                )

            self.assertEqual(outcome.deleted, 0)
            self.assertTrue(protected_dir.exists())
            self.assertTrue(marker.exists())


if __name__ == "__main__":
    unittest.main()
