from __future__ import annotations

import unittest
from unittest.mock import patch

from services.common.disk_thresholds import DiskPressureLevel, DiskThresholds, evaluate_disk_pressure
from services.common.env import Env
from services.ops_retention.artifact_policy import (
    ArtifactCategory,
    ArtifactDisposition,
    artifact_disposition_map,
)
from services.ops_retention.config import load_ops_retention_config, load_retention_windows
from services.ops_retention.log_policy import CANONICAL_LOG_POLICIES, LogClass, LogStorageTier


class TestOpsRetentionPoliciesP0S1(unittest.TestCase):
    def test_log_policy_inventory_and_defaults(self) -> None:
        self.assertEqual(set(CANONICAL_LOG_POLICIES.keys()), set(LogClass))

        self.assertEqual(CANONICAL_LOG_POLICIES[LogClass.APPLICATION].rotate_mib, 20)
        self.assertEqual(CANONICAL_LOG_POLICIES[LogClass.APPLICATION].keep_files, 10)
        self.assertEqual(CANONICAL_LOG_POLICIES[LogClass.SMOKE_OPS].rotate_mib, 5)
        self.assertEqual(CANONICAL_LOG_POLICIES[LogClass.SMOKE_OPS].keep_files, 12)

        for policy in CANONICAL_LOG_POLICIES.values():
            self.assertEqual(policy.storage_tier, LogStorageTier.PROJECT_FILE)

    def test_artifact_disposition_inventory(self) -> None:
        disposition = artifact_disposition_map()

        self.assertEqual(disposition[ArtifactCategory.TEMP_PREVIEWS], ArtifactDisposition.DISPOSABLE)
        self.assertEqual(disposition[ArtifactCategory.TERMINAL_WORKSPACES], ArtifactDisposition.DISPOSABLE)
        self.assertEqual(disposition[ArtifactCategory.CURRENT_SQLITE_DB], ArtifactDisposition.PROTECTED)
        self.assertEqual(disposition[ArtifactCategory.OUTSIDE_ALLOWLIST_SCOPE], ArtifactDisposition.PROTECTED)

    def test_retention_windows_and_log_dir_contract(self) -> None:
        env_vars = {
            "FACTORY_STORAGE_ROOT": "storage-default",
            "FACTORY_LOG_DIR": "/tmp/factory-logs",
            "FACTORY_RETENTION_PREVIEW_HOURS": "36",
            "FACTORY_RETENTION_EXPORT_DAYS": "21",
            "FACTORY_RETENTION_TRANSIENT_REPORT_DAYS": "8",
            "FACTORY_RETENTION_TERMINAL_WORKSPACE_HOURS": "20",
            "FACTORY_RETENTION_FAILED_WORKSPACE_DAYS": "5",
            "FACTORY_SMOKE_DISK_WARN_PERCENT": "17",
            "FACTORY_SMOKE_DISK_WARN_GIB": "24",
            "FACTORY_SMOKE_DISK_FAIL_PERCENT": "9",
            "FACTORY_SMOKE_DISK_FAIL_GIB": "11",
        }
        with patch.dict("os.environ", env_vars, clear=True):
            env = Env.load()
            cfg = load_ops_retention_config(env)

        self.assertEqual(str(cfg.log_dir), "/tmp/factory-logs")
        self.assertEqual(cfg.windows.preview_hours, 36)
        self.assertEqual(cfg.windows.export_days, 21)
        self.assertEqual(cfg.windows.transient_report_days, 8)
        self.assertEqual(cfg.windows.terminal_workspace_hours, 20)
        self.assertEqual(cfg.windows.failed_workspace_days, 5)
        self.assertEqual(cfg.disk_thresholds, DiskThresholds(warn_percent=17.0, warn_gib=24.0, fail_percent=9.0, fail_gib=11.0))

    def test_retention_windows_defaults(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            windows = load_retention_windows()

        self.assertEqual(windows.preview_hours, 24)
        self.assertEqual(windows.export_days, 14)
        self.assertEqual(windows.transient_report_days, 7)
        self.assertEqual(windows.terminal_workspace_hours, 12)
        self.assertEqual(windows.failed_workspace_days, 3)

    def test_disk_pressure_model(self) -> None:
        thresholds = DiskThresholds(warn_percent=15.0, warn_gib=20.0, fail_percent=8.0, fail_gib=10.0)

        self.assertEqual(
            evaluate_disk_pressure(free_percent=30.0, free_gib=25.0, thresholds=thresholds),
            DiskPressureLevel.OK,
        )
        self.assertEqual(
            evaluate_disk_pressure(free_percent=12.0, free_gib=25.0, thresholds=thresholds),
            DiskPressureLevel.WARNING,
        )
        self.assertEqual(
            evaluate_disk_pressure(free_percent=30.0, free_gib=9.0, thresholds=thresholds),
            DiskPressureLevel.CRITICAL,
        )


if __name__ == "__main__":
    unittest.main()
