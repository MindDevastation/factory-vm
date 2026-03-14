from __future__ import annotations

import logging
import os
import unittest
from pathlib import Path

from services.common.logging_setup import (
    _CONFIGURED_FOR,
    get_logger,
    resolve_log_class,
    resolve_log_file_policy,
    setup_logging,
)
from services.ops_retention.log_policy import LogClass

from tests._helpers import temp_env


class TestSetupLogging(unittest.TestCase):
    def _reset_root_logging(self) -> None:
        _CONFIGURED_FOR.clear()
        root = logging.getLogger()
        for h in list(root.handlers):
            try:
                h.close()
            except Exception:
                pass
            root.removeHandler(h)

    def test_setup_logging_creates_service_log_file(self) -> None:
        with temp_env() as (_, env):
            # reset configured services for isolation
            self._reset_root_logging()
            self.addCleanup(self._reset_root_logging)

            setup_logging(env, service="factory_api")
            log = get_logger("factory_api")
            log.info("hello")

            p = Path(env.storage_root) / "logs" / "app.log"
            self.assertTrue(p.exists())
            txt = p.read_text(encoding="utf-8", errors="ignore")
            self.assertIn("hello", txt)


    def test_setup_logging_respects_factory_log_dir_override(self) -> None:
        with temp_env() as (td, env):
            self._reset_root_logging()
            self.addCleanup(self._reset_root_logging)

            override_dir = Path(td.name) / "custom-logs"
            os.environ["FACTORY_LOG_DIR"] = str(override_dir)

            setup_logging(env, service="worker-uploader")
            log = get_logger("worker-uploader")
            log.info("override-log")

            p = override_dir / "pipeline.log"
            self.assertTrue(p.exists())
            txt = p.read_text(encoding="utf-8", errors="ignore")
            self.assertIn("override-log", txt)


    def test_resolve_log_policy_mapping(self) -> None:
        self.assertEqual(resolve_log_class("factory_api"), LogClass.APPLICATION)
        self.assertEqual(resolve_log_class("worker-qa"), LogClass.WORKER_RUNTIME)
        self.assertEqual(resolve_log_class("worker-uploader"), LogClass.UPLOADER_RENDER)
        self.assertEqual(resolve_log_class("worker-cleanup"), LogClass.RECOVERY_AUDIT)
        self.assertEqual(resolve_log_class("bot"), LogClass.BOT)
        self.assertEqual(resolve_log_class("ops-health-smoke"), LogClass.SMOKE_OPS)

        self.assertEqual(resolve_log_file_policy("factory_api"), (Path("app.log"), 20 * 1024 * 1024, 10))
        self.assertEqual(resolve_log_file_policy("worker-uploader"), (Path("pipeline.log"), 25 * 1024 * 1024, 8))

    def test_rotation_bounds_file_count(self) -> None:
        with temp_env() as (_, env):
            self._reset_root_logging()
            self.addCleanup(self._reset_root_logging)

            setup_logging(env, service="ops-smoke")
            logger = get_logger("ops-smoke")

            for _ in range(200):
                logger.info("X" * 60_000)

            log_dir = Path(env.storage_root) / "logs"
            ops_logs = sorted(log_dir.glob("ops.log*"))
            self.assertLessEqual(len(ops_logs), 13)
            self.assertTrue((log_dir / "ops.log").exists())
            self.assertGreater((log_dir / "ops.log").stat().st_size, 0)



if __name__ == "__main__":
    unittest.main()
