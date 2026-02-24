from __future__ import annotations

import logging
import unittest
from pathlib import Path

from services.common.logging_setup import setup_logging, get_logger, _CONFIGURED_FOR

from tests._helpers import temp_env


class TestSetupLogging(unittest.TestCase):
    def test_setup_logging_creates_service_log_file(self) -> None:
        with temp_env() as (_, env):
            # reset configured services for isolation
            _CONFIGURED_FOR.clear()

            # reset root handlers to reduce cross-test interference
            root = logging.getLogger()
            for h in list(root.handlers):
                try:
                    h.close()
                except Exception:
                    pass
                root.removeHandler(h)

            setup_logging(env, service="svc_test")
            log = get_logger("svc_test")
            log.info("hello")

            p = Path(env.storage_root) / "logs" / "svc_test.log"
            self.assertTrue(p.exists())
            txt = p.read_text(encoding="utf-8", errors="ignore")
            self.assertIn("hello", txt)

            # Ensure file handlers are closed to avoid ResourceWarning in test output.
            logging.shutdown()


if __name__ == "__main__":
    unittest.main()
