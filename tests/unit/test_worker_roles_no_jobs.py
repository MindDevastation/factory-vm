from __future__ import annotations

import os
import tempfile
import unittest

from services.common.env import Env
from services.common import db as dbm
from services.workers.orchestrator import orchestrator_cycle
from services.workers.uploader import uploader_cycle


class TestWorkerRolesNoJobs(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        os.environ["FACTORY_DB_PATH"] = os.path.join(self.td.name, "db.sqlite3")
        os.environ["FACTORY_STORAGE_ROOT"] = os.path.join(self.td.name, "storage")
        os.environ["FACTORY_BASIC_AUTH_PASS"] = "x"
        os.environ["TG_ADMIN_CHAT_ID"] = "0"
        os.environ["TELEGRAM_ENABLED"] = "0"
        os.environ["ORIGIN_BACKEND"] = "local"
        os.environ["UPLOAD_BACKEND"] = "mock"
        self.env = Env.load()

        conn = dbm.connect(self.env)
        try:
            dbm.migrate(conn)
        finally:
            conn.close()

    def tearDown(self) -> None:
        self.td.cleanup()

    def test_orchestrator_cycle_empty_queue(self) -> None:
        # should not crash on empty DB
        orchestrator_cycle(env=self.env, worker_id="t-orch")

    def test_uploader_cycle_empty_queue(self) -> None:
        uploader_cycle(env=self.env, worker_id="t-upl")
