from __future__ import annotations

import os
import socket
import tempfile
import unittest

from services.common.env import Env
from services.common import db as dbm


class TestWorkerHeartbeat(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        os.environ["FACTORY_DB_PATH"] = os.path.join(self.td.name, "db.sqlite3")
        os.environ["FACTORY_STORAGE_ROOT"] = os.path.join(self.td.name, "storage")
        os.environ["FACTORY_BASIC_AUTH_PASS"] = "x"
        self.env = Env.load()

        conn = dbm.connect(self.env)
        try:
            dbm.migrate(conn)
        finally:
            conn.close()

    def tearDown(self) -> None:
        self.td.cleanup()

    def test_touch_and_list_workers(self) -> None:
        conn = dbm.connect(self.env)
        try:
            dbm.touch_worker(
                conn,
                worker_id="qa:123",
                role="qa",
                pid=123,
                hostname=socket.gethostname(),
                details={"x": 1},
            )
            rows = dbm.list_workers(conn)
        finally:
            conn.close()

        self.assertTrue(rows)
        self.assertEqual(rows[0]["worker_id"], "qa:123")
