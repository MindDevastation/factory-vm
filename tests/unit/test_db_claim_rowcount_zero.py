from __future__ import annotations

import unittest
from typing import Any, Dict, Optional

from services.common import db as dbm


class _FakeCursor:
    def __init__(self, *, row: Optional[Dict[str, Any]] = None, rowcount: int = 1):
        self._row = row
        self.rowcount = rowcount

    def fetchone(self):
        return self._row


class _FakeConn:
    """Stub connection to cover claim_job() rowcount != 1 branch."""

    def __init__(self):
        self._select_done = False

    def execute(self, sql: str, params: tuple = ()):  # type: ignore[override]
        s = sql.strip()
        if s.startswith("BEGIN") or s.startswith("COMMIT"):
            return _FakeCursor()
        if "SELECT" in s and "FROM jobs" in s and not self._select_done:
            self._select_done = True
            return _FakeCursor(row={"id": 123})
        if s.startswith("UPDATE jobs"):
            return _FakeCursor(rowcount=0)
        return _FakeCursor()


class TestDbClaimRowcountZero(unittest.TestCase):
    def test_claim_job_returns_none_when_update_rowcount_zero(self) -> None:
        conn = _FakeConn()
        job_id = dbm.claim_job(
            conn,  # type: ignore[arg-type]
            want_state="READY_FOR_RENDER",
            worker_id="w",
            lock_ttl_sec=10,
        )
        self.assertIsNone(job_id)
