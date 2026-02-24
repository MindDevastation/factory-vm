from __future__ import annotations

import os
import tempfile
import unittest

from services.common.env import Env
from services.common import db as dbm


class TestDbClaimJob(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        os.environ["FACTORY_DB_PATH"] = os.path.join(self.td.name, "db.sqlite3")
        os.environ["FACTORY_STORAGE_ROOT"] = os.path.join(self.td.name, "storage")
        os.environ["FACTORY_BASIC_AUTH_PASS"] = "x"
        self.env = Env.load()

        conn = dbm.connect(self.env)
        try:
            dbm.migrate(conn)
            ts = dbm.now_ts()
            conn.execute(
                "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                ("ch", "Ch", "X", 1.0, "rp", 0),
            )
            conn.execute(
                "INSERT INTO render_profiles(name, video_w, video_h, fps, vcodec_required, audio_sr, audio_ch, acodec_required) VALUES(?,?,?,?,?,?,?,?)",
                ("rp", 1920, 1080, 24.0, "h264", 48000, 2, "aac"),
            )
            cur = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
                (1, "t", "d", "[]", None, None, f"m_{int(ts)}", ts),
            )
            self.release_id = int(cur.lastrowid)
        finally:
            conn.close()

    def tearDown(self) -> None:
        self.td.cleanup()

    def _insert_job(self, *, state: str, locked: bool = False, locked_at: float | None = None, retry_at: float | None = None) -> int:
        conn = dbm.connect(self.env)
        try:
            ts = dbm.now_ts()
            cur = conn.execute(
                "INSERT INTO jobs(release_id, job_type, state, stage, priority, attempt, locked_by, locked_at, retry_at, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.release_id,
                    "RENDER_LONG",
                    state,
                    "FETCH",
                    1,
                    0,
                    "w" if locked else None,
                    locked_at,
                    retry_at,
                    ts,
                    ts,
                ),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def test_claim_skips_nonexpired_lock(self) -> None:
        jid = self._insert_job(state="READY_FOR_RENDER", locked=True, locked_at=dbm.now_ts())
        conn = dbm.connect(self.env)
        try:
            got = dbm.claim_job(conn, want_state="READY_FOR_RENDER", worker_id="x", lock_ttl_sec=3600)
        finally:
            conn.close()
        self.assertIsNone(got)
        self.assertIsNotNone(jid)

    def test_claim_reclaims_expired_lock(self) -> None:
        ttl = 10
        past = dbm.now_ts() - ttl - 1
        jid = self._insert_job(state="READY_FOR_RENDER", locked=True, locked_at=past)
        conn = dbm.connect(self.env)
        try:
            got = dbm.claim_job(conn, want_state="READY_FOR_RENDER", worker_id="x", lock_ttl_sec=ttl)
        finally:
            conn.close()
        self.assertEqual(got, jid)

    def test_claim_respects_retry_at(self) -> None:
        future = dbm.now_ts() + 3600
        self._insert_job(state="READY_FOR_RENDER", retry_at=future)
        ok_jid = self._insert_job(state="READY_FOR_RENDER")

        conn = dbm.connect(self.env)
        try:
            got = dbm.claim_job(conn, want_state="READY_FOR_RENDER", worker_id="x", lock_ttl_sec=3600)
        finally:
            conn.close()
        self.assertEqual(got, ok_jid)
