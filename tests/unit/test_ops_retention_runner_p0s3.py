from __future__ import annotations

import logging
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from services.common import db as dbm
from services.common.env import Env
from services.ops_retention.config import RetentionWindows
from services.ops_retention.runner import execute_retention


class TestOpsRetentionRunnerP0S3(unittest.TestCase):
    def _seed_job_state(self, env: Env, *, job_id: int, state: str) -> None:
        with dbm.connect(env) as conn:
            dbm.migrate(conn)
            channel = dbm.create_channel(conn, slug=f"ch_{job_id}", display_name=f"Channel {job_id}")
            ts = dbm.now_ts()
            release_cur = conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES(?, ?, ?, '[]', NULL, NULL, NULL, ?)
                """,
                (int(channel["id"]), f"r-{job_id}", "", ts),
            )
            release_id = int(release_cur.lastrowid)
            conn.execute(
                """
                INSERT INTO jobs(
                    id, release_id, job_type, state, stage, priority, attempt,
                    retry_of_job_id, root_job_id, attempt_no, force_refetch_inputs,
                    created_at, updated_at
                ) VALUES(?, ?, 'UI', ?, ?, 0, 0, NULL, ?, 1, 0, ?, ?)
                """,
                (job_id, release_id, state, state, job_id, ts, ts),
            )

    def test_scan_is_non_destructive_and_emits_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previews = root / "previews"
            previews.mkdir(parents=True)
            expired = previews / "preview_old.mp4"
            expired.write_text("x", encoding="utf-8")
            old_ts = time.time() - (26 * 3600)
            os.utime(expired, (old_ts, old_ts))

            events: list[dict[str, object]] = []
            with patch.dict("os.environ", {"FACTORY_STORAGE_ROOT": str(root)}, clear=False):
                env = Env.load()
                outcome = execute_retention(
                    env=env,
                    windows=RetentionWindows(),
                    execution_mode="scan",
                    logger=logging.getLogger("test.retention.scan"),
                    event_sink=events.append,
                )

            self.assertTrue(expired.exists())
            self.assertEqual(outcome.deleted, 0)
            self.assertGreaterEqual(outcome.skipped, 1)
            event_names = [str(evt["event_name"]) for evt in events]
            self.assertIn("retention.scan.start", event_names)
            self.assertIn("retention.scan.complete", event_names)
            self.assertIn("retention.skip", event_names)

    def test_active_workspace_without_markers_but_runtime_referenced_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspaces = root / "workspace"
            workspaces.mkdir(parents=True)
            active_ws = workspaces / "job_101"
            active_ws.mkdir()
            old_ts = time.time() - (80 * 3600)
            os.utime(active_ws, (old_ts, old_ts))

            events: list[dict[str, object]] = []
            with patch.dict("os.environ", {"FACTORY_STORAGE_ROOT": str(root)}, clear=False):
                env = Env.load()
                self._seed_job_state(env, job_id=101, state="RENDERING")
                outcome = execute_retention(
                    env=env,
                    windows=RetentionWindows(),
                    execution_mode="run",
                    logger=logging.getLogger("test.retention.active-ref"),
                    event_sink=events.append,
                )

            self.assertTrue(active_ws.exists())
            self.assertEqual(outcome.deleted, 0)
            reason_codes = [str(evt["reason_code"]) for evt in events if evt["event_name"] == "retention.skip"]
            self.assertIn("RETENTION_SKIP_ACTIVE_WORKSPACE", reason_codes)

    def test_run_deletes_only_expired_and_skips_active_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previews = root / "previews"
            workspaces = root / "workspace"
            previews.mkdir(parents=True)
            workspaces.mkdir(parents=True)

            preview_old = previews / "old_preview.mp4"
            preview_old.write_text("old", encoding="utf-8")
            preview_recent = previews / "recent_preview.mp4"
            preview_recent.write_text("new", encoding="utf-8")

            ws_active = workspaces / "job_201"
            ws_active.mkdir()
            ws_old = workspaces / "job_202"
            ws_old.mkdir()

            now = time.time()
            os.utime(preview_old, (now - (26 * 3600), now - (26 * 3600)))
            os.utime(preview_recent, (now - 600, now - 600))
            os.utime(ws_active, (now - (80 * 3600), now - (80 * 3600)))
            os.utime(ws_old, (now - (80 * 3600), now - (80 * 3600)))

            events: list[dict[str, object]] = []
            with patch.dict("os.environ", {"FACTORY_STORAGE_ROOT": str(root)}, clear=False):
                env = Env.load()
                self._seed_job_state(env, job_id=201, state="RENDERING")
                self._seed_job_state(env, job_id=202, state="FAILED")
                outcome = execute_retention(
                    env=env,
                    windows=RetentionWindows(),
                    execution_mode="run",
                    logger=logging.getLogger("test.retention.run"),
                    event_sink=events.append,
                )

            self.assertFalse(preview_old.exists())
            self.assertTrue(preview_recent.exists())
            self.assertTrue(ws_active.exists())
            self.assertFalse(ws_old.exists())
            self.assertGreaterEqual(outcome.deleted, 2)
            reason_codes = [str(evt["reason_code"]) for evt in events if evt["event_name"] in {"retention.skip", "retention.delete.success"}]
            self.assertIn("RETENTION_SKIP_TOO_RECENT", reason_codes)
            self.assertIn("RETENTION_SKIP_ACTIVE_WORKSPACE", reason_codes)
            self.assertIn("RETENTION_DELETE_TEMP_PREVIEW_EXPIRED", reason_codes)
            self.assertIn("RETENTION_DELETE_TERMINAL_WORKSPACE_EXPIRED", reason_codes)


if __name__ == "__main__":
    unittest.main()
