from __future__ import annotations

import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir, preview_path
from services.workers.orchestrator import orchestrator_cycle

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job, add_local_inputs_for_job


class _FakeProc:
    def __init__(self, *, release_dir: Path, cancel_flag: Path | None = None, exit_code: int = 0):
        self._release_dir = release_dir
        self._exit_code = exit_code
        self._terminated = False
        self._cancel_flag = cancel_flag

        class _Stdout:
            def __init__(self, outer: _FakeProc):
                self._o = outer

            def __iter__(self):
                # If we want to cancel, create marker BEFORE first output line.
                # Orchestrator checks cancellation at least once per second, and we
                # want the marker to be visible on the first read.
                if self._o._cancel_flag is not None:
                    self._o._cancel_flag.parent.mkdir(parents=True, exist_ok=True)
                    self._o._cancel_flag.write_text("cancel", encoding="utf-8")
                    yield "0.0 %"
                    return

                yield "0.0 %"

                # simulate progress
                yield "10.0 %"
                # create an mp4 as if renderer produced it
                self._o._release_dir.mkdir(parents=True, exist_ok=True)
                (self._o._release_dir / "out.mp4").write_bytes(b"mp4")
                yield "99.5 %"

        self.stdout = _Stdout(self)

    def terminate(self):
        self._terminated = True

    def wait(self):
        return self._exit_code


class TestOrchestratorMockRender(unittest.TestCase):
    def test_orchestrator_happy_path_to_qa_running(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["ORIGIN_BACKEND"] = "local"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="READY_FOR_RENDER", stage="FETCH")
            add_local_inputs_for_job(env, job_id, tracks=1)

            # Fake preview: just create a file
            def _fake_preview(*, src_mp4: Path, dst_mp4: Path, seconds: int, width: int, height: int, fps: int, v_bitrate: str, a_bitrate: str):
                dst_mp4.parent.mkdir(parents=True, exist_ok=True)
                dst_mp4.write_bytes(b"preview")

            # release dir is determined by channel display name
            release_dir = Path(env.storage_root) / "workspace" / f"job_{job_id}" / "YouTubeRoot" / "Darkwood Reverie" / "Release"

            with patch("services.workers.orchestrator.subprocess.Popen", lambda *a, **k: _FakeProc(release_dir=release_dir)), patch(
                "services.workers.orchestrator.make_preview_60s", _fake_preview
            ):
                orchestrator_cycle(env=env, worker_id="t-orch")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                assert job is not None
                self.assertEqual(job["state"], "QA_RUNNING")
                self.assertEqual(job["stage"], "QA")
                # outputs should exist
                out = outbox_dir(env, job_id) / "render.mp4"
                self.assertTrue(out.exists())
                self.assertTrue(preview_path(env, job_id).exists())
            finally:
                conn.close()

    def test_orchestrator_cancel_via_marker(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["ORIGIN_BACKEND"] = "local"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="READY_FOR_RENDER", stage="FETCH")
            add_local_inputs_for_job(env, job_id, tracks=1)

            # release dir
            release_dir = Path(env.storage_root) / "workspace" / f"job_{job_id}" / "YouTubeRoot" / "Darkwood Reverie" / "Release"
            cancel_flag = Path(env.storage_root) / "workspace" / f"job_{job_id}" / "YouTubeRoot" / ".cancel"

            with patch("services.workers.orchestrator.subprocess.Popen", lambda *a, **k: _FakeProc(release_dir=release_dir, cancel_flag=cancel_flag)):
                orchestrator_cycle(env=env, worker_id="t-orch")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                assert job is not None
                self.assertEqual(job["state"], "CANCELLED")
                self.assertEqual(job["stage"], "CANCELLED")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
