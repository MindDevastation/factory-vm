from __future__ import annotations

import importlib
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir
from services.workers.orchestrator import orchestrator_cycle
from services.workers.qa import qa_cycle
from services.workers.uploader import uploader_cycle
from services.workers.cleanup import cleanup_cycle

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job, add_local_inputs_for_job, basic_auth_header


class _FakeProc:
    def __init__(self, *, release_dir: Path, exit_code: int = 0):
        self._release_dir = release_dir
        self._exit_code = exit_code

        class _Stdout:
            def __init__(self, outer: _FakeProc):
                self._o = outer

            def __iter__(self):
                yield "0.0 %"
                yield "25.0 %"
                # create mp4
                self._o._release_dir.mkdir(parents=True, exist_ok=True)
                (self._o._release_dir / "out.mp4").write_bytes(b"mp4")
                yield "100.0 %"

        self.stdout = _Stdout(self)

    def terminate(self):
        pass

    def wait(self):
        return self._exit_code


class TestPipelineE2EMocked(unittest.TestCase):
    def test_pipeline_reaches_wait_approval_then_approve_publish_cleanup(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["ORIGIN_BACKEND"] = "local"
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="READY_FOR_RENDER", stage="FETCH")
            add_local_inputs_for_job(env, job_id, tracks=1)

            def _fake_preview(*, src_mp4: Path, dst_mp4: Path, seconds: int, width: int, height: int, fps: int, v_bitrate: str, a_bitrate: str):
                dst_mp4.parent.mkdir(parents=True, exist_ok=True)
                dst_mp4.write_bytes(b"preview")

            probe = {
                "streams": [
                    {"codec_type": "video", "codec_name": "h264", "avg_frame_rate": "24/1", "width": 1920, "height": 1080, "duration": "30.0"},
                    {"codec_type": "audio", "codec_name": "aac", "sample_rate": "48000", "channels": 2, "duration": "30.0"},
                ]
            }

            release_dir = Path(env.storage_root) / "workspace" / f"job_{job_id}" / "YouTubeRoot" / "Darkwood Reverie" / "Release"

            with patch("services.workers.orchestrator.subprocess.Popen", lambda *a, **k: _FakeProc(release_dir=release_dir)), patch(
                "services.workers.orchestrator.make_preview_60s", _fake_preview
            ):
                orchestrator_cycle(env=env, worker_id="t-orch")

            with patch("services.workers.qa.ffprobe_json", lambda p: probe), patch("services.workers.qa.volumedetect", lambda p, seconds: (-30.0, -2.0, None)):
                qa_cycle(env=env, worker_id="t-qa")

            uploader_cycle(env=env, worker_id="t-upl")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                yt = conn.execute("SELECT * FROM youtube_uploads WHERE job_id=?", (job_id,)).fetchone()
            finally:
                conn.close()

            assert job is not None
            self.assertEqual(job["state"], "WAIT_APPROVAL")
            self.assertIsNotNone(yt)

            # API approve + publish
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            ra = client.post(f"/v1/jobs/{job_id}/approve", json={"comment": "ok"}, headers=h)
            self.assertEqual(ra.status_code, 200)

            rp = client.post(f"/v1/jobs/{job_id}/mark_published", json={}, headers=h)
            self.assertEqual(rp.status_code, 200)

            # Create mp4/preview to be cleaned
            mp4 = outbox_dir(env, job_id) / "render.mp4"
            self.assertTrue(mp4.exists())

            # Force delete_mp4_at in past
            conn2 = dbm.connect(env)
            try:
                dbm.update_job_state(conn2, job_id, state="PUBLISHED", stage="APPROVAL", delete_mp4_at=dbm.now_ts() - 1)
            finally:
                conn2.close()

            cleanup_cycle(env=env, worker_id="t-clean")

            conn3 = dbm.connect(env)
            try:
                job3 = dbm.get_job(conn3, job_id)
            finally:
                conn3.close()

            assert job3 is not None
            self.assertEqual(job3["state"], "CLEANED")


if __name__ == "__main__":
    unittest.main()
