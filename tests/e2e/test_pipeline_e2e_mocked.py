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


    def _seed_publish_job(self, env, *, publish_state: str, reason_code: str | None = None, scheduled_at: float | None = None) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch
            ts = dbm.now_ts()
            cur = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                (int(ch["id"]), f"e2e-{publish_state}", "d", "[]", ts),
            )
            job_id = dbm.insert_job_with_lineage_defaults(
                conn,
                release_id=int(cur.lastrowid),
                job_type="UI",
                state="UPLOADED",
                stage="PUBLISH",
                priority=1,
                attempt=0,
                created_at=ts,
                updated_at=ts,
            )
            conn.execute(
                "UPDATE jobs SET publish_state=?, publish_reason_code=?, publish_scheduled_at=?, publish_last_transition_at=?, publish_drift_detected_at=? WHERE id=?",
                (publish_state, reason_code, scheduled_at, ts, ts if publish_state == "publish_state_drift_detected" else None, job_id),
            )
            conn.commit()
            return job_id
        finally:
            conn.close()

    def test_epic3_publish_paths_manual_auto_retry_drift(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            client = TestClient(importlib.reload(mod).app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            # baseline approval + manual completion
            j_manual = self._seed_publish_job(env, publish_state="manual_handoff_pending")
            ack = client.post(f"/v1/publish/jobs/{j_manual}/acknowledge", headers=h, json={"confirm": True, "reason": "own", "request_id": "e2e-ack"})
            self.assertEqual(ack.status_code, 200)
            done = client.post(
                f"/v1/publish/jobs/{j_manual}/mark-completed",
                headers=h,
                json={"confirm": True, "reason": "done", "request_id": "e2e-done", "actual_published_at": "2026-03-29T00:00:00Z", "video_id": "yt-1"},
            )
            self.assertEqual(done.status_code, 200)

            # automatic scheduled publish path (reschedule -> waiting_for_schedule)
            j_sched = self._seed_publish_job(env, publish_state="ready_to_publish")
            rs = client.post(
                f"/v1/publish/jobs/{j_sched}/reschedule",
                headers=h,
                json={"confirm": True, "reason": "delay", "request_id": "e2e-rs", "scheduled_at": "2026-12-01T00:00:00Z"},
            )
            self.assertEqual(rs.status_code, 200)
            self.assertEqual(rs.json()["result"]["publish_state_after"], "waiting_for_schedule")

            # retries exhausted -> manual handoff
            j_retry = self._seed_publish_job(env, publish_state="manual_handoff_pending", reason_code="retries_exhausted")
            detail_retry = client.get(f"/v1/publish/jobs/{j_retry}", headers=h)
            self.assertEqual(detail_retry.status_code, 200)
            self.assertEqual(detail_retry.json()["publish_state"], "manual_handoff_pending")

            # external manual publish -> drift detected -> operator completion path
            j_drift = self._seed_publish_job(env, publish_state="publish_state_drift_detected", reason_code="external_manual_publish_detected")
            to_manual = client.post(
                f"/v1/publish/jobs/{j_drift}/move-to-manual",
                headers=h,
                json={"confirm": True, "reason": "operator handling", "request_id": "e2e-mtm"},
            )
            self.assertEqual(to_manual.status_code, 200)
            ack2 = client.post(f"/v1/publish/jobs/{j_drift}/acknowledge", headers=h, json={"confirm": True, "reason": "ack", "request_id": "e2e-ack2"})
            self.assertEqual(ack2.status_code, 200)
            done2 = client.post(
                f"/v1/publish/jobs/{j_drift}/mark-completed",
                headers=h,
                json={"confirm": True, "reason": "completed", "request_id": "e2e-done2", "actual_published_at": "2026-03-29T00:00:00Z", "url": "https://youtube.test/v"},
            )
            self.assertEqual(done2.status_code, 200)


if __name__ == "__main__":
    unittest.main()
