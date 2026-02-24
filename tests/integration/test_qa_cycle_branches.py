from __future__ import annotations

import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from services.common import db as dbm
from services.common.config import PoliciesCfg
from services.common.env import Env
from services.common.paths import outbox_dir
from services.workers import qa as qa_worker

from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestQaCycleBranches(unittest.TestCase):
    def _write_mp4(self, env: Env, job_id: int) -> Path:
        p = outbox_dir(env, job_id) / "render.mp4"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"0")
        return p

    def test_missing_mp4_sets_qa_failed(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="QA_RUNNING", stage="QA")

            qa_worker.qa_cycle(env=env, worker_id="wqa")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "QA_FAILED")
                self.assertEqual(str(job.get("error_reason")), "missing mp4")
            finally:
                conn.close()

    def test_warnings_block_pipeline_when_configured(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="QA_RUNNING", stage="QA")
            self._write_mp4(env, job_id)

            policies = PoliciesCfg(
                raw={
                    "qa_policy": {
                        "warning_blocks_pipeline": True,
                        "duration_diff_hard_fail_sec": 2.0,
                        "video": {"fps_target": 24, "fps_tolerance": 0.5},
                        "loudness": {"warn_if_max_volume_gte_db": -0.1, "warn_if_mean_volume_gt_db": -10.0, "warn_if_mean_volume_lt_db": -55.0},
                    }
                }
            )

            probe = {
                "streams": [
                    {"codec_type": "video", "codec_name": "h264", "width": 999, "height": 888, "avg_frame_rate": "10/1", "duration": "10"},
                    {"codec_type": "audio", "codec_name": "aac", "sample_rate": "44100", "channels": 1, "duration": "5"},
                ]
            }

            with mock.patch.object(qa_worker, "load_policies", return_value=policies), \
                mock.patch.object(qa_worker, "ffprobe_json", return_value=probe), \
                mock.patch.object(qa_worker, "volumedetect", return_value=(-20.0, -0.05, None)):
                qa_worker.qa_cycle(env=env, worker_id="wqa")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "QA_FAILED")
                self.assertIn("QA blocked", str(job.get("error_reason")))
            finally:
                conn.close()

    def test_warnings_do_not_block_when_configured(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="QA_RUNNING", stage="QA")
            self._write_mp4(env, job_id)

            # Fetch expected params from DB so the test is robust to config changes.
            conn = dbm.connect(env)
            try:
                expected = conn.execute(
                    """
                    SELECT rp.video_w, rp.video_h, rp.fps, rp.vcodec_required,
                           rp.audio_sr, rp.audio_ch, rp.acodec_required
                    FROM jobs j
                    JOIN releases r ON r.id = j.release_id
                    JOIN channels c ON c.id = r.channel_id
                    JOIN render_profiles rp ON rp.name = c.render_profile
                    WHERE j.id = ?
                    """,
                    (job_id,),
                ).fetchone()
            finally:
                conn.close()

            policies = PoliciesCfg(
                raw={
                    "qa_policy": {
                        "warning_blocks_pipeline": False,
                        "video": {"fps_target": 24, "fps_tolerance": 0.5},
                        "loudness": {"warn_if_max_volume_gte_db": -0.1, "warn_if_mean_volume_gt_db": -10.0, "warn_if_mean_volume_lt_db": -55.0},
                    }
                }
            )

            probe = {
                "streams": [
                    # match expected codec/resolution but force fps warning
                    {
                        "codec_type": "video",
                        "codec_name": str(expected["vcodec_required"]),
                        "width": int(expected["video_w"]),
                        "height": int(expected["video_h"]),
                        "avg_frame_rate": "10/1",
                        "duration": "10",
                    },
                    {
                        "codec_type": "audio",
                        "codec_name": str(expected["acodec_required"]),
                        "sample_rate": str(expected["audio_sr"]),
                        "channels": int(expected["audio_ch"]),
                        "duration": "10",
                    },
                ]
            }

            with mock.patch.object(qa_worker, "load_policies", return_value=policies), \
                mock.patch.object(qa_worker, "ffprobe_json", return_value=probe), \
                mock.patch.object(qa_worker, "volumedetect", return_value=(-20.0, -0.05, None)):
                qa_worker.qa_cycle(env=env, worker_id="wqa")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "UPLOADING")
                self.assertEqual(str(job.get("stage")), "UPLOAD")
            finally:
                conn.close()

    def test_cancel_flag_exception_releases_lock(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="QA_RUNNING", stage="QA")
            self._write_mp4(env, job_id)

            with mock.patch.object(qa_worker, "cancel_flag_path", side_effect=RuntimeError("boom")):
                qa_worker.qa_cycle(env=env, worker_id="wqa")

            conn = dbm.connect(env)
            try:
                row = conn.execute("SELECT locked_by FROM jobs WHERE id = ?", (job_id,)).fetchone()
                self.assertIsNone(row["locked_by"])
            finally:
                conn.close()
