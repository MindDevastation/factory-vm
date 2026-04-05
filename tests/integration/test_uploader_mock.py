from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir
from services.publish_runtime.schedule import evaluate_publish_schedule
from services.workers.uploader import uploader_cycle

from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class TestUploaderMock(unittest.TestCase):
    def _run_upload(self, env: Env, *, job_id: int) -> dict:
        mp4 = outbox_dir(env, job_id) / "render.mp4"
        mp4.parent.mkdir(parents=True, exist_ok=True)
        mp4.write_bytes(b"mp4")
        uploader_cycle(env=env, worker_id="t-upl")
        conn = dbm.connect(env)
        try:
            job = dbm.get_job(conn, job_id)
            yt = conn.execute("SELECT * FROM youtube_uploads WHERE job_id=?", (job_id,)).fetchone()
            self.assertIsNotNone(job)
            self.assertIsNotNone(yt)
            return dict(job or {})
        finally:
            conn.close()

    def _seed_policy(self, env: Env, *, publish_mode: str = "auto", global_pause: bool = False) -> None:
        conn = dbm.connect(env)
        try:
            conn.execute(
                """
                INSERT INTO publish_policy_project_defaults(
                    singleton_key, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                ) VALUES(1,?,?,?,?,?,?,?,?)
                """,
                (publish_mode, "public", "policy_requires_manual", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "admin", "seed", "req-policy"),
            )
            conn.execute(
                """
                INSERT INTO publish_audit_status_project_defaults(
                    singleton_key, status, created_at, updated_at, updated_by, last_reason, last_request_id
                ) VALUES(1, 'approved', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-audit')
                """
            )
            conn.execute(
                """
                INSERT INTO publish_global_controls(singleton_key, auto_publish_paused, reason, updated_at, updated_by)
                VALUES(1, ?, ?, '2026-01-01T00:00:00Z', 'admin')
                """,
                (1 if global_pause else 0, "paused" if global_pause else None),
            )
        finally:
            conn.close()

    def _mark_ready_for_auto_publish(self, env: Env, *, job_id: int, visibility: str = "public") -> None:
        conn = dbm.connect(env)
        try:
            conn.execute(
                """
                UPDATE jobs
                SET state = 'WAIT_APPROVAL',
                    publish_state = 'ready_to_publish',
                    publish_delivery_mode_effective = 'automatic',
                    publish_target_visibility = ?,
                    publish_attempt_count = 0,
                    publish_retry_at = NULL
                WHERE id = ?
                """,
                (visibility, job_id),
            )
            conn.execute(
                """
                INSERT INTO youtube_uploads(job_id, video_id, url, studio_url, privacy, uploaded_at)
                VALUES(?, ?, 'https://example.test', 'https://studio.example.test', 'private', 1.0)
                ON CONFLICT(job_id) DO UPDATE SET video_id=excluded.video_id, privacy='private'
                """,
                (job_id, f"mock-{job_id}"),
            )
        finally:
            conn.close()

    def test_uploader_mock_sets_wait_approval_and_private_uploaded_init(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="auto", global_pause=False)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            job = self._run_upload(env, job_id=job_id)

            self.assertEqual(job["state"], "WAIT_APPROVAL")
            self.assertEqual(job["publish_state"], "ready_to_publish")
            self.assertEqual(job["publish_delivery_mode_effective"], "automatic")
            self.assertEqual(job["publish_resolved_scope"], "project")

    def test_policy_block_lands_in_policy_blocked(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="auto", global_pause=True)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            job = self._run_upload(env, job_id=job_id)
            self.assertEqual(job["publish_state"], "policy_blocked")
            self.assertEqual(job["publish_reason_code"], "global_pause_active")

    def test_future_schedule_lands_in_waiting_for_schedule(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="auto", global_pause=False)

            future_utc = (datetime.now(timezone.utc) + timedelta(hours=3)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            conn = dbm.connect(env)
            try:
                rel_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                conn.execute("UPDATE releases SET planned_at = ? WHERE id = ?", (future_utc, rel_id))
            finally:
                conn.close()

            job = self._run_upload(env, job_id=job_id)
            self.assertEqual(job["publish_state"], "waiting_for_schedule")
            expected = evaluate_publish_schedule(planned_at=future_utc)
            self.assertAlmostEqual(float(job["publish_scheduled_at"]), float(expected.publish_scheduled_at_ts or 0), delta=1.0)

    def test_no_schedule_auto_lands_in_ready_to_publish(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="auto", global_pause=False)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            job = self._run_upload(env, job_id=job_id)
            self.assertEqual(job["publish_state"], "ready_to_publish")
            self.assertIsNone(job["publish_scheduled_at"])

    def test_no_schedule_manual_only_lands_in_manual_handoff_pending(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="manual_only", global_pause=False)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            job = self._run_upload(env, job_id=job_id)
            self.assertEqual(job["publish_state"], "manual_handoff_pending")
            self.assertEqual(job["publish_delivery_mode_effective"], "manual")

    def test_auto_publish_executor_success_public(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="auto", global_pause=False)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            self._mark_ready_for_auto_publish(env, job_id=job_id, visibility="public")

            uploader_cycle(env=env, worker_id="t-upl")

            conn = dbm.connect(env)
            try:
                job = dict(dbm.get_job(conn, job_id) or {})
                yt = conn.execute("SELECT privacy FROM youtube_uploads WHERE job_id = ?", (job_id,)).fetchone()
                self.assertEqual(job["publish_state"], "published_public")
                self.assertEqual(job["publish_attempt_count"], 1)
                self.assertEqual(yt["privacy"], "public")
            finally:
                conn.close()

    def test_auto_publish_executor_success_unlisted(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "mock"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="auto", global_pause=False)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            self._mark_ready_for_auto_publish(env, job_id=job_id, visibility="unlisted")

            uploader_cycle(env=env, worker_id="t-upl")

            conn = dbm.connect(env)
            try:
                job = dict(dbm.get_job(conn, job_id) or {})
                yt = conn.execute("SELECT privacy FROM youtube_uploads WHERE job_id = ?", (job_id,)).fetchone()
                self.assertEqual(job["publish_state"], "published_unlisted")
                self.assertEqual(job["publish_attempt_count"], 1)
                self.assertEqual(yt["privacy"], "unlisted")
            finally:
                conn.close()

    def test_auto_publish_retriable_failure_lands_retry_pending(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/tmp/client.json"
            os.environ["YT_TOKENS_DIR"] = "/tmp/yt-tokens"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="auto", global_pause=False)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            self._mark_ready_for_auto_publish(env, job_id=job_id, visibility="public")
            os.makedirs("/tmp/yt-tokens/darkwood-reverie", exist_ok=True)
            with open("/tmp/yt-tokens/darkwood-reverie/token.json", "w", encoding="utf-8") as fh:
                fh.write("{}")

            from unittest import mock

            with mock.patch("services.workers.uploader.YouTubeClient") as yt_cls:
                yt_cls.return_value.set_video_privacy.side_effect = TimeoutError("timeout")
                uploader_cycle(env=env, worker_id="t-upl")

            conn = dbm.connect(env)
            try:
                job = dict(dbm.get_job(conn, job_id) or {})
                self.assertEqual(job["publish_state"], "retry_pending")
                self.assertEqual(job["publish_attempt_count"], 1)
                self.assertEqual(job["publish_last_error_code"], "timeout")
                self.assertIsNotNone(job["publish_retry_at"])
            finally:
                conn.close()

    def test_auto_publish_attempt_three_retriable_exhausts_to_manual_handoff(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/tmp/client.json"
            os.environ["YT_TOKENS_DIR"] = "/tmp/yt-tokens"
            env = Env.load()
            seed_minimal_db(env)
            self._seed_policy(env, publish_mode="auto", global_pause=False)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            self._mark_ready_for_auto_publish(env, job_id=job_id, visibility="public")
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE jobs SET publish_attempt_count = 2 WHERE id = ?", (job_id,))
            finally:
                conn.close()
            os.makedirs("/tmp/yt-tokens/darkwood-reverie", exist_ok=True)
            with open("/tmp/yt-tokens/darkwood-reverie/token.json", "w", encoding="utf-8") as fh:
                fh.write("{}")

            from unittest import mock

            with mock.patch("services.workers.uploader.YouTubeClient") as yt_cls:
                yt_cls.return_value.set_video_privacy.side_effect = TimeoutError("timeout")
                uploader_cycle(env=env, worker_id="t-upl")

            conn = dbm.connect(env)
            try:
                job = dict(dbm.get_job(conn, job_id) or {})
                self.assertEqual(job["publish_state"], "manual_handoff_pending")
                self.assertEqual(job["publish_attempt_count"], 3)
                self.assertEqual(job["publish_reason_code"], "retries_exhausted")
                self.assertIsNone(job["publish_retry_at"])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
