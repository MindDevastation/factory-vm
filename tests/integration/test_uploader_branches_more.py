from __future__ import annotations

import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir
from services.workers import uploader as uploader_worker

from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class _YTStub:
    def __init__(self, *a, **k):
        pass

    def upload_private(self, *, video_path: Path, title: str, description: str, tags: list[str]):
        return type("R", (), {"video_id": "vid123"})()

    def set_thumbnail(self, *, video_id: str, image_path: Path) -> None:
        raise RuntimeError("thumb fail")


class _YTNoThumb(_YTStub):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.thumbnail_calls = 0

    def set_thumbnail(self, *, video_id: str, image_path: Path) -> None:
        self.thumbnail_calls += 1


class TestUploaderBranchesMore(unittest.TestCase):
    def _write_channel_token(self, *, tokens_dir: str, channel_slug: str) -> None:
        token_file = Path(tokens_dir) / channel_slug / "token.json"
        token_file.parent.mkdir(parents=True, exist_ok=True)
        token_file.write_text("{}", encoding="utf-8")

    def test_missing_mp4_terminal_when_max_attempts_exceeded(self) -> None:
        with temp_env() as (_, _env0):
            env0 = Env.load()
            env = replace(env0, max_upload_attempts=1)
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")

            uploader_worker.uploader_cycle(env=env, worker_id="wu")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "UPLOAD_FAILED")
            finally:
                conn.close()

    def test_real_youtube_upload_success_and_thumbnail_failure_is_ignored(self) -> None:
        with temp_env() as (_, _env0):
            env0 = Env.load()
            env = replace(env0, upload_backend="youtube", yt_client_secret_json="/env/client_secret.json", yt_tokens_dir="/tmp/yt-tokens")
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-b")
            self._write_channel_token(tokens_dir=env.yt_tokens_dir, channel_slug="channel-b")

            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"x")
            cover_dir = outbox_dir(env, job_id) / "cover"
            cover_dir.mkdir(parents=True, exist_ok=True)
            (cover_dir / "cover.png").write_bytes(b"\x89PNG\r\n\x1a\n")

            with mock.patch.object(uploader_worker, "YouTubeClient", _YTStub):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "WAIT_APPROVAL")
                yt = conn.execute("SELECT video_id FROM youtube_uploads WHERE job_id = ?", (job_id,)).fetchone()
                self.assertEqual(str(yt["video_id"]), "vid123")
            finally:
                conn.close()

    def test_real_youtube_upload_exception_sets_failed_when_max_attempts_1(self) -> None:
        with temp_env() as (_, _env0):
            env0 = Env.load()
            env = replace(env0, upload_backend="youtube", max_upload_attempts=1, yt_client_secret_json="/env/client_secret.json", yt_tokens_dir="/tmp/yt-tokens")
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-b")
            self._write_channel_token(tokens_dir=env.yt_tokens_dir, channel_slug="channel-b")

            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"x")

            class _YTFail(_YTStub):
                def upload_private(self, *, video_path: Path, title: str, description: str, tags: list[str]):
                    raise RuntimeError("upload fail")

            with mock.patch.object(uploader_worker, "YouTubeClient", _YTFail):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "UPLOAD_FAILED")
            finally:
                conn.close()

    def test_get_job_none_releases_lock(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")

            with mock.patch.object(uploader_worker.dbm, "get_job", return_value=None):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")

            conn = dbm.connect(env)
            try:
                row = conn.execute("SELECT locked_by FROM jobs WHERE id = ?", (job_id,)).fetchone()
                self.assertIsNone(row["locked_by"])
            finally:
                conn.close()

    def test_real_youtube_upload_without_cover_skips_thumbnail(self) -> None:
        with temp_env() as (_, _env0):
            env0 = Env.load()
            env = replace(env0, upload_backend="youtube", yt_client_secret_json="/env/client_secret.json", yt_tokens_dir="/tmp/yt-tokens")
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-c")
            self._write_channel_token(tokens_dir=env.yt_tokens_dir, channel_slug="channel-c")

            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"x")

            yt_inst = _YTNoThumb()

            with mock.patch.object(uploader_worker, "YouTubeClient", lambda *a, **k: yt_inst):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "WAIT_APPROVAL")
                self.assertEqual(yt_inst.thumbnail_calls, 0)
            finally:
                conn.close()


    def test_missing_channel_and_env_youtube_token_fails_with_clear_error(self) -> None:
        with temp_env() as (_, _env0):
            env0 = Env.load()
            env = replace(env0, upload_backend="youtube", yt_tokens_dir="/tmp/yt-tokens", yt_client_secret_json="")
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-d")
            self._write_channel_token(tokens_dir=env.yt_tokens_dir, channel_slug="channel-d")

            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"x")

            uploader_worker.uploader_cycle(env=env, worker_id="wu")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "UPLOAD_FAILED")
                self.assertIn("YT_CLIENT_SECRET_JSON is required", str(job["error_reason"]))
                row = conn.execute("SELECT locked_by FROM jobs WHERE id = ?", (job_id,)).fetchone()
                self.assertIsNone(row["locked_by"])
            finally:
                conn.close()


    def test_youtube_client_init_failure_is_terminal(self) -> None:
        with temp_env() as (_, _env0):
            env0 = Env.load()
            env = replace(env0, upload_backend="youtube", yt_client_secret_json="/env/client_secret.json", yt_tokens_dir="/tmp/yt-tokens")
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="titanwave-sonic")
            self._write_channel_token(tokens_dir=env.yt_tokens_dir, channel_slug="titanwave-sonic")

            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"x")

            class _YTInitFail:
                def __init__(self, *a, **k):
                    raise RuntimeError("bad oauth client")

            with mock.patch.object(uploader_worker, "YouTubeClient", _YTInitFail):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "UPLOAD_FAILED")
                self.assertIn("youtube client init failed", str(job["error_reason"]))
            finally:
                conn.close()

    def test_cancel_flag_exception_branch_releases_lock(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")

            with mock.patch.object(uploader_worker, "cancel_flag_path", side_effect=RuntimeError("boom")):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")

            conn = dbm.connect(env)
            try:
                row = conn.execute("SELECT locked_by FROM jobs WHERE id = ?", (job_id,)).fetchone()
                self.assertIsNone(row["locked_by"])
            finally:
                conn.close()
