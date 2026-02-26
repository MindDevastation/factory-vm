from __future__ import annotations

import os
import unittest
from dataclasses import dataclass

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir
from services.workers.uploader import uploader_cycle

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job


@dataclass(frozen=True)
class _Res:
    video_id: str


class _FakeYT:
    last_init: tuple[str, str] | None = None

    def __init__(self, *, client_secret_json: str, token_json: str):
        self._thumb_calls = 0
        _FakeYT.last_init = (client_secret_json, token_json)

    def upload_private(self, *, video_path, title, description, tags):
        return _Res(video_id="vid123")

    def set_thumbnail(self, *, video_id: str, image_path):
        self._thumb_calls += 1


class TestUploaderYoutubeMocked(unittest.TestCase):
    def test_youtube_backend_path(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/env/client_secret.json"
            os.environ["YT_TOKEN_JSON"] = "/env/token.json"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD")
            ob = outbox_dir(env, job_id)
            mp4 = ob / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            cover = ob / "cover" / "cover.png"
            cover.parent.mkdir(parents=True, exist_ok=True)
            cover.write_bytes(b"png")

            import services.workers.uploader as upl

            old = upl.YouTubeClient
            upl.YouTubeClient = _FakeYT  # type: ignore[assignment]
            try:
                uploader_cycle(env=env, worker_id="t-upl")
            finally:
                upl.YouTubeClient = old  # type: ignore[assignment]

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                yt = conn.execute("SELECT * FROM youtube_uploads WHERE job_id=?", (job_id,)).fetchone()
            finally:
                conn.close()

            assert job is not None
            self.assertEqual(job["state"], "WAIT_APPROVAL")
            self.assertIsNotNone(yt)
            assert yt is not None
            self.assertEqual(yt["video_id"], "vid123")

    def test_channel_slug_uses_convention_token_path(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/env/client_secret.json"
            os.environ["YT_TOKEN_BASE_DIR"] = "/secure/youtube"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-b")
            ob = outbox_dir(env, job_id)
            mp4 = ob / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            import services.workers.uploader as upl

            old = upl.YouTubeClient
            upl.YouTubeClient = _FakeYT  # type: ignore[assignment]
            try:
                uploader_cycle(env=env, worker_id="t-upl")
            finally:
                upl.YouTubeClient = old  # type: ignore[assignment]

            self.assertEqual(_FakeYT.last_init, ("/env/client_secret.json", "/secure/youtube/channel-b/token.json"))

    def test_channel_credentials_fallback_to_global_env_when_base_dir_missing(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/env/client_secret.json"
            os.environ["YT_TOKEN_JSON"] = "/env/token.json"
            os.environ["YT_TOKEN_BASE_DIR"] = ""
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-c")
            ob = outbox_dir(env, job_id)
            mp4 = ob / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            import services.workers.uploader as upl

            old = upl.YouTubeClient
            upl.YouTubeClient = _FakeYT  # type: ignore[assignment]
            try:
                uploader_cycle(env=env, worker_id="t-upl")
            finally:
                upl.YouTubeClient = old  # type: ignore[assignment]

            self.assertEqual(_FakeYT.last_init, ("/env/client_secret.json", "/env/token.json"))

    def test_terminal_failure_when_credentials_missing(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-d")
            ob = outbox_dir(env, job_id)
            mp4 = ob / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            uploader_cycle(env=env, worker_id="t-upl")

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                yt = conn.execute("SELECT * FROM youtube_uploads WHERE job_id=?", (job_id,)).fetchone()
            finally:
                conn.close()

            assert job is not None
            self.assertEqual(job["state"], "UPLOAD_FAILED")
            self.assertIn("YT_TOKEN_BASE_DIR", str(job["error_reason"]))
            self.assertIsNotNone(yt)
            assert yt is not None
            self.assertIn("YT_TOKEN_JSON", str(yt["error"]))


if __name__ == "__main__":
    unittest.main()
