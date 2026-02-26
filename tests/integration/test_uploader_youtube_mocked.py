from __future__ import annotations

import os
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

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
    def test_youtube_backend_uses_channel_token_convention(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/env/client_secret.json"
            with tempfile.TemporaryDirectory() as tokens_dir:
                os.environ["YT_TOKENS_DIR"] = tokens_dir
                env = Env.load()
                seed_minimal_db(env)

                job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-b")
                ob = outbox_dir(env, job_id)
                mp4 = ob / "render.mp4"
                mp4.parent.mkdir(parents=True, exist_ok=True)
                mp4.write_bytes(b"mp4")

                token_path = Path(tokens_dir) / "channel-b" / "token.json"
                token_path.parent.mkdir(parents=True, exist_ok=True)
                token_path.write_text("{}", encoding="utf-8")

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
                self.assertEqual(_FakeYT.last_init, ("/env/client_secret.json", str(token_path)))

    def test_youtube_backend_missing_channel_token_is_terminal_upload_failed(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/env/client_secret.json"
            with tempfile.TemporaryDirectory() as tokens_dir:
                os.environ["YT_TOKENS_DIR"] = tokens_dir
                env = Env.load()
                seed_minimal_db(env)

                job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-b")
                ob = outbox_dir(env, job_id)
                mp4 = ob / "render.mp4"
                mp4.parent.mkdir(parents=True, exist_ok=True)
                mp4.write_bytes(b"mp4")

                uploader_cycle(env=env, worker_id="t-upl")

                conn = dbm.connect(env)
                try:
                    job = dbm.get_job(conn, job_id)
                finally:
                    conn.close()

                assert job is not None
                expected_path = str(Path(tokens_dir) / "channel-b" / "token.json")
                self.assertEqual(str(job["state"]), "UPLOAD_FAILED")
                self.assertEqual(
                    str(job["error_reason"]),
                    f"YouTube token missing for channel channel-b at {expected_path}",
                )


if __name__ == "__main__":
    unittest.main()
