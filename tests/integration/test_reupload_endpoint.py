from __future__ import annotations

import importlib
import os
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir
from services.workers.uploader import uploader_cycle

from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class _FakeYT:
    upload_calls = 0

    def __init__(self, *, client_secret_json: str, token_json: str):
        self.client_secret_json = client_secret_json
        self.token_json = token_json

    def upload_private(
        self,
        *,
        video_path: Path,
        title: str,
        description: str,
        tags: list[str],
        audience_is_for_kids: bool = False,
        video_language: str = "en",
    ):
        _FakeYT.upload_calls += 1
        return type("R", (), {"video_id": "vid-fresh"})()

    def set_thumbnail(self, *, video_id: str, image_path: Path) -> None:
        return None

    def has_playlist_management_scope(self) -> bool:
        return True


class TestUiJobReuploadEndpoint(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _create_upload_failed_job(self, env: Env) -> int:
        job_id = insert_release_and_job(env, state="UPLOAD_FAILED", stage="UPLOAD", channel_slug="darkwood-reverie")
        conn = dbm.connect(env)
        try:
            conn.execute(
                "INSERT INTO youtube_uploads(job_id, video_id, url, studio_url, privacy, uploaded_at, error) VALUES(?,?,?,?,?,?,?)",
                (job_id, "vid-old", "https://old", "https://studio-old", "private", dbm.now_ts(), "old failure"),
            )
            conn.commit()
        finally:
            conn.close()
        return job_id

    def test_auth_required(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_upload_failed_job(env)
            _, client = self._new_client()
            resp = client.post(f"/v1/ui/jobs/{job_id}/reupload")
            self.assertEqual(resp.status_code, 401)

    def test_invalid_state_returns_409(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/ui/jobs/{job_id}/reupload", headers=h)
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "UIJ_REUPLOAD_NOT_ALLOWED")

    def test_requires_rendered_mp4(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_upload_failed_job(env)
            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/ui/jobs/{job_id}/reupload", headers=h)
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "UIJ_REUPLOAD_RENDER_MISSING")

    def test_reupload_requeues_upload_and_clears_stale_youtube_row(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_upload_failed_job(env)
            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/ui/jobs/{job_id}/reupload", headers=h)
            self.assertEqual(resp.status_code, 200)
            self.assertTrue(resp.json()["enqueued"])

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                row = conn.execute("SELECT * FROM youtube_uploads WHERE job_id = ?", (job_id,)).fetchone()
            finally:
                conn.close()
            self.assertEqual(str(job["state"]), "UPLOADING")
            self.assertEqual(str(job["stage"]), "UPLOAD")
            self.assertEqual(str(job["error_reason"] or ""), "")
            self.assertIsNone(row)

    def test_reupload_with_existing_youtube_row_runs_fresh_upload(self) -> None:
        with temp_env() as (_, _):
            os.environ["UPLOAD_BACKEND"] = "youtube"
            os.environ["YT_CLIENT_SECRET_JSON"] = "/env/client_secret.json"
            os.environ["YT_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "yt_tokens")
            env = Env.load()
            seed_minimal_db(env)
            job_id = self._create_upload_failed_job(env)

            mp4 = outbox_dir(env, job_id) / "render.mp4"
            mp4.parent.mkdir(parents=True, exist_ok=True)
            mp4.write_bytes(b"mp4")
            token = Path(env.yt_tokens_dir) / "darkwood-reverie" / "token.json"
            token.parent.mkdir(parents=True, exist_ok=True)
            token.write_text("{}", encoding="utf-8")

            _FakeYT.upload_calls = 0
            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/ui/jobs/{job_id}/reupload", headers=h)
            self.assertEqual(resp.status_code, 200)

            import services.workers.uploader as uploader_mod

            old = uploader_mod.YouTubeClient
            uploader_mod.YouTubeClient = _FakeYT  # type: ignore[assignment]
            try:
                uploader_cycle(env=env, worker_id="wu-reupload")
            finally:
                uploader_mod.YouTubeClient = old  # type: ignore[assignment]

            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                row = conn.execute("SELECT video_id FROM youtube_uploads WHERE job_id = ?", (job_id,)).fetchone()
            finally:
                conn.close()

            self.assertEqual(_FakeYT.upload_calls, 1)
            self.assertEqual(str(job["state"]), "WAIT_APPROVAL")
            assert row is not None
            self.assertEqual(str(row["video_id"]), "vid-fresh")


if __name__ == "__main__":
    unittest.main()
