from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import json
import unittest
from unittest import mock

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import outbox_dir
from services.workers import uploader as uploader_worker
from tests._helpers import insert_release_and_job, seed_minimal_db, temp_env


class _PlaylistYT:
    def __init__(self) -> None:
        self.created: dict[str, str] = {}
        self.added: list[tuple[str, str]] = []
        self.fail_ambiguous = False
        self.fail_once_add = False
        self._failed_once = False

    def upload_private(self, *, video_path: Path, title: str, description: str, tags: list[str]):
        return type("R", (), {"video_id": "vid123"})()

    def set_thumbnail(self, *, video_id: str, image_path: Path) -> None:
        return None

    def resolve_or_create_playlist(self, *, title: str) -> tuple[str, bool]:
        if self.fail_ambiguous:
            raise RuntimeError(f"ambiguous playlist title match: {title}")
        if title in self.created:
            return self.created[title], False
        playlist_id = f"NEW_{len(self.created)+1}"
        self.created[title] = playlist_id
        return playlist_id, True

    def add_video_to_playlist(self, *, playlist_id: str, video_id: str) -> None:
        if self.fail_once_add and not self._failed_once:
            self._failed_once = True
            raise RuntimeError("attach fail")
        self.added.append((playlist_id, video_id))


class TestUploaderPlaylistAssignment(unittest.TestCase):
    def _seed_upload_job(self, env: Env) -> int:
        job_id = insert_release_and_job(env, state="UPLOADING", stage="UPLOAD", channel_slug="channel-b")
        mp4 = outbox_dir(env, job_id) / "render.mp4"
        mp4.parent.mkdir(parents=True, exist_ok=True)
        mp4.write_bytes(b"x")
        return job_id

    def _set_release_playlist_targets(self, env: Env, *, job_id: int, playlist_ids: list[str], playlist_create_title: str | None) -> None:
        conn = dbm.connect(env)
        try:
            conn.execute(
                """
                UPDATE releases
                SET playlists_json = ?, playlist_create_title = ?
                WHERE id = (SELECT release_id FROM jobs WHERE id = ?)
                """,
                (json.dumps(playlist_ids), playlist_create_title, job_id),
            )
        finally:
            conn.close()

    def test_upload_assigns_existing_playlist_ids(self) -> None:
        with temp_env() as (_, env0):
            env = replace(env0, upload_backend="youtube", yt_client_secret_json="/env/client.json", yt_tokens_dir="/tmp/yt-tokens")
            seed_minimal_db(env)
            job_id = self._seed_upload_job(env)
            self._set_release_playlist_targets(env, job_id=job_id, playlist_ids=["PL_A"], playlist_create_title=None)
            token = Path(env.yt_tokens_dir) / "channel-b" / "token.json"
            token.parent.mkdir(parents=True, exist_ok=True)
            token.write_text("{}", encoding="utf-8")
            yt = _PlaylistYT()
            with mock.patch.object(uploader_worker, "YouTubeClient", lambda *a, **k: yt):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")
            self.assertEqual(yt.added, [("PL_A", "vid123")])

    def test_upload_assigns_create_title_only_and_union_with_existing_ids(self) -> None:
        with temp_env() as (_, env0):
            env = replace(env0, upload_backend="youtube", yt_client_secret_json="/env/client.json", yt_tokens_dir="/tmp/yt-tokens")
            seed_minimal_db(env)
            job_id = self._seed_upload_job(env)
            self._set_release_playlist_targets(env, job_id=job_id, playlist_ids=["PL_A"], playlist_create_title="My New Playlist")
            token = Path(env.yt_tokens_dir) / "channel-b" / "token.json"
            token.parent.mkdir(parents=True, exist_ok=True)
            token.write_text("{}", encoding="utf-8")
            yt = _PlaylistYT()
            with mock.patch.object(uploader_worker, "YouTubeClient", lambda *a, **k: yt):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")
            self.assertIn(("PL_A", "vid123"), yt.added)
            self.assertIn(("NEW_1", "vid123"), yt.added)

    def test_ambiguous_title_sets_retry_instead_of_false_success(self) -> None:
        with temp_env() as (_, env0):
            env = replace(env0, upload_backend="youtube", yt_client_secret_json="/env/client.json", yt_tokens_dir="/tmp/yt-tokens")
            seed_minimal_db(env)
            job_id = self._seed_upload_job(env)
            self._set_release_playlist_targets(env, job_id=job_id, playlist_ids=[], playlist_create_title="Dup")
            token = Path(env.yt_tokens_dir) / "channel-b" / "token.json"
            token.parent.mkdir(parents=True, exist_ok=True)
            token.write_text("{}", encoding="utf-8")
            yt = _PlaylistYT()
            yt.fail_ambiguous = True
            with mock.patch.object(uploader_worker, "YouTubeClient", lambda *a, **k: yt):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")
            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertNotEqual(str(job["state"]), "WAIT_APPROVAL")
            finally:
                conn.close()

    def test_retry_does_not_duplicate_playlist_creation(self) -> None:
        with temp_env() as (_, env0):
            env = replace(env0, upload_backend="youtube", yt_client_secret_json="/env/client.json", yt_tokens_dir="/tmp/yt-tokens")
            seed_minimal_db(env)
            job_id = self._seed_upload_job(env)
            self._set_release_playlist_targets(env, job_id=job_id, playlist_ids=[], playlist_create_title="Retry Playlist")
            token = Path(env.yt_tokens_dir) / "channel-b" / "token.json"
            token.parent.mkdir(parents=True, exist_ok=True)
            token.write_text("{}", encoding="utf-8")
            yt = _PlaylistYT()
            yt.fail_once_add = True
            with mock.patch.object(uploader_worker, "YouTubeClient", lambda *a, **k: yt):
                uploader_worker.uploader_cycle(env=env, worker_id="wu")
                uploader_worker.uploader_cycle(env=env, worker_id="wu")
            self.assertEqual(len(yt.created), 1)


if __name__ == "__main__":
    unittest.main()
