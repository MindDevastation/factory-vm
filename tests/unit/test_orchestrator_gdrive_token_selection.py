from __future__ import annotations

import tempfile
import unittest
import os

from services.common import db as dbm
from services.workers.orchestrator import orchestrator_cycle

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job
from pathlib import Path
from unittest.mock import patch

from services.common.env import Env
from services.workers.orchestrator import _fetch_asset_to


class FakeDriveClient:
    def __init__(self, *, service_account_json: str, oauth_client_json: str, oauth_token_json: str):
        self.service_account_json = service_account_json
        self.oauth_client_json = oauth_client_json
        self.oauth_token_json = oauth_token_json

    def download_to_path(self, _origin_id: str, dest: Path) -> None:
        Path(dest).write_text("ok", encoding="utf-8")


def _make_env(*, gdrive_sa_json: str, gdrive_tokens_dir: str, gdrive_oauth_token_json: str) -> Env:
    return Env(
        db_path="db.sqlite3",
        storage_root="storage",
        bind="0.0.0.0",
        port=8080,
        basic_user="admin",
        basic_pass="pass",
        origin_backend="gdrive",
        origin_local_root="",
        upload_backend="mock",
        telegram_enabled=0,
        gdrive_root_id="",
        gdrive_library_root_id="",
        gdrive_sa_json=gdrive_sa_json,
        gdrive_oauth_client_json="dummy",
        gdrive_oauth_token_json=gdrive_oauth_token_json,
        oauth_redirect_base_url="",
        oauth_state_secret="",
        gdrive_client_secret_json="",
        gdrive_tokens_dir=gdrive_tokens_dir,
        yt_client_secret_json="",
        yt_tokens_dir="",
        tg_bot_token="",
        tg_admin_chat_id=0,
        qa_volumedetect_seconds=60,
        job_lock_ttl_sec=3600,
        retry_backoff_sec=300,
        max_render_attempts=3,
        max_upload_attempts=3,
        worker_sleep_sec=1,
        metadata_preview_ttl_sec=1800,
    )


class TestOrchestratorGDriveTokenSelection(unittest.TestCase):
    def test_fetch_asset_uses_per_channel_oauth_token(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            slug = "channel-slug"
            token_path = Path(td) / slug / "token.json"
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            env = _make_env(
                gdrive_sa_json="",
                gdrive_tokens_dir=td,
                gdrive_oauth_token_json="/secure/gdrive_token.json",
            )
            asset = {"origin": "GDRIVE", "origin_id": "file-id"}
            dest = Path(td) / "downloaded.dat"

            with patch("services.workers.orchestrator.DriveClient", FakeDriveClient):
                drive = _fetch_asset_to(env=env, drive=None, asset=asset, dest=dest, channel_slug=slug)

            self.assertIsNotNone(drive)
            self.assertEqual(drive.oauth_token_json, str(token_path))

    def test_fetch_asset_raises_when_per_channel_token_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = _make_env(
                gdrive_sa_json="",
                gdrive_tokens_dir=td,
                gdrive_oauth_token_json="/secure/gdrive_token.json",
            )
            asset = {"origin": "GDRIVE", "origin_id": "file-id"}
            dest = Path(td) / "downloaded.dat"

            with patch("services.workers.orchestrator.DriveClient", FakeDriveClient):
                with self.assertRaisesRegex(RuntimeError, r"^GDrive token missing for channel"):
                    _fetch_asset_to(env=env, drive=None, asset=asset, dest=dest, channel_slug="missing-slug")

    def test_fetch_asset_force_refetch_local_replaces_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = _make_env(
                gdrive_sa_json="",
                gdrive_tokens_dir=td,
                gdrive_oauth_token_json="/secure/gdrive_token.json",
            )
            src = Path(td) / "source.wav"
            src.write_text("fresh", encoding="utf-8")
            dest = Path(td) / "dst.wav"
            dest.write_text("stale", encoding="utf-8")
            asset = {"origin": "LOCAL", "origin_id": str(src)}

            _fetch_asset_to(env=env, drive=None, asset=asset, dest=dest, channel_slug="slug", force_refetch_inputs=True)

            self.assertEqual(dest.read_text(encoding="utf-8"), "fresh")

    def test_fetch_asset_default_behavior_keeps_existing_path_valid_for_copy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = _make_env(
                gdrive_sa_json="",
                gdrive_tokens_dir=td,
                gdrive_oauth_token_json="/secure/gdrive_token.json",
            )
            src = Path(td) / "source.wav"
            src.write_text("fresh", encoding="utf-8")
            dest = Path(td) / "dst.wav"
            dest.write_text("stale", encoding="utf-8")
            asset = {"origin": "LOCAL", "origin_id": str(src)}

            _fetch_asset_to(env=env, drive=None, asset=asset, dest=dest, channel_slug="slug")

            self.assertEqual(dest.read_text(encoding="utf-8"), "fresh")

    def test_force_refetch_applies_to_separate_cover_fetch(self) -> None:
        class _FakeProc:
            def __init__(self, *, release_dir: Path):
                self._release_dir = release_dir
                self.stdout = iter(["0.0 %", "100.0 %"])

            def terminate(self):
                return None

            def wait(self):
                self._release_dir.mkdir(parents=True, exist_ok=True)
                (self._release_dir / "out.mp4").write_bytes(b"mp4")
                return 0

        with temp_env() as (_, _env0):
            os.environ["ORIGIN_BACKEND"] = "local"
            env = Env.load()
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="READY_FOR_RENDER", stage="FETCH")

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE jobs SET force_refetch_inputs = 1 WHERE id = ?", (job_id,))
                job = dbm.get_job(conn, job_id)
                assert job is not None
                ch = dbm.get_channel_by_slug(conn, str(job["channel_slug"]))
                assert ch is not None
                ch_id = int(ch["id"])

                base = Path(env.storage_root) / "test_inputs" / f"job_{job_id}_separate_cover"
                base.mkdir(parents=True, exist_ok=True)

                track = base / "track_1.wav"
                track.write_bytes(b"RIFF0000WAVEfmt ")
                bg = base / "bg.png"
                bg.write_text("bg-fresh", encoding="utf-8")
                cover = base / "cover.png"
                cover.write_text("cover-fresh", encoding="utf-8")

                tid = dbm.create_asset(conn, channel_id=ch_id, kind="AUDIO", origin="LOCAL", origin_id=str(track), name=track.name, path=str(track))
                dbm.link_job_input(conn, job_id, tid, "TRACK", 0)
                bid = dbm.create_asset(conn, channel_id=ch_id, kind="IMAGE", origin="LOCAL", origin_id=str(bg), name=bg.name, path=str(bg))
                dbm.link_job_input(conn, job_id, bid, "BACKGROUND", 0)
                cid = dbm.create_asset(conn, channel_id=ch_id, kind="IMAGE", origin="LOCAL", origin_id=str(cover), name=cover.name, path=str(cover))
                dbm.link_job_input(conn, job_id, cid, "COVER", 0)
            finally:
                conn.close()

            def _fake_preview(*, src_mp4: Path, dst_mp4: Path, seconds: int, width: int, height: int, fps: int, v_bitrate: str, a_bitrate: str):
                dst_mp4.parent.mkdir(parents=True, exist_ok=True)
                dst_mp4.write_bytes(b"preview")

            real_fetch = __import__("services.workers.orchestrator", fromlist=["_fetch_asset_to"])._fetch_asset_to

            def _fake_fetch_asset_to(**kwargs):
                dest = Path(kwargs["dest"])
                force = bool(kwargs.get("force_refetch_inputs", False))

                if dest.name == "cover.png" and "tmp_cover" in str(dest):
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text("stale-cover", encoding="utf-8")
                    if not force:
                        return kwargs.get("drive")

                return real_fetch(**kwargs)

            release_dir = Path(env.storage_root) / "workspace" / f"job_{job_id}" / "YouTubeRoot" / "Darkwood Reverie" / "Release"

            with patch("services.workers.orchestrator.subprocess.Popen", lambda *a, **k: _FakeProc(release_dir=release_dir)), patch(
                "services.workers.orchestrator.make_preview_60s", _fake_preview
            ), patch("services.workers.orchestrator._fetch_asset_to", _fake_fetch_asset_to):
                orchestrator_cycle(env=env, worker_id="t-orch")

            cover_out = Path(env.storage_root) / "outbox" / f"job_{job_id}" / "cover" / "cover.png"
            self.assertTrue(cover_out.exists())
            self.assertEqual(cover_out.read_text(encoding="utf-8"), "cover-fresh")


if __name__ == "__main__":
    unittest.main()
