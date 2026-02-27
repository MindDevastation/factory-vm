from __future__ import annotations

import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
