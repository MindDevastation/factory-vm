from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from services.common.env import Env
from services.workers.track_jobs import _build_track_catalog_drive_client, _track_catalog_token_path


class TestWorkerTrackJobsGDriveToken(unittest.TestCase):
    def _make_env(self, *, tokens_dir: str, client_secret: str) -> Env:
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
            gdrive_sa_json="/secure/sa.json",
            gdrive_oauth_client_json="/secure/global_client.json",
            gdrive_oauth_token_json="/secure/global_token.json",
            oauth_redirect_base_url="",
            oauth_state_secret="",
            gdrive_client_secret_json=client_secret,
            gdrive_tokens_dir=tokens_dir,
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

    def test_track_catalog_token_path_uses_channel_slug(self) -> None:
        env = self._make_env(tokens_dir="/secure/gdrive/channels", client_secret="/secure/gdrive/client_secret.json")
        token_path = _track_catalog_token_path(env=env, channel_slug="darkwood-reverie")
        self.assertEqual(str(token_path), "/secure/gdrive/channels/darkwood-reverie/token.json")

    def test_build_drive_client_uses_per_channel_token_and_client_secret_only(self) -> None:
        with patch("services.workers.track_jobs.oauth_token_path", return_value=Path("/tmp/tokens/ch-a/token.json")), patch(
            "services.workers.track_jobs.os.access", return_value=True
        ), patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.is_file", return_value=True), patch(
            "services.workers.track_jobs.DriveClient"
        ) as drive_cls:
            env = self._make_env(tokens_dir="/secure/gdrive/channels", client_secret="/secure/gdrive/client_secret.json")
            _build_track_catalog_drive_client(env=env, channel_slug="ch-a")

        drive_cls.assert_called_once_with(
            service_account_json="",
            oauth_client_json="/secure/gdrive/client_secret.json",
            oauth_token_json="/tmp/tokens/ch-a/token.json",
        )


if __name__ == "__main__":
    unittest.main()
