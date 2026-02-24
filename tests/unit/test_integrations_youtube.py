from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


class TestYouTubeIntegrationMocked(unittest.TestCase):
    def test_init_raises_when_deps_missing(self):
        from services.integrations import youtube as ytm

        with patch.object(ytm, "_GOOGLE_IMPORT_ERROR", Exception("missing")):
            with self.assertRaises(RuntimeError):
                ytm.YouTubeClient(client_secret_json="x.json", token_json="t.json")

    def test_init_refresh_token_flow_writes_token(self):
        from services.integrations import youtube as ytm

        with tempfile.TemporaryDirectory() as td:
            token = Path(td) / "token.json"
            client = Path(td) / "client.json"
            client.write_text("{}", encoding="utf-8")

            creds = SimpleNamespace(valid=False, expired=True, refresh_token="rt")
            creds.refresh = Mock()
            creds.to_json = Mock(return_value="{\"ok\": true}")

            fake_build = Mock(return_value=SimpleNamespace())

            with (
                patch.object(ytm, "_GOOGLE_IMPORT_ERROR", None),
                patch.object(ytm, "Credentials", SimpleNamespace(from_authorized_user_file=Mock(return_value=creds))),
                patch.object(ytm, "Request", object),
                patch.object(ytm, "InstalledAppFlow", SimpleNamespace(from_client_secrets_file=Mock())),
                patch.object(ytm, "build", fake_build),
            ):
                ytm.YouTubeClient(client_secret_json=str(client), token_json=str(token))

            creds.refresh.assert_called_once()
            self.assertTrue(token.exists())

    def test_init_console_flow_when_no_refresh_token(self):
        from services.integrations import youtube as ytm

        with tempfile.TemporaryDirectory() as td:
            token = Path(td) / "token.json"
            client = Path(td) / "client.json"
            client.write_text("{}", encoding="utf-8")

            creds = SimpleNamespace(valid=False, expired=False, refresh_token=None)
            creds.to_json = Mock(return_value="{\"ok\": true}")

            flow = SimpleNamespace(run_console=Mock(return_value=creds))

            with (
                patch.object(ytm, "_GOOGLE_IMPORT_ERROR", None),
                patch.object(ytm, "Credentials", SimpleNamespace(from_authorized_user_file=Mock(return_value=creds))),
                patch.object(ytm, "InstalledAppFlow", SimpleNamespace(from_client_secrets_file=Mock(return_value=flow))),
                patch.object(ytm, "build", Mock(return_value=SimpleNamespace())),
            ):
                ytm.YouTubeClient(client_secret_json=str(client), token_json=str(token))

            self.assertTrue(token.exists())
            flow.run_console.assert_called_once()

    def test_upload_private_strips_hash_and_loops_until_resp(self):
        from services.integrations import youtube as ytm

        yt = SimpleNamespace()
        req = SimpleNamespace(next_chunk=Mock(side_effect=[(None, None), (None, {"id": "VID"})]))
        yt.videos = Mock(return_value=SimpleNamespace(insert=Mock(return_value=req)))

        with (
            patch.object(ytm, "_GOOGLE_IMPORT_ERROR", None),
            patch.object(ytm, "Credentials", SimpleNamespace(from_authorized_user_file=Mock(return_value=SimpleNamespace(valid=True)))),
            patch.object(ytm, "build", Mock(return_value=yt)),
            patch.object(ytm, "MediaFileUpload", Mock()),
        ):
            c = ytm.YouTubeClient(client_secret_json="client.json", token_json="token.json")
            c._yt = yt
            res = c.upload_private(video_path=Path("/tmp/a.mp4"), title="t", description="d", tags=["#a", "b", "", "#c"])

        self.assertEqual(res.video_id, "VID")
        body = yt.videos.return_value.insert.call_args.kwargs["body"]
        self.assertEqual(body["snippet"]["tags"], ["a", "b", "c"])

    def test_set_thumbnail_executes(self):
        from services.integrations import youtube as ytm

        yt = SimpleNamespace()
        yt.thumbnails = Mock(return_value=SimpleNamespace(set=Mock(return_value=SimpleNamespace(execute=Mock()))))

        with (
            patch.object(ytm, "_GOOGLE_IMPORT_ERROR", None),
            patch.object(ytm, "Credentials", SimpleNamespace(from_authorized_user_file=Mock(return_value=SimpleNamespace(valid=True)))),
            patch.object(ytm, "build", Mock(return_value=yt)),
            patch.object(ytm, "MediaFileUpload", Mock()),
        ):
            c = ytm.YouTubeClient(client_secret_json="client.json", token_json="token.json")
            c._yt = yt
            c.set_thumbnail(video_id="VID", image_path=Path("/tmp/x.png"))

        yt.thumbnails.return_value.set.assert_called_once()
