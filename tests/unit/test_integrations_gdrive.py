from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


class _FakeDownloader:
    def __init__(self, fh, req):
        self._fh = fh
        self._req = req
        self._called = 0

    def next_chunk(self):
        self._called += 1
        if self._called == 1:
            self._fh.write(b"hello")
            return None, False
        return None, True


class TestGDriveIntegrationMocked(unittest.TestCase):
    def test_init_raises_when_deps_missing(self):
        from services.integrations import gdrive as gdm

        with patch.object(gdm, "_GOOGLE_IMPORT_ERROR", Exception("missing")):
            with self.assertRaises(RuntimeError):
                gdm.DriveClient(service_account_json="x", oauth_client_json="", oauth_token_json="")

    def test_init_service_account_flow(self):
        from services.integrations import gdrive as gdm

        fake_creds = SimpleNamespace(valid=True)
        sa = SimpleNamespace(Credentials=SimpleNamespace(from_service_account_file=Mock(return_value=fake_creds)))
        svc = SimpleNamespace(files=Mock())

        with (
            patch.object(gdm, "_GOOGLE_IMPORT_ERROR", None),
            patch.object(gdm, "service_account", sa),
            patch.object(gdm, "build", Mock(return_value=svc)),
        ):
            c = gdm.DriveClient(service_account_json="sa.json", oauth_client_json="", oauth_token_json="")

        self.assertIsNotNone(c)

    def test_init_oauth_refresh_flow_writes_token(self):
        from services.integrations import gdrive as gdm

        with tempfile.TemporaryDirectory() as td:
            token = Path(td) / "token.json"
            client = Path(td) / "client.json"
            client.write_text("{}", encoding="utf-8")

            creds = SimpleNamespace(valid=False, expired=True, refresh_token="rt")
            creds.refresh = Mock()
            creds.to_json = Mock(return_value="{\"ok\": true}")

            with (
                patch.object(gdm, "_GOOGLE_IMPORT_ERROR", None),
                patch.object(gdm, "Credentials", SimpleNamespace(from_authorized_user_file=Mock(return_value=creds))),
                patch.object(gdm, "Request", object),
                patch.object(gdm, "InstalledAppFlow", SimpleNamespace(from_client_secrets_file=Mock())),
                patch.object(gdm, "build", Mock(return_value=SimpleNamespace(files=Mock()))),
            ):
                gdm.DriveClient(service_account_json="", oauth_client_json=str(client), oauth_token_json=str(token))

            creds.refresh.assert_called_once()
            self.assertTrue(token.exists())

    def test_init_oauth_console_flow_when_no_refresh(self):
        from services.integrations import gdrive as gdm

        with tempfile.TemporaryDirectory() as td:
            token = Path(td) / "token.json"
            client = Path(td) / "client.json"
            client.write_text("{}", encoding="utf-8")

            creds = SimpleNamespace(valid=False, expired=False, refresh_token=None)
            creds.to_json = Mock(return_value="{\"ok\": true}")
            flow = SimpleNamespace(run_console=Mock(return_value=creds))

            with (
                patch.object(gdm, "_GOOGLE_IMPORT_ERROR", None),
                patch.object(gdm, "Credentials", SimpleNamespace(from_authorized_user_file=Mock(return_value=creds))),
                patch.object(gdm, "InstalledAppFlow", SimpleNamespace(from_client_secrets_file=Mock(return_value=flow))),
                patch.object(gdm, "build", Mock(return_value=SimpleNamespace(files=Mock()))),
            ):
                gdm.DriveClient(service_account_json="", oauth_client_json=str(client), oauth_token_json=str(token))

            flow.run_console.assert_called_once()
            self.assertTrue(token.exists())

    def test_list_children_paginates(self):
        from services.integrations import gdrive as gdm

        page1 = {"files": [{"id": "1", "name": "a", "mimeType": "x"}], "nextPageToken": "t"}
        page2 = {"files": [{"id": "2", "name": "b", "mimeType": "y"}]}

        exec_mock = Mock(side_effect=[page1, page2])
        list_mock = Mock(return_value=SimpleNamespace(execute=exec_mock))
        files_obj = SimpleNamespace(list=list_mock, get_media=Mock())
        svc = SimpleNamespace(files=Mock(return_value=files_obj))

        with (
            patch.object(gdm, "_GOOGLE_IMPORT_ERROR", None),
            patch.object(
                gdm,
                "service_account",
                SimpleNamespace(Credentials=SimpleNamespace(from_service_account_file=Mock(return_value=SimpleNamespace(valid=True)))),
            ),
            patch.object(gdm, "build", Mock(return_value=svc)),
        ):
            c = gdm.DriveClient(service_account_json="sa.json", oauth_client_json="", oauth_token_json="")
            items = c.list_children("parent")

        self.assertEqual([i.id for i in items], ["1", "2"])

    def test_find_child_folder_and_file(self):
        from services.integrations import gdrive as gdm

        items = [
            gdm.DriveItem(id="f", name="folder", mime_type="application/vnd.google-apps.folder"),
            gdm.DriveItem(id="x", name="file.txt", mime_type="text/plain"),
        ]

        c = object.__new__(gdm.DriveClient)
        c._svc = None
        with patch.object(gdm.DriveClient, "list_children", Mock(return_value=items)):
            self.assertIsNotNone(gdm.DriveClient.find_child_folder(c, "p", "folder"))
            self.assertIsNotNone(gdm.DriveClient.find_child_file(c, "p", "file.txt"))

    def test_download_text_and_to_path(self):
        from services.integrations import gdrive as gdm

        files_obj = SimpleNamespace(get_media=Mock(return_value=object()))
        svc = SimpleNamespace(files=Mock(return_value=files_obj))

        c = object.__new__(gdm.DriveClient)
        c._svc = svc

        with patch.object(gdm, "MediaIoBaseDownload", _FakeDownloader):
            txt = gdm.DriveClient.download_text(c, "file")
            self.assertEqual(txt, "hello")

            with tempfile.TemporaryDirectory() as td:
                dest = Path(td) / "out.txt"
                gdm.DriveClient.download_to_path(c, "file", dest)
                self.assertTrue(dest.exists())
                self.assertGreater(dest.stat().st_size, 0)
