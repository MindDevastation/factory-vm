from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

# Google Drive deps are optional (only required when ORIGIN_BACKEND=GDRIVE).
_GOOGLE_IMPORT_ERROR: Exception | None = None
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
except Exception as e:  # ImportError in most cases
    _GOOGLE_IMPORT_ERROR = e
    service_account = None  # type: ignore[assignment]
    build = None  # type: ignore[assignment]
    MediaIoBaseDownload = None  # type: ignore[assignment]
    InstalledAppFlow = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]
    Credentials = None  # type: ignore[assignment]

SCOPES = ["https://www.googleapis.com/auth/drive"]


@dataclass(frozen=True)
class DriveItem:
    id: str
    name: str
    mime_type: str


class DriveClient:
    def __init__(self, *, service_account_json: str, oauth_client_json: str, oauth_token_json: str):
        if _GOOGLE_IMPORT_ERROR is not None:
            raise RuntimeError(
                'Google Drive dependencies are missing. Install: ' 
                'pip install google-api-python-client google-auth google-auth-oauthlib'
            )

        creds = None
        if service_account_json:
            creds = service_account.Credentials.from_service_account_file(service_account_json, scopes=SCOPES)
        elif oauth_client_json and oauth_token_json:
            creds = Credentials.from_authorized_user_file(oauth_token_json, SCOPES)
            if not creds.valid:
                if creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(oauth_client_json, SCOPES)
                    creds = flow.run_console()
                Path(oauth_token_json).parent.mkdir(parents=True, exist_ok=True)
                Path(oauth_token_json).write_text(creds.to_json(), encoding="utf-8")
        else:
            raise RuntimeError("Drive auth not configured. Set GDRIVE_SERVICE_ACCOUNT_JSON or OAuth files.")

        self._svc = build("drive", "v3", credentials=creds, cache_discovery=False)

    def list_children(self, parent_id: str) -> List[DriveItem]:
        q = f"'{parent_id}' in parents and trashed=false"
        out: List[DriveItem] = []
        page_token: Optional[str] = None
        while True:
            res = (
                self._svc.files()
                .list(q=q, fields="nextPageToken,files(id,name,mimeType)", pageSize=1000, pageToken=page_token)
                .execute()
            )
            for f in res.get("files", []):
                out.append(DriveItem(id=f["id"], name=f["name"], mime_type=f["mimeType"]))
            page_token = res.get("nextPageToken")
            if not page_token:
                break
        return out

    def find_child_folder(self, parent_id: str, name: str) -> Optional[DriveItem]:
        for it in self.list_children(parent_id):
            if it.mime_type == "application/vnd.google-apps.folder" and it.name == name:
                return it
        return None

    def find_child_file(self, parent_id: str, name: str) -> Optional[DriveItem]:
        for it in self.list_children(parent_id):
            if it.mime_type != "application/vnd.google-apps.folder" and it.name == name:
                return it
        return None

    def download_text(self, file_id: str) -> str:
        req = self._svc.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return fh.getvalue().decode("utf-8")

    def update_name(self, file_id: str, new_name: str) -> None:
        self._svc.files().update(fileId=file_id, body={"name": new_name}).execute()

    def download_to_path(self, file_id: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        req = self._svc.files().get_media(fileId=file_id)
        fh = io.FileIO(dest, "wb")
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.close()
