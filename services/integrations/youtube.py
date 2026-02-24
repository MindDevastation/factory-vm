from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

# YouTube deps are optional (only required when UPLOAD_BACKEND is not 'mock').
_GOOGLE_IMPORT_ERROR: Exception | None = None
try:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
except Exception as e:  # ImportError in most cases
    _GOOGLE_IMPORT_ERROR = e
    build = None  # type: ignore[assignment]
    MediaFileUpload = None  # type: ignore[assignment]
    InstalledAppFlow = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]
    Credentials = None  # type: ignore[assignment]

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


@dataclass(frozen=True)
class UploadResult:
    video_id: str


class YouTubeClient:
    def __init__(self, *, client_secret_json: str, token_json: str):
        if _GOOGLE_IMPORT_ERROR is not None:
            raise RuntimeError(
                'YouTube dependencies are missing. Install: '
                'pip install google-api-python-client google-auth google-auth-oauthlib'
            )

        creds = Credentials.from_authorized_user_file(token_json, SCOPES)

        # Prefer refresh-token flow for unattended operation.
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(client_secret_json, SCOPES)
                creds = flow.run_console()

            Path(token_json).parent.mkdir(parents=True, exist_ok=True)
            Path(token_json).write_text(creds.to_json(), encoding="utf-8")

        self._yt = build("youtube", "v3", credentials=creds, cache_discovery=False)

    def upload_private(
        self,
        *,
        video_path: Path,
        title: str,
        description: str,
        tags: List[str],
    ) -> UploadResult:
        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": [t.lstrip("#") for t in tags if t],
                "categoryId": "10",
            },
            "status": {"privacyStatus": "private"},
        }
        media = MediaFileUpload(str(video_path), chunksize=8 * 1024 * 1024, resumable=True)
        req = self._yt.videos().insert(part="snippet,status", body=body, media_body=media)

        resp = None
        while resp is None:
            _, resp = req.next_chunk()

        return UploadResult(video_id=resp["id"])

    def set_thumbnail(self, *, video_id: str, image_path: Path) -> None:
        media = MediaFileUpload(str(image_path))
        self._yt.thumbnails().set(videoId=video_id, media_body=media).execute()
