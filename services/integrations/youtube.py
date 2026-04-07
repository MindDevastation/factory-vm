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

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]


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

    def set_video_privacy(self, *, video_id: str, privacy_status: str) -> None:
        normalized = str(privacy_status or "").strip().lower()
        if normalized not in {"private", "public", "unlisted"}:
            raise ValueError("privacy_status must be one of: private, public, unlisted")
        body = {"id": str(video_id), "status": {"privacyStatus": normalized}}
        self._yt.videos().update(part="status", body=body).execute()

    def list_playlists(self) -> list[dict[str, str]]:
        items: list[dict[str, str]] = []
        next_page_token: str | None = None
        while True:
            response = (
                self._yt.playlists()
                .list(part="snippet", mine=True, maxResults=50, pageToken=next_page_token)
                .execute()
            )
            raw_items = response.get("items") or []
            for item in raw_items:
                playlist_id = str(item.get("id") or "").strip()
                snippet = item.get("snippet") or {}
                playlist_title = str(snippet.get("title") or "").strip()
                if playlist_id and playlist_title:
                    items.append({"playlist_id": playlist_id, "playlist_title": playlist_title})
            token = response.get("nextPageToken")
            next_page_token = str(token) if token else None
            if not next_page_token:
                break
        return items

    def resolve_or_create_playlist(self, *, title: str) -> tuple[str, bool]:
        wanted = str(title or "").strip()
        if not wanted:
            raise ValueError("playlist title is required")
        existing = [item for item in self.list_playlists() if str(item.get("playlist_title") or "").strip() == wanted]
        if len(existing) > 1:
            raise RuntimeError(f"ambiguous playlist title match: {wanted}")
        if len(existing) == 1:
            return str(existing[0]["playlist_id"]), False
        response = self._yt.playlists().insert(
            part="snippet,status",
            body={"snippet": {"title": wanted}, "status": {"privacyStatus": "public"}},
        ).execute()
        playlist_id = str(response.get("id") or "").strip()
        if not playlist_id:
            raise RuntimeError("failed to create playlist")
        return playlist_id, True

    def playlist_contains_video(self, *, playlist_id: str, video_id: str) -> bool:
        next_page_token: str | None = None
        target = str(video_id)
        while True:
            response = (
                self._yt.playlistItems()
                .list(part="snippet", playlistId=str(playlist_id), maxResults=50, pageToken=next_page_token)
                .execute()
            )
            for item in response.get("items") or []:
                snippet = item.get("snippet") or {}
                resource = snippet.get("resourceId") or {}
                if str(resource.get("kind") or "") == "youtube#video" and str(resource.get("videoId") or "") == target:
                    return True
            token = response.get("nextPageToken")
            next_page_token = str(token) if token else None
            if not next_page_token:
                break
        return False

    def add_video_to_playlist(self, *, playlist_id: str, video_id: str) -> None:
        if self.playlist_contains_video(playlist_id=playlist_id, video_id=video_id):
            return
        self._yt.playlistItems().insert(
            part="snippet",
            body={
                "snippet": {
                    "playlistId": str(playlist_id),
                    "resourceId": {"kind": "youtube#video", "videoId": str(video_id)},
                }
            },
        ).execute()
