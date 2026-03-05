from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

PREVIEW_TTL_SECONDS = 10 * 60
"""Preview TTL in seconds (10 minutes)."""

MAX_PREVIEWS = 20
"""Maximum number of previews kept in memory."""


class PreviewStoreError(Exception):
    """Base preview store error."""


class PreviewNotFoundError(PreviewStoreError):
    """Raised when preview id is unknown or already consumed."""


class PreviewExpiredError(PreviewStoreError):
    """Raised when preview has expired."""


class PreviewUsernameMismatchError(PreviewStoreError):
    """Raised when preview does not belong to the username."""


@dataclass
class _Entry:
    username: str
    preview: Any
    created_at: float
    used: bool = False


class PreviewStore:
    def __init__(
        self,
        *,
        ttl_seconds: int = PREVIEW_TTL_SECONDS,
        max_previews: int = MAX_PREVIEWS,
        now_fn: Callable[[], float] | None = None,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._max_previews = max_previews
        self._now_fn = now_fn or time.time
        self._entries: dict[str, _Entry] = {}

    def put(self, username: str, preview: Any) -> str:
        self._prune_expired()
        self._prune_to_capacity()

        preview_id = uuid.uuid4().hex
        self._entries[preview_id] = _Entry(
            username=username,
            preview=preview,
            created_at=self._now_fn(),
        )
        return preview_id

    def get(self, username: str, preview_id: str) -> Any:
        entry = self._entries.get(preview_id)
        if entry is None or entry.used:
            raise PreviewNotFoundError("preview not found")
        if self._is_expired(entry):
            self._entries.pop(preview_id, None)
            raise PreviewExpiredError("preview expired")
        if entry.username != username:
            raise PreviewUsernameMismatchError("preview does not belong to username")
        return entry.preview

    def mark_used(self, preview_id: str) -> None:
        entry = self._entries.get(preview_id)
        if entry is None:
            raise PreviewNotFoundError("preview not found")
        if self._is_expired(entry):
            self._entries.pop(preview_id, None)
            raise PreviewExpiredError("preview expired")
        entry.used = True

    def _is_expired(self, entry: _Entry) -> bool:
        return self._now_fn() - entry.created_at > self._ttl_seconds

    def _prune_expired(self) -> None:
        for preview_id in list(self._entries.keys()):
            if self._is_expired(self._entries[preview_id]):
                self._entries.pop(preview_id, None)

    def _prune_to_capacity(self) -> None:
        while len(self._entries) >= self._max_previews:
            oldest_preview_id = min(self._entries.items(), key=lambda item: item[1].created_at)[0]
            self._entries.pop(oldest_preview_id, None)
