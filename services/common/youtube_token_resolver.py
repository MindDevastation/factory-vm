from __future__ import annotations

import os
from pathlib import Path


class YouTubeTokenResolutionError(RuntimeError):
    """Raised when a per-channel YouTube token path cannot be resolved."""


def resolve_channel_token_path(*, channel_slug: str, tokens_dir: str | None = None) -> str:
    slug = str(channel_slug or "").strip()
    if not slug:
        raise YouTubeTokenResolutionError("channel_slug is required for YouTube uploads")

    base_dir = str(tokens_dir if tokens_dir is not None else os.getenv("YT_TOKENS_DIR") or "").strip()
    if not base_dir:
        raise YouTubeTokenResolutionError("YT_TOKENS_DIR is required for YouTube uploads")

    token_path = Path(base_dir).joinpath(slug, "token.json")
    if not token_path.is_file() or not os.access(token_path, os.R_OK):
        raise YouTubeTokenResolutionError(f"YouTube token missing for channel {slug} at {token_path}")

    return str(token_path)
