from __future__ import annotations

import os
import sqlite3
from typing import Optional, Tuple


class YouTubeCredentialResolutionError(RuntimeError):
    """Raised when YouTube credential paths cannot be resolved for a channel."""


def resolve_youtube_channel_credentials(
    channel_slug: str,
    *,
    conn: sqlite3.Connection,
    global_client_secret_path: Optional[str] = None,
    global_token_path: Optional[str] = None,
) -> Tuple[str, str, str]:
    """Resolve YouTube credential paths for a channel.

    Resolution order for token path:
      1) channel yt_token_json_path in DB channels table
      2) YT_TOKEN_JSON from environment

    Client secret path selection:
      - channel yt_client_secret_json_path from DB if present
      - otherwise YT_CLIENT_SECRET_JSON from environment

    Validation policy intentionally checks only for non-empty paths.
    Path existence/readability is left to the uploader/client initialization phase.
    """

    token_path = str(global_token_path if global_token_path is not None else (os.getenv("YT_TOKEN_JSON") or "")).strip()
    client_secret_path = str(
        global_client_secret_path
        if global_client_secret_path is not None
        else (os.getenv("YT_CLIENT_SECRET_JSON") or "")
    ).strip()
    source_label = "global"

    channel = conn.execute(
        """
        SELECT yt_token_json_path, yt_client_secret_json_path
        FROM channels
        WHERE slug = ?
        LIMIT 1
        """,
        (channel_slug,),
    ).fetchone()

    if channel:
        token_override = channel["yt_token_json_path"]
        if token_override:
            token_path = str(token_override).strip()
            source_label = "channel"

        client_secret_override = channel["yt_client_secret_json_path"]
        if client_secret_override:
            client_secret_path = str(client_secret_override).strip()

    if not token_path or not client_secret_path:
        raise YouTubeCredentialResolutionError(
            f"YouTube credentials not configured for channel {channel_slug}"
        )

    return client_secret_path, token_path, source_label
