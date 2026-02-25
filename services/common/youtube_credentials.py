from __future__ import annotations

import os
from typing import Tuple

from services.common.config import load_channels


class YouTubeCredentialResolutionError(RuntimeError):
    """Raised when YouTube credential paths cannot be resolved for a channel."""


def resolve_youtube_channel_credentials(channel_slug: str) -> Tuple[str, str, str]:
    """Resolve YouTube credential paths for a channel.

    Resolution order for token path:
      1) channel yt_token_json_path in configs/channels.yaml
      2) YT_TOKEN_JSON from environment

    Client secret path selection:
      - channel yt_client_secret_json_path if present
      - otherwise YT_CLIENT_SECRET_JSON from environment

    Validation policy intentionally checks only for non-empty paths.
    Path existence/readability is left to the uploader/client initialization phase.
    """

    token_path = (os.getenv("YT_TOKEN_JSON") or "").strip()
    client_secret_path = (os.getenv("YT_CLIENT_SECRET_JSON") or "").strip()
    source_label = "global"

    channels = load_channels("configs/channels.yaml")
    for channel in channels:
        if channel.slug != channel_slug:
            continue

        if channel.yt_token_json_path:
            token_path = str(channel.yt_token_json_path).strip()
            source_label = "channel"

        if channel.yt_client_secret_json_path:
            client_secret_path = str(channel.yt_client_secret_json_path).strip()

        break

    if not token_path:
        raise YouTubeCredentialResolutionError(
            f"YouTube credentials not configured for channel {channel_slug}"
        )

    return client_secret_path, token_path, source_label
