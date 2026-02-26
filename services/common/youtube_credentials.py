from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple


class YouTubeCredentialResolutionError(RuntimeError):
    """Raised when YouTube credential paths cannot be resolved for a channel."""


def resolve_youtube_channel_credentials(
    channel_slug: str,
    *,
    token_base_dir: Optional[str] = None,
    client_secret_base_dir: Optional[str] = None,
    global_client_secret_path: Optional[str] = None,
    global_token_path: Optional[str] = None,
) -> Tuple[str, str, str]:
    """Resolve YouTube credential paths for a channel.

    Resolution order:
      1) "convention":
         - token: <YT_TOKEN_BASE_DIR>/<channel_slug>/token.json
         - client secret: YT_CLIENT_SECRET_JSON OR <YT_CLIENT_SECRET_BASE_DIR>/client_secret.json
      2) "global_env" fallback:
         - token: YT_TOKEN_JSON
         - client secret: YT_CLIENT_SECRET_JSON

    Validation checks only non-empty paths and does not access filesystem.
    """

    slug = str(channel_slug or "").strip()
    resolved_token_base_dir = str(token_base_dir if token_base_dir is not None else (os.getenv("YT_TOKEN_BASE_DIR") or "")).strip()
    resolved_client_secret_base_dir = str(
        client_secret_base_dir if client_secret_base_dir is not None else (os.getenv("YT_CLIENT_SECRET_BASE_DIR") or "")
    ).strip()
    resolved_global_client_secret = str(
        global_client_secret_path if global_client_secret_path is not None else (os.getenv("YT_CLIENT_SECRET_JSON") or "")
    ).strip()
    resolved_global_token = str(global_token_path if global_token_path is not None else (os.getenv("YT_TOKEN_JSON") or "")).strip()

    if resolved_token_base_dir and slug:
        token_path = str(Path(resolved_token_base_dir) / slug / "token.json")
        client_secret_path = resolved_global_client_secret
        if not client_secret_path and resolved_client_secret_base_dir:
            client_secret_path = str(Path(resolved_client_secret_base_dir) / "client_secret.json")
        if token_path and client_secret_path:
            return client_secret_path, token_path, "convention"

    if resolved_global_token and resolved_global_client_secret:
        return resolved_global_client_secret, resolved_global_token, "global_env"

    raise YouTubeCredentialResolutionError(
        "YouTube credentials are misconfigured for channel "
        f"{slug or '<empty>'}. Configure YT_TOKEN_BASE_DIR + "
        "(YT_CLIENT_SECRET_JSON or YT_CLIENT_SECRET_BASE_DIR) or set global "
        "YT_TOKEN_JSON + YT_CLIENT_SECRET_JSON."
    )
