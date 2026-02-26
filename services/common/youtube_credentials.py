from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple


class YouTubeCredentialResolutionError(RuntimeError):
    """Raised when YouTube credential paths cannot be resolved for a channel."""


def _norm(value: Optional[str]) -> str:
    return str(value or "").strip()


def _join_if_set(base_dir: str, *parts: str) -> str:
    base = _norm(base_dir)
    if not base:
        return ""
    return str(Path(base).joinpath(*parts))


def resolve_youtube_channel_credentials(
    channel_slug: str,
    *,
    global_client_secret_path: Optional[str] = None,
    global_token_path: Optional[str] = None,
    token_base_dir: Optional[str] = None,
    client_secret_base_dir: Optional[str] = None,
) -> Tuple[str, str, str]:
    """Resolve YouTube credential paths for a channel.

    Resolution order:
      1) Convention paths when `channel_slug` and `YT_TOKEN_BASE_DIR` are available:
         token: <YT_TOKEN_BASE_DIR>/<channel_slug>/token.json
         client_secret: YT_CLIENT_SECRET_JSON (global), otherwise
                        <YT_CLIENT_SECRET_BASE_DIR>/client_secret.json
      2) Global env fallback:
         token: YT_TOKEN_JSON
         client_secret: YT_CLIENT_SECRET_JSON

    Validation policy checks only for non-empty paths.
    """

    slug = _norm(channel_slug)
    resolved_token_base_dir = _norm(token_base_dir if token_base_dir is not None else os.getenv("YT_TOKEN_BASE_DIR"))
    resolved_client_secret_base_dir = _norm(
        client_secret_base_dir if client_secret_base_dir is not None else os.getenv("YT_CLIENT_SECRET_BASE_DIR")
    )

    client_secret_from_env = _norm(
        global_client_secret_path if global_client_secret_path is not None else os.getenv("YT_CLIENT_SECRET_JSON")
    )
    token_from_env = _norm(global_token_path if global_token_path is not None else os.getenv("YT_TOKEN_JSON"))

    if slug and resolved_token_base_dir:
        token_path = _join_if_set(resolved_token_base_dir, slug, "token.json")
        client_secret_path = client_secret_from_env or _join_if_set(
            resolved_client_secret_base_dir,
            "client_secret.json",
        )
        if token_path and client_secret_path:
            return client_secret_path, token_path, "convention"

    if client_secret_from_env and token_from_env:
        return client_secret_from_env, token_from_env, "global_env"

    raise YouTubeCredentialResolutionError(
        "YouTube credentials not configured. Set per-channel convention "
        "(YT_TOKEN_BASE_DIR with job.channel_slug and YT_CLIENT_SECRET_JSON or YT_CLIENT_SECRET_BASE_DIR) "
        "or global fallback (YT_TOKEN_JSON + YT_CLIENT_SECRET_JSON)."
    )
