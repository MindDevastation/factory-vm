from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from google_auth_oauthlib.flow import Flow

from services.common.env import Env

GDRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
YOUTUBE_SCOPE = "https://www.googleapis.com/auth/youtube.upload"
_STATE_TTL_SECONDS = 600


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * ((4 - len(data) % 4) % 4)
    return base64.urlsafe_b64decode(data + padding)


def sign_state(
    *,
    secret: str,
    kind: str,
    channel_slug: str | None = None,
    now_ts: int | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    ts = int(time.time() if now_ts is None else now_ts)
    payload: dict[str, Any] = {
        "kind": kind,
        "ts": ts,
        "nonce": secrets.token_urlsafe(12),
    }
    if channel_slug is not None:
        payload["channel_slug"] = channel_slug
    if extra:
        payload.update(extra)
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
    return f"{_b64url_encode(payload_bytes)}.{_b64url_encode(signature)}"


def verify_state(
    *,
    secret: str,
    expected_kind: str,
    state: str,
    now_ts: int | None = None,
    require_channel_slug: bool = True,
) -> dict[str, Any]:
    if not state or "." not in state:
        raise HTTPException(400, "invalid oauth state")

    payload_part, sig_part = state.split(".", 1)
    try:
        payload_bytes = _b64url_decode(payload_part)
        signature = _b64url_decode(sig_part)
    except Exception as exc:
        raise HTTPException(400, "invalid oauth state") from exc

    expected_sig = hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
    if not hmac.compare_digest(signature, expected_sig):
        raise HTTPException(400, "invalid oauth state signature")

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise HTTPException(400, "invalid oauth state") from exc

    if payload.get("kind") != expected_kind:
        raise HTTPException(400, "invalid oauth state kind")

    ts = int(payload.get("ts", 0))
    now = int(time.time() if now_ts is None else now_ts)
    if ts <= 0 or now - ts > _STATE_TTL_SECONDS:
        raise HTTPException(400, "expired oauth state")

    if require_channel_slug:
        slug = str(payload.get("channel_slug") or "").strip()
        if not slug:
            raise HTTPException(400, "invalid oauth state channel")
        payload["channel_slug"] = slug
    return payload


def oauth_token_path(*, base_dir: str, channel_slug: str) -> Path:
    root = Path(base_dir).expanduser()
    return root / channel_slug / "token.json"


def ensure_token_dir(token_path: Path) -> None:
    token_dir = token_path.parent
    token_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    if not os.access(token_dir, os.W_OK):
        raise HTTPException(500, f"token directory is not writable: {token_dir}")


def build_authorization_url(*, client_secret_path: str, scope: str, redirect_uri: str, state: str) -> str:
    flow = Flow.from_client_secrets_file(client_secret_path, scopes=[scope], redirect_uri=redirect_uri)
    auth_url, _ = flow.authorization_url(access_type="offline", include_granted_scopes="true", state=state, prompt="consent")
    return auth_url


def exchange_code_for_token_json(
    *, client_secret_path: str, scope: str, redirect_uri: str, code: str
) -> str:
    flow = Flow.from_client_secrets_file(client_secret_path, scopes=[scope], redirect_uri=redirect_uri)
    flow.fetch_token(code=code)
    credentials = flow.credentials
    if credentials is None:
        raise HTTPException(500, "oauth token exchange failed")
    return credentials.to_json()


def redirect_uri(env: Env, kind: str) -> str:
    base = env.oauth_redirect_base_url.rstrip("/")
    return f"{base}/v1/oauth/{kind}/callback"


def validate_oauth_config(env: Env, *, kind: str) -> tuple[str, str, str]:
    if not env.oauth_redirect_base_url:
        raise HTTPException(500, "OAUTH_REDIRECT_BASE_URL is not configured")
    if not env.oauth_state_secret:
        raise HTTPException(500, "OAUTH_STATE_SECRET is not configured")

    if kind == "gdrive":
        if not env.gdrive_client_secret_json:
            raise HTTPException(500, "GDRIVE_CLIENT_SECRET_JSON is not configured")
        if not env.gdrive_tokens_dir:
            raise HTTPException(500, "GDRIVE_TOKENS_DIR is not configured")
        return env.gdrive_client_secret_json, env.gdrive_tokens_dir, GDRIVE_SCOPE

    if not env.yt_client_secret_json:
        raise HTTPException(500, "YT_CLIENT_SECRET_JSON is not configured")
    if not env.yt_tokens_dir:
        raise HTTPException(500, "YT_TOKENS_DIR is not configured")
    return env.yt_client_secret_json, env.yt_tokens_dir, YOUTUBE_SCOPE
