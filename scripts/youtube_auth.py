from __future__ import annotations

import argparse
import os
from pathlib import Path

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


def _build_flow(client_secret: str):
    from google_auth_oauthlib.flow import InstalledAppFlow

    return InstalledAppFlow.from_client_secrets_file(client_secret, SCOPES)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and store a YouTube OAuth token for one channel.")
    parser.add_argument("--channel-slug", required=True, help="Channel slug used under YT_TOKENS_DIR")
    return parser.parse_args(argv)


def _token_path(tokens_dir: str, channel_slug: str) -> Path:
    return Path(tokens_dir) / channel_slug / "token.json"


def main(argv: list[str] | None = None, flow_builder=_build_flow) -> int:
    args = _parse_args(argv)

    client_secret = os.environ.get("YT_CLIENT_SECRET_JSON", "").strip()
    tokens_dir = os.environ.get("YT_TOKENS_DIR", "").strip()
    legacy_token_json = os.environ.get("YT_TOKEN_JSON", "").strip()

    if not client_secret:
        print("YT_CLIENT_SECRET_JSON is required.")
        return 1
    if not tokens_dir:
        print("YT_TOKENS_DIR is required and must not be empty.")
        return 1
    if legacy_token_json:
        print("Warning: YT_TOKEN_JSON is deprecated and ignored; using YT_TOKENS_DIR/channel_slug/token.json.")

    token_path = _token_path(tokens_dir, args.channel_slug)

    flow = flow_builder(client_secret)
    creds = flow.run_console()

    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json(), encoding="utf-8")
    print(f"Wrote token: {token_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
