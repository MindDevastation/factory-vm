from __future__ import annotations

import os
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


def main() -> None:
    client_secret = os.environ.get("YT_CLIENT_SECRET_JSON", "")
    token_json = os.environ.get("YT_TOKEN_JSON", "")
    if not client_secret or not token_json:
        print("Set YT_CLIENT_SECRET_JSON and YT_TOKEN_JSON env vars first.")
        return

    flow = InstalledAppFlow.from_client_secrets_file(client_secret, SCOPES)
    creds = flow.run_console()
    Path(token_json).parent.mkdir(parents=True, exist_ok=True)
    Path(token_json).write_text(creds.to_json(), encoding="utf-8")
    print(f"Wrote token: {token_json}")


if __name__ == "__main__":
    main()
