from google_auth_oauthlib.flow import InstalledAppFlow
from pathlib import Path

SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]

flow = InstalledAppFlow.from_client_secrets_file("../secure/client_secret.json", SCOPES)
creds = flow.run_local_server(port=0)  # откроет браузер

Path("yt_token.json").write_text(creds.to_json(), encoding="utf-8")
print("Saved yt_token.json")
