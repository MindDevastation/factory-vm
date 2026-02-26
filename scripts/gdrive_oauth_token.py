from google_auth_oauthlib.flow import InstalledAppFlow
import json

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

flow = InstalledAppFlow.from_client_secrets_file("../secure/client_secret.json", SCOPES)
creds = flow.run_local_server(port=0)

with open("token.json", "w", encoding="utf-8") as f:
    f.write(creds.to_json())

print("Saved token.json")
