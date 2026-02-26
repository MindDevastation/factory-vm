# Launch guide (MVP v0) — step-by-step

## 0) Prepare a VM (Ubuntu 22.04+ recommended)
1) Ensure you have at least:
   - 2 CPU cores (more is better for CPU render)
   - 8 GB RAM (minimum)
   - 80+ GB disk (depends on your long video sizes)
2) Open port 8080 to your IP OR use a tunnel (Cloudflare/Tailscale).

## 1) Install system dependencies
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip ffmpeg
```

## 2) Put project on server
- Upload this zip, unzip to `/opt/factory-vm` (recommended).
```bash
sudo mkdir -p /opt/factory-vm
sudo chown -R $USER:$USER /opt/factory-vm
cd /opt/factory-vm
unzip factory-vm_mvp_v0_ready.zip
```

## 3) Create virtualenv and install deps
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

## 4) Configure Telegram bot
1) Open Telegram, find @BotFather, create a bot, get token.
2) Get your chat_id:
   - easiest: create a private group, add the bot, send any message.
   - then use @RawDataBot in that group to see chat_id (manual).
3) Save:
   - TG_BOT_TOKEN
   - TG_ADMIN_CHAT_ID

## 5) Configure Google Drive (origin)
Recommended for MVP: Service Account.
1) Create a Google Cloud project.
2) Enable "Google Drive API".
3) Create a Service Account + JSON key.
4) Create a Drive folder structure:
   /FactoryOrigin/channels/<channel_slug>/incoming/<release_folder>/
     meta.json
     audio/*.wav
     images/cover.png
5) Share `/FactoryOrigin` folder with the service account email (Viewer is enough).
6) Put the JSON key on server at `/secure/gdrive_service_account.json`.

## 6) Configure YouTube OAuth (upload)
1) In Google Cloud project enable "YouTube Data API v3".
2) Configure OAuth consent screen.
3) Create OAuth Client "Desktop App", download JSON.
4) Put it on server: `/secure/youtube/client_secret.json`.
5) Create token on server:
```bash
export YT_CLIENT_SECRET_JSON=/secure/youtube/client_secret.json
export YT_TOKEN_JSON=/secure/youtube/global/token.json
python scripts/youtube_auth.py
```

## 7) Create deploy/env
```bash
cp deploy/env.example deploy/env
nano deploy/env
```
Fill all required fields:
- FACTORY_DB_PATH, FACTORY_STORAGE_ROOT
- GDRIVE_ROOT_ID, GDRIVE_SERVICE_ACCOUNT_JSON
- YT_CLIENT_SECRET_JSON, YT_TOKEN_JSON (global fallback)
- TG_BOT_TOKEN, TG_ADMIN_CHAT_ID
- BASIC AUTH user/pass for dashboard

Channels config source of truth:
- `configs/channels.yaml` is seed-only for `python scripts/seed_configs.py`.
- Runtime reads channels from DB (`channels` table); after deploy do not edit YAML expecting live changes.

YouTube credentials:
- Runtime uses env vars `YT_CLIENT_SECRET_JSON` and `YT_TOKEN_JSON`.

## 8) Init DB + seed configs
```bash
source .venv/bin/activate
python scripts/init_db.py
python scripts/seed_configs.py
```

## 9) Run services (MVP: in 3 terminals)
Terminal A (API + dashboard):
```bash
source .venv/bin/activate
python -m services.factory_api
```

Terminal B (workers):
```bash
source .venv/bin/activate
python -m services.workers --role importer
```
Open additional terminals for:
- orchestrator
- qa
- uploader
- cleanup

Terminal C (Telegram bot):
```bash
source .venv/bin/activate
python -m services.bot
```

## 10) First test
1) Create a release folder in Drive with `meta.json` and assets.
2) Wait ~5–30 seconds for importer to create a job.
3) Open dashboard: http://<VM_IP>:8080/
4) Orchestrator renders → QA → upload (private) → bot sends preview + link.
5) Approve in bot → publish in Studio → Mark Published in bot.
6) After 48h MP4 is deleted.
