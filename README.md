# Factory VM — MVP v0 (ready)

## What this is
A runnable MVP scaffold that:
- imports releases from Google Drive (origin)
- renders on VM using your existing `render_worker/main.py`
- runs QA (warnings block)
- uploads to YouTube as PRIVATE
- sends Telegram approval with 60s preview + buttons
- keeps MP4 until you confirm Publish, then deletes it after 48h

## Quickstart (high level)
1) Install system deps: ffmpeg + python3.11
2) Create venv + install requirements
3) Put secrets into `deploy/env` (copy from env.example)
4) Init DB: `python scripts/init_db.py`
5) Seed configs: `python scripts/seed_configs.py`
6) Run API: `python -m services.factory_api`
7) Run workers:
   - `python -m services.workers --role importer`
   - `python -m services.workers --role orchestrator`
   - `python -m services.workers --role qa`
   - `python -m services.workers --role uploader`
   - `python -m services.workers --role cleanup`
8) Run bot: `python -m services.bot`
9) Drop a release folder on Drive (meta.json + audio + cover). Importer will pick it up.

Dashboard: http://<VM_IP>:8080/

Worker heartbeat: http://<VM_IP>:8080/v1/workers

VPS API-only mode (without importer):
- `python scripts/run_stack.py --profile prod --with-bot 1 --no-importer`
- You can also disable importer via env override: `IMPORTER_ENABLED=0 python scripts/run_stack.py --profile prod --with-bot 1`

Tests:
- Unit tests: `PYTHONPATH=. python -m unittest discover -s tests -v`
- Smoke QA test: `PYTHONPATH=. FACTORY_BASIC_AUTH_PASS=... python scripts/selftest_smoke.py`
- Claim stress test: `PYTHONPATH=. python scripts/stress_claim_job.py`

YouTube credential config (paths only):
- Global env `YT_CLIENT_SECRET_JSON` + `YT_TOKEN_JSON` remain the default fallback for all channels.
- `configs/channels.yaml` is seed-only: run `python scripts/seed_configs.py` to sync per-channel overrides into DB (`channels.yt_*` columns).
- Recommended VPS layout:
  - `/secure/youtube/client_secret.json`
  - `/secure/youtube/global/token.json`
  - `/secure/youtube/channels/<channel_slug>/token.json`
- Never store OAuth JSON content in repo or DB; store only filesystem paths in YAML seed/env, persisted as paths in DB.

Manual verification: multi-channel YouTube upload (local)
1) Generate two different OAuth token files (one per YouTube account/channel):
   ```bash
   mkdir -p /secure/youtube/channels/channel-a /secure/youtube/channels/channel-b

   export YT_CLIENT_SECRET_JSON=/secure/youtube/client_secret.json

   export YT_TOKEN_JSON=/secure/youtube/channels/channel-a/token.json
   python scripts/youtube_auth.py  # sign in with Channel A Google account

   export YT_TOKEN_JSON=/secure/youtube/channels/channel-b/token.json
   python scripts/youtube_auth.py  # sign in with Channel B Google account
   ```
2) Put token paths into `configs/channels.yaml` and seed DB:
   ```yaml
   channels:
     - slug: "channel-a"
       yt_token_json_path: "/secure/youtube/channels/channel-a/token.json"
       yt_client_secret_json_path: "/secure/youtube/client_secret.json"

     - slug: "channel-b"
       yt_token_json_path: "/secure/youtube/channels/channel-b/token.json"
       yt_client_secret_json_path: "/secure/youtube/client_secret.json"
   ```
3) Seed channel config into DB and run with real uploader backend:
   ```bash
   python scripts/seed_configs.py
   export UPLOAD_BACKEND=youtube
   python -m services.workers --role uploader
   ```
4) Confirm in logs that channel-level credentials are used for each job:
   - Look for `resolved youtube credentials` log lines.
   - Confirm `channel_slug` matches expected channel and `source_label` is `channel`.
   - Run one upload job for `channel-a` and one for `channel-b`; each should emit its own `channel_slug` resolution log before upload.
5) Safety for manual tests:
   - Keep test uploads as `private` (default uploader behavior).
   - If needed for reviewer access, set videos to `unlisted` manually in YouTube Studio after upload.

Publish flow:
- Bot sends YouTube private link + 60s preview.
- Approve in bot.
- Publish manually in YouTube Studio.
- Press "Mark Published" in bot.
