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

Channels config source of truth:
- `configs/channels.yaml` is **seed-only**. Use it only with `python scripts/seed_configs.py` to seed/refresh DB rows.
- After deploy, runtime components (API/workers/importer/uploader) read channel data from the DB (`channels` table), not from YAML.
- Do not edit `configs/channels.yaml` expecting live runtime behavior changes; reseed DB instead.

OAuth credentials and token storage:
- Configure redirect/signing settings:
  - `OAUTH_REDIRECT_BASE_URL=http://localhost:8080` (or your HTTPS domain)
  - `OAUTH_STATE_SECRET=<random-long-secret>`
- YouTube:
  - `YT_CLIENT_SECRET_JSON=/secure/youtube/client_secret.json`
  - `YT_TOKENS_DIR=/secure/youtube/channels`
  - Per-channel token path: `${YT_TOKENS_DIR}/${channel_slug}/token.json`
- Google Drive:
  - `GDRIVE_CLIENT_SECRET_JSON=/secure/gdrive/client_secret.json`
  - `GDRIVE_TOKENS_DIR=/secure/gdrive/channels`
  - Per-channel token path: `${GDRIVE_TOKENS_DIR}/${channel_slug}/token.json`
- Use the dashboard OAuth Tokens section to start consent for each channel (Generate/Regenerate actions). The UI shows only token presence + last update time and never exposes token contents.

Publish flow:
- Bot sends YouTube private link + 60s preview.
- Approve in bot.
- Publish manually in YouTube Studio.
- Press "Mark Published" in bot.

Scaling runbook:
- For onboarding a new channel and enabling YouTube uploads, see `docs/scaling_channels.md`.
