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

YouTube credentials:
- Configure uploader credentials via env (`YT_CLIENT_SECRET_JSON`, `YT_TOKEN_JSON`).

Publish flow:
- Bot sends YouTube private link + 60s preview.
- Approve in bot.
- Publish manually in YouTube Studio.
- Press "Mark Published" in bot.
