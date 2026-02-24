# Local debug mode (run on your PC)

Goal: run the whole pipeline locally BEFORE deploying to VPS.

## 1) Create local origin folder
Create a folder (default: ./local_origin) with the same structure as Drive origin:

local_origin/
  channels/
    darkwood-reverie/
      incoming/
        release_001/
          meta.json
          audio/
            my_track.wav
          images/
            cover.png

## 2) Create env file for local profile
Copy and edit:
- deploy/env.example -> deploy/env.local

Minimal fields for local:
ORIGIN_BACKEND=local
ORIGIN_LOCAL_ROOT=local_origin
UPLOAD_BACKEND=mock
TELEGRAM_ENABLED=0

(You can enable Telegram if you want approvals in chat.)

## 3) Run
In terminal:
  export FACTORY_PROFILE=local
  python scripts/init_db.py
  python scripts/seed_configs.py
  python -m services.factory_api

In separate terminals:
  export FACTORY_PROFILE=local
  python -m services.workers --role importer
  python -m services.workers --role orchestrator
  python -m services.workers --role qa
  python -m services.workers --role uploader
  python -m services.workers --role cleanup

Dashboard: http://127.0.0.1:8080/

## 4) What happens in local mode
- Importer reads releases from local_origin (filesystem)
- Orchestrator renders using render_worker/main.py
- QA runs the same checks
- Uploader uses mock backend (no YouTube upload). The "url" becomes file://... for debugging.
- You approve/reject via Dashboard API.


## Быстрые команды

- `make venv`
- `make init-local`
- `make gen-release`
- `make local-up`
