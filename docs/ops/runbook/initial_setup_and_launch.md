# Initial Setup and Launch (Production VPS)

Preferred production path: deploy with provided systemd unit templates in `deploy/systemd/` and environment file `deploy/env` (deployment-configured location per unit files).

## 1) Host + runtime prerequisites

1. Install system dependencies:

```bash
# Fallback below ignores third-party source fragments (for example apt.llvm.org outages).
sudo apt-get update || sudo apt-get -o Dir::Etc::sourcelist="sources.list" -o Dir::Etc::sourceparts="-" update
sudo apt-get install -y python3 python3-venv python3-pip ffmpeg || sudo apt-get -o Dir::Etc::sourcelist="sources.list" -o Dir::Etc::sourceparts="-" install -y python3 python3-venv python3-pip ffmpeg
```

2. Prepare project directory and virtualenv (example root used by shipped units is `/opt/factory-vm`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

## 2) Required app initialization

From repository root:

```bash
python scripts/init_db.py
python scripts/seed_configs.py
```

## 3) Configure environment

- Create and maintain deployment env file using `deploy/env.example` (or `deploy/env.prod.example`) as source.
- Keep production env at the deployment-configured path/command defined in `deploy/systemd/*.service` (`EnvironmentFile=...`).

## 4) Install/enable production services

Install unit files using deployment-configured path/command defined in `deploy/systemd/*.service` and reload daemon:

```bash
sudo systemctl daemon-reload
```

Enable/start required units (names verified in repo):

```bash
sudo systemctl enable --now factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
```

Optional units depending on enabled flows:

```bash
sudo systemctl enable --now factory-importer.service
sudo systemctl enable --now factory-bot.service
```

## 5) First launch verification

1. Run production smoke gate:

```bash
python scripts/doctor.py production-smoke --profile prod
```

2. Verify worker heartbeat endpoint from API host (Basic Auth required via `FACTORY_BASIC_AUTH_USER` / `FACTORY_BASIC_AUTH_PASS` from deployment env):

```bash
curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
```

3. If smoke is not `OK`, follow incident playbooks in `./playbooks/` before starting production jobs.

## Source anchors

- Runtime/bootstrap commands: `README.md`, `scripts/init_db.py`, `scripts/seed_configs.py`
- Production service path and unit names: `deploy/systemd/*.service`
- Smoke verification command: `scripts/doctor.py`, `docs/ops/production_smoke.md`
