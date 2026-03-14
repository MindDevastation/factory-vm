# Daily Checks (Operator Start-of-Day SOP)

Preferred production path: one smoke gate + endpoint check + service status review.

## 1) Run smoke gate (required)

```bash
python scripts/doctor.py production-smoke --profile prod
```

- `exit_code=0` (`OK`) means proceed.
- `exit_code=1/2/3` means investigate before processing new workload.

## 2) API and worker heartbeat checks

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/v1/workers
```

## 3) Service state review (systemd deployments)

Use the deployment-configured command/path defined in your service manager. For repository-provided systemd unit names:

```bash
systemctl status factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
```

If importer or bot are enabled:

```bash
systemctl status factory-importer.service factory-bot.service
```

## 4) Optional diagnostics (debug-only)

- JSON smoke output:

```bash
python scripts/doctor.py production-smoke --profile prod --json
```

- Focused smoke checks:

```bash
python scripts/doctor.py production-smoke --profile prod --checks runner_bootstrap,pipeline_readiness
```

Do not treat these debug forms as replacement for the required smoke command in step 1.
