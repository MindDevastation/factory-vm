# SOP: Service Control and Logs

## Preferred production path

Use the deployment service manager definitions (systemd files in `deploy/systemd/` for the provided deployment pattern).

## Core service names in repository-provided systemd units

- `factory-api.service`
- `factory-orchestrator.service`
- `factory-qa.service`
- `factory-uploader.service`
- `factory-cleanup.service`
- optional: `factory-importer.service`, `factory-bot.service`

## Standard checks

```bash
systemctl status factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
journalctl -u factory-api.service -n 200 --no-pager
```

For optional services:

```bash
systemctl status factory-importer.service factory-bot.service
```

## Known implementation note

A dedicated `factory-track-jobs.service` file is not present in `deploy/systemd/` in this repository state; if track jobs are required, use the runtime command from existing docs:

```bash
python -m services.workers --role track_jobs
```
