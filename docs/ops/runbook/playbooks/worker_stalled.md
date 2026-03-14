# Playbook: Worker Stalled / Missing Heartbeats

## Trigger

- `/v1/workers` shows missing or stale required roles, or
- smoke fails `required_runtime_roles` / `worker_heartbeat` / `pipeline_readiness`.

## Preferred production path

1. Check core worker service status:

```bash
systemctl status factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
```

2. If importer/bot flow is enabled, also verify:

```bash
systemctl status factory-importer.service factory-bot.service
```

3. Re-run smoke for readiness confirmation:

```bash
python scripts/doctor.py production-smoke --profile prod
```

## Debug-only / internal fallback

For targeted single-role validation outside normal service manager operations:

```bash
python -m services.workers --role track_jobs --once
```

Do not treat one-off worker process execution as normal production steady-state management.
