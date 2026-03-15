# SOP: When Failed or Stale Jobs Accumulate

## Purpose
Provide a repeatable operator triage path when job failures or stale worker heartbeats start accumulating.

## When to use
- `/v1/workers` shows stale/missing required roles.
- Smoke fails `worker_heartbeat`, `required_runtime_roles`, or `pipeline_readiness`.
- Ops monitoring indicates rising failed/retry job volume tied to worker health.

## Preconditions
- Operator can inspect worker service status/logs.
- Deployment-specific worker service names are confirmed from `deploy/systemd/*.service`.

## Steps
1. Run production smoke to snapshot readiness:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Check worker heartbeat state:
   ```bash
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
   ```
3. Verify core worker services (systemd deployment example):
   ```bash
   systemctl status factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
   ```
4. If importer/bot flow is enabled, check optional services too:
   ```bash
   systemctl status factory-importer.service factory-bot.service
   ```
5. Review recent worker logs using deployment-specific log commands (for systemd, `journalctl -u <service> -n 200 --no-pager`).
6. Re-run smoke after service recovery actions to verify role freshness.

## Expected success result
- Required roles report healthy/fresh heartbeats.
- Smoke returns `OK` for runtime role and pipeline readiness checks.
- New job failures stop accumulating from worker unavailability.

## Escalation / next document if failed
- Follow `../playbooks/worker_heartbeat_missing.md` as primary remediation.
- If API endpoint is unstable, also use `../playbooks/api_not_responding.md`.
- If issue persists after playbook steps, escalate as production incident.
