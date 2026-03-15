# Playbook: Worker Heartbeat Missing

## Symptoms
- `/v1/workers` shows missing or stale required roles.
- Smoke fails `worker_heartbeat`, `required_runtime_roles`, or `pipeline_readiness`.
- Jobs remain queued/stale while API is reachable.

## Likely causes
- Worker service(s) stopped, stuck, or crash-looping.
- Optional flow services expected by current profile are not running.
- Worker runtime error after deploy/config change.

## Checks to perform
1. Check worker heartbeat endpoint:
   ```bash
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
   ```
2. Check core worker services (systemd deployments):
   ```bash
   systemctl status factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
   ```
3. If enabled, check importer/bot services:
   ```bash
   systemctl status factory-importer.service factory-bot.service
   ```
4. Inspect recent logs for unhealthy workers:
   ```bash
   journalctl -u factory-orchestrator.service -u factory-qa.service -u factory-uploader.service -u factory-cleanup.service -n 200 --no-pager
   ```

## Actions to take
- **Preferred production path:** Recover worker services with deployment service manager commands, then run smoke.
- If API is also failing, handle `api_not_responding.md` first.
- **Alternative / debug-only path:** One-off worker validation (`python -m services.workers --role track_jobs --once`) is allowed only for diagnosis, not steady-state production operation.

## Verification after fix
- `/v1/workers` reports required roles as active/fresh.
- `python scripts/doctor.py production-smoke --profile prod` passes readiness checks.

## Escalation / fallback
- If heartbeats remain stale after service recovery, escalate with service status + journal output.
- If job failures continue with healthy heartbeats, continue with `repeated_job_failures.md`.
