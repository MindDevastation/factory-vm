# Playbook: API Not Responding

## Symptoms
- `curl -fsS http://127.0.0.1:8080/health` fails or times out.
- `python scripts/doctor.py production-smoke --profile prod` fails `api_health`/`pipeline_readiness`.
- Dashboard (`/`) is unreachable.

## Likely causes
- `factory-api.service` is down or restarting.
- API dependency issue after deploy/reboot (env, DB path, missing runtime dependency).
- Local bind/port conflict on `127.0.0.1:8080`.

## Checks to perform
1. Confirm API health endpoint failure:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   ```
2. Confirm service status (systemd deployments):
   ```bash
   systemctl status factory-api.service
   ```
3. Inspect recent API logs:
   ```bash
   journalctl -u factory-api.service -n 200 --no-pager
   ```
4. Run smoke to capture related check failures:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```

## Actions to take
- **Preferred production path:** Restart API through deployment service manager, then re-check health and smoke.
- If API is healthy but smoke still fails on workers, continue with `worker_heartbeat_missing.md`.
- **Alternative / debug-only path:** Run API directly (`python -m services.factory_api`) only for short-lived debugging; return to service-managed runtime after confirmation.

## Verification after fix
- `curl -fsS http://127.0.0.1:8080/health` succeeds.
- `curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers` succeeds.
- `python scripts/doctor.py production-smoke --profile prod` returns `overall_status=OK`.

## Escalation / fallback
- If API repeatedly crashes after restart, freeze rollout and escalate as production incident with `journalctl` excerpts.
- If DB/access checks fail during smoke, route to `backup_restore_verification_failure.md`.
