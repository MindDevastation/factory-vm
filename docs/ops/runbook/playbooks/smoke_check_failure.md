# Playbook: Smoke Check Failure

## Symptoms
- `python scripts/doctor.py production-smoke --profile prod` exits non-zero.
- Smoke overall status is `WARNING` or `FAIL`.

## Likely causes
- One or more readiness domains degraded (API, workers, disk, DB/access, integration).
- Recent deploy/reboot/restore changed runtime state.

## Checks to perform
1. Re-run smoke once to rule out transient execution issues:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Capture detailed output:
   ```bash
   python scripts/doctor.py production-smoke --profile prod --json --json-out /tmp/production-smoke.json
   ```
3. Confirm key quick signals:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
   ```

## Actions to take
- **Preferred production path:** Route by failing check domain:
  - API checks fail → `api_not_responding.md`
  - Worker heartbeat/runtime roles fail → `worker_heartbeat_missing.md`
  - Disk checks fail/warn critically → `low_disk_space.md`
  - DB/access/restore-related checks fail → `backup_restore_verification_failure.md`
  - Integration blockers appear → `token_or_integration_readiness_problem.md`
- **Alternative / debug-only path:** targeted `--checks` smoke runs are for diagnosis; final go/no-go must use full smoke.

## Verification after fix
- Full smoke command returns `exit_code=0` and `overall_status=OK`.
- Any incident-specific follow-on checks also pass.

## Escalation / fallback
- If full smoke remains non-OK after mapped playbook execution, escalate with `/tmp/production-smoke.json` and command outputs.
