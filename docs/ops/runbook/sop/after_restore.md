# SOP: After Restore

## Purpose
Validate service recovery and runtime readiness immediately after a backup restore.

## When to use
- After running `scripts/ops_backup_restore.py restore --backup-id <backup_id>`.

## Preconditions
- Restore command already completed.
- Service restart method is known from deployment artifacts (`deploy/systemd/*.service` for systemd pattern).
- API endpoint is expected on `127.0.0.1:8080`.

## Steps
1. Confirm restore command output includes `restore_ok backup_id=<backup_id> restore_id=<restore_id>`.
2. Start services manually with your deployment-specific service manager command/path.
3. Run production smoke:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
4. Verify API health:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   ```
5. Verify workers endpoint:
   ```bash
   curl -fsS http://127.0.0.1:8080/v1/workers
   ```
6. Check quarantine output under `<FACTORY_BACKUP_DIR>/quarantine/<restore_id>/` for replaced pre-restore files.
7. Record restore ID and verification timestamp in operator notes.

## Expected success result
- Restore completion marker indicates `restore_ok`.
- Smoke returns `OK`.
- API and workers endpoints respond successfully.

## Escalation / next document if failed
- Re-verify/select backup and follow `backup_and_restore_cli.md`.
- Use scenario guide `../post_restore_verification.md`.
- Use `../playbooks/api_unhealthy.md` or `../playbooks/worker_stalled.md` based on failing signal.
