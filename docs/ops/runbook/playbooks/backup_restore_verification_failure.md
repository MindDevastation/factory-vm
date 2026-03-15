# Playbook: Backup/Restore Verification Failure

## Symptoms
- `backup verify` fails for target backup ID.
- Post-restore checks fail (smoke/API/workers not healthy).
- Restore output is missing `restore_ok backup_id=<...> restore_id=<...>`.

## Likely causes
- Backup artifact corruption/incomplete files.
- Wrong backup selected for restore target state.
- Services not stopped/started according to restore guardrails.

## Checks to perform
1. List available backups:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py backup list
   ```
2. Verify intended backup before restore:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py backup verify --backup-id <backup_id>
   ```
3. If restore was attempted, confirm command output and quarantine path:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py restore --backup-id <backup_id>
   ```
4. Run post-restore readiness checks:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   curl -fsS http://127.0.0.1:8080/health
   curl -fsS http://127.0.0.1:8080/v1/workers
   ```

## Actions to take
- **Preferred production path:** Follow `sop/backup_and_restore_cli.md` exactly (verify-before-restore, service stop marker, manual service restart).
- If verification fails, select a different verified backup ID; do not force manual file replacement.
- **Alternative / debug-only path:** limited artifact inspection in backup directories is acceptable for diagnosis only.

## Verification after fix
- `backup verify` succeeds for selected backup.
- Restore run prints `restore_ok ...` and post-restore checks pass.

## Escalation / fallback
- If no available backup verifies, escalate as incident and halt further restore attempts.
