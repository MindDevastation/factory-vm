# SOP: Before Risky Maintenance

## Purpose
Reduce rollback risk before maintenance actions that may impact runtime/data integrity.

## When to use
- Before manual config edits, system package changes, storage moves, or service-level maintenance.

## Preconditions
- Maintenance window approved.
- Operator has permission to stop/start services.
- Backup storage path and env are available.

## Steps
1. Run a baseline production smoke check:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Create a backup using the preferred backup CLI:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py backup create
   ```
3. List backups and capture the newest `backup_id`:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py backup list
   ```
4. Verify the selected backup before maintenance:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py backup verify --backup-id <backup_id>
   ```
5. Record planned rollback command using the same `backup_id`.
6. Proceed with maintenance only after baseline smoke + backup verify both pass.

## Expected success result
- Baseline smoke is `OK`.
- A new backup exists and `backup verify` succeeds for chosen `backup_id`.
- Rollback reference is documented before maintenance starts.

## Escalation / next document if failed
- Do not continue maintenance.
- Use `backup_and_restore_cli.md` for backup/restore guardrails.
- Use `../post_restore_verification.md` after any restore rollback.
