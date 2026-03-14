# SOP: Backup and Restore CLI

Preferred production path: use `scripts/ops_backup_restore.py` only, with explicit verify-before-restore and manual service stop/start.

## Backup create/list/verify

```bash
PYTHONPATH=. python scripts/ops_backup_restore.py backup create
PYTHONPATH=. python scripts/ops_backup_restore.py backup list
PYTHONPATH=. python scripts/ops_backup_restore.py backup verify --backup-id <backup_id>
```

## Restore workflow

1. Stop API/workers via deployment-configured service manager command/path.
2. Ensure services-stopped marker file exists (default `<FACTORY_BACKUP_DIR>/.services_stopped`, or custom `FACTORY_SERVICES_STOPPED_FILE`).
3. Run restore:

```bash
PYTHONPATH=. python scripts/ops_backup_restore.py restore --backup-id <backup_id>
```

4. Start services manually.
5. Run post-restore verification runbook (`../post_restore_verification.md`).

## Guardrails

- Automatic service stop/start is not implemented.
- If restore fails checksum/manifest validation, do not force manual file replacement; select another verified backup.
