# SOP: Backup and Restore CLI

Preferred production path: use `scripts/ops_backup_restore.py` only, with explicit verify-before-restore and manual service stop/start.

## Scheduled backup automation (production)

Use systemd timer automation for regular backups:

```bash
sudo cp deploy/systemd/factory-backup.service /etc/systemd/system/factory-backup.service
sudo cp deploy/systemd/factory-backup.timer /etc/systemd/system/factory-backup.timer
sudo systemctl daemon-reload
sudo systemctl enable --now factory-backup.timer
systemctl list-timers factory-backup.timer
```

The timer runs `scripts/ops_backup_schedule.py run`, which executes `backup create` and then `backup verify` for `<FACTORY_BACKUP_DIR>/latest_successful`.

To validate scheduler health and last runs:

```bash
systemctl status factory-backup.timer factory-backup.service
journalctl -u factory-backup.service -n 100 --no-pager
```

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

Expected success output includes `restore_ok backup_id=<backup_id> restore_id=<restore_id> quarantine_dir=/quarantine/<restore_id>/ restored=<count>`.

4. Start services manually.
5. Run post-restore verification runbook (`../post_restore_verification.md`).

## Guardrails

- Automatic service stop/start is not implemented.
- Manual stop marker policy is intentional (`.services_stopped`) to prevent unsafe restore into running services.
- If restore fails checksum/manifest validation, do not force manual file replacement; select another verified backup.
