# Backup / Restore Foundation (P0)

This document describes the P0 backup/restore CLI foundation for Factory VM.

## Scope (allowlist only)

Backups include only:

- `FACTORY_DB_PATH`
- `FACTORY_ENV_FILES`
- `FACTORY_BACKUP_CONFIG_PATHS`
- `FACTORY_BACKUP_EXPORT_DIRS`

## Hard requirements implemented

- `FACTORY_BACKUP_DIR` is required.
- Backups are timestamped snapshot directories (`YYYYmmddTHHMMSSZ`).
- Backup root directory is forced to mode `0700`.
- Created backup files (manifest + DB backup) are mode `0600`.
- DB backup uses SQLite online backup API (`sqlite3.Connection.backup`).
- Manifest never stores secret values (it stores paths/metadata only).
- Retention runs only after a successful backup:
  - keep last 7 daily
  - keep last 4 weekly
  - keep last 6 monthly
  - always keep latest successful snapshot
- Restore requires explicit confirmation that services are stopped.
- Restore does **not** auto-start services in P0.

## Environment variables

Required:

- `FACTORY_BACKUP_DIR` (absolute or relative path)

Existing + optional for backup scope:

- `FACTORY_DB_PATH`
- `FACTORY_ENV_FILES` (comma-separated file paths)
- `FACTORY_BACKUP_CONFIG_PATHS` (comma-separated file/dir paths)
- `FACTORY_BACKUP_EXPORT_DIRS` (comma-separated dir paths)

If `FACTORY_ENV_FILES` is unset, defaults to existing runtime env files among:

- `deploy/env`
- `deploy/env.local`
- `deploy/env.prod`

## CLI usage

```bash
PYTHONPATH=. FACTORY_BACKUP_DIR=./data/backups python scripts/ops_backup_restore.py backup
PYTHONPATH=. FACTORY_BACKUP_DIR=./data/backups python scripts/ops_backup_restore.py list
PYTHONPATH=. FACTORY_BACKUP_DIR=./data/backups python scripts/ops_backup_restore.py restore \
  --snapshot 20260102T030405Z \
  --services-stopped-file /tmp/factory.services.stopped
```

Create the `--services-stopped-file` only after all factory services are stopped.
