# Backup / Restore Operator SOP (P0-S5)

> **Runbook canonical note:** The canonical operator workflow now lives in `docs/ops/runbook/README.md` and the linked SOPs/scenarios.
>
> **Deprecated/secondary note:** This page is a secondary CLI reference only. If any procedural step here conflicts with the runbook package, follow `docs/ops/runbook/*` as the operational source of truth.

## Summary (secondary reference)

Use this document for command-level backup/restore CLI details only. For end-to-end operator flow, use:

- `docs/ops/runbook/sop/backup_and_restore_cli.md`
- `docs/ops/runbook/post_restore_verification.md`

This SOP covers the current backup/restore CLI implemented by `scripts/ops_backup_restore.py`.

## 1) What is included in backup

Backups only include paths from the explicit backup scope:

- SQLite DB at `FACTORY_DB_PATH` (stored as `db/app.sqlite3` in snapshot).
- Environment files from `FACTORY_ENV_FILES` (explicit allowlist only).
- Config file/dir paths from `FACTORY_BACKUP_CONFIG_PATHS`.
- Export file/dir paths from `FACTORY_BACKUP_EXPORT_DIRS`.
- Snapshot metadata files:
  - `manifest.json`
  - `checksums.sha256`

Backup root layout:

- `<FACTORY_BACKUP_DIR>/index.json`
- `<FACTORY_BACKUP_DIR>/latest_successful`
- `<FACTORY_BACKUP_DIR>/snapshots/<backup_id>/...`
- `<FACTORY_BACKUP_DIR>/quarantine/<restore_id>/...` (created during restore)

`backup_id` format is UTC timestamp `YYYYmmddTHHMMSSZ`.

## 2) What is excluded

- Any file/dir not reachable from the four scope env vars above.
- Service runtime state/process state (systemd state, running workers, memory).
- Automatic service stop/start automation (not implemented in P0).
- Secret values are not copied into manifest fields; only source paths and checksums/metadata are recorded.

## 3) Required env vars

Minimum required for CLI startup:

- `FACTORY_BACKUP_DIR`
- `FACTORY_DB_PATH`

Backup scope env vars:

- `FACTORY_ENV_FILES`
- `FACTORY_BACKUP_CONFIG_PATHS`
- `FACTORY_BACKUP_EXPORT_DIRS`

Important format note:

- `FACTORY_ENV_FILES`, `FACTORY_BACKUP_CONFIG_PATHS`, and `FACTORY_BACKUP_EXPORT_DIRS` are **colon-separated** path lists (`:`), not comma-separated.

Default behavior when `FACTORY_ENV_FILES` is unset:

- Env file scope is empty. No implicit fallback paths are added.

## 4) How to create backup

```bash
PYTHONPATH=. python scripts/ops_backup_restore.py backup create
```

Expected success output:

- `backup_created=<FACTORY_BACKUP_DIR>/snapshots/<backup_id>`

Operational notes:

- `FACTORY_BACKUP_DIR` is created with mode `0700`.
- Generated snapshot files are written with restrictive permissions (`0600`).
- If an allowlisted env/config/export path does not exist, backup fails fast with `error_code=OPS_BACKUP_CONFIG_INVALID`.
- Retention pruning is attempted after successful snapshot creation.

## 4.1) Production scheduling (systemd timer)

Preferred production automation path uses systemd timer units:

- `deploy/systemd/factory-backup.service`
- `deploy/systemd/factory-backup.timer`

The service runs:

```bash
python -m scripts.ops_backup_schedule run
```

Wrapper behavior:

- runs `backup create`,
- reads `<FACTORY_BACKUP_DIR>/latest_successful`,
- runs `backup verify --backup-id <latest_successful>`,
- emits `scheduled_backup_ok backup_id=<backup_id>` on success.

Recommended install/enable:

```bash
sudo cp deploy/systemd/factory-backup.service /etc/systemd/system/factory-backup.service
sudo cp deploy/systemd/factory-backup.timer /etc/systemd/system/factory-backup.timer
sudo systemctl daemon-reload
sudo systemctl enable --now factory-backup.timer
systemctl list-timers factory-backup.timer
```

Default schedule from the unit file:

- `OnCalendar=*-*-* 03:17:00`
- `Persistent=true` (catch-up run after downtime)
- `RandomizedDelaySec=600`

If production policy requires another window, change only `OnCalendar` in `factory-backup.timer`.

## 5) How to list/verify backups

List indexed snapshots:

```bash
PYTHONPATH=. python scripts/ops_backup_restore.py backup list
```

Output columns:

- `backup_id<TAB>status<TAB>created_at`

Verify a specific backup before restore:

```bash
PYTHONPATH=. python scripts/ops_backup_restore.py backup verify --backup-id <backup_id>
```

Expected success output:

- `verify_ok backup_id=<backup_id> snapshot=<snapshot_path>`

If verification fails, command exits non-zero and prints `error_code=...`.

## 6) Step-by-step restore procedure

Minimum restore runbook:

1. **Stop services** (web + workers) using your service manager.
2. **Mark services stopped** by creating the restore guard file (guard-file policy, not daemon-state inspection):
   - default marker path: `<FACTORY_BACKUP_DIR>/.services_stopped`
   - or set `FACTORY_SERVICES_STOPPED_FILE` to a custom path and create that file.
3. **List available backups**:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py backup list
   ```
4. **Verify target backup**:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py backup verify --backup-id <backup_id>
   ```
5. **Run restore**:
   ```bash
   PYTHONPATH=. python scripts/ops_backup_restore.py restore --backup-id <backup_id>
   ```
6. **Check restore success** from stdout:
   - `restore_ok backup_id=<backup_id> restore_id=<restore_id> quarantine_dir=<path> restored=<count>`
7. **Restart services manually** (no auto-restart is implemented).
8. **Perform post-restore health validation**:
   - app process is up,
   - endpoints/health checks respond,
   - job processing resumes,
   - expected configs/env files are present,
   - DB integrity issues are absent (restore runs SQLite `PRAGMA integrity_check` and fails if not `ok`).

## 7) What quarantine means

During restore, any existing target file/dir being replaced is moved to:

- `<FACTORY_BACKUP_DIR>/quarantine/<restore_id>/...`

This is a rollback safety copy of pre-restore targets. Quarantine paths are URL-encoded absolute target paths to avoid path collisions.

## 8) Permissions/secret handling

- Backup root and snapshots are private (`0700` root dir, sensitive files `0600`).
- Env files may contain secrets; they are backed up as files but manifest only stores metadata:
  - source path,
  - stored artifact path,
  - size/checksum,
  - `contains_secrets=true` marker for env items.
- Do not share snapshot contents, `checksums.sha256`, env artifacts, or quarantine copies outside privileged operator channels.

## 9) Retention behavior

Retention applies to successful snapshots and is triggered by:

- `backup create` (post-success prune), and
- explicit prune command:
  ```bash
  PYTHONPATH=. python scripts/ops_backup_restore.py backup prune
  ```

Policy implemented:

- always keep latest successful snapshot,
- keep up to 7 daily,
- keep up to 4 weekly,
- keep up to 6 monthly.

Retention labels are recorded per snapshot in `index.json` under `retention_labels`.

## 9.1) Operational checks for schedule + retention

Check scheduler state:

```bash
systemctl status factory-backup.timer factory-backup.service
systemctl list-timers factory-backup.timer
```

Check recent scheduled backup logs:

```bash
journalctl -u factory-backup.service -n 100 --no-pager
```

Expected success signals in logs/stdout:

- `backup_created=<FACTORY_BACKUP_DIR>/snapshots/<backup_id>`
- `verify_ok backup_id=<backup_id> snapshot=<snapshot_path>`
- `scheduled_backup_ok backup_id=<backup_id>`

Verify snapshots and retention/prune effects:

```bash
PYTHONPATH=. python scripts/ops_backup_restore.py backup list
PYTHONPATH=. python scripts/ops_backup_restore.py backup prune
```

Retention is functioning when:

- old non-retained snapshots disappear from `<FACTORY_BACKUP_DIR>/snapshots`,
- `backup list` still shows `latest` plus daily/weekly/monthly retained backups,
- `index.json` entries include expected `retention_labels`.

## 10) Common failure modes and operator actions

- `error_code=OPS_RESTORE_SERVICES_RUNNING`
  - Cause: services-stopped marker file missing.
  - Action: stop services, create marker file, rerun restore.
- `error_code=OPS_RESTORE_BACKUP_NOT_FOUND`
  - Cause: backup id missing in index or not `SUCCESS`.
  - Action: rerun `backup list`, choose valid `SUCCESS` id.
- `error_code=OPS_RESTORE_CHECKSUM_FAILED`
  - Cause: snapshot content/checksum mismatch.
  - Action: do not restore from that snapshot; select another verified backup.
- `error_code=OPS_RESTORE_MANIFEST_INVALID`
  - Cause: malformed or incomplete manifest/artifact mapping.
  - Action: select another backup and run `backup verify` first.
- `error_code=OPS_RESTORE_DB_INTEGRITY_FAILED`
  - Cause: restored SQLite DB failed integrity check.
  - Action: restore a different verified snapshot; keep failed artifacts for incident analysis.
- `error_code=OPS_RESTORE_QUARANTINE_FAILED`
  - Cause: cannot create quarantine directory (permissions/path issue).
  - Action: fix backup dir permissions/free space, retry restore.
- `error_code=OPS_RETENTION_PRUNE_FAILED`
  - Cause: prune operation failed.
  - Action: backup may still be created; inspect logs and run prune again after remediation.
