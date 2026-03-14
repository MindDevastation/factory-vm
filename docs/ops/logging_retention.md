# Logging retention and disk-pressure operations policy

This document defines the operator runbook for `SPEC_OPS_LOGGING_RETENTION_v1.0` P0-S5.

## 1) What this policy covers

This policy covers four operator-owned controls:

- bounded project log files under `FACTORY_LOG_DIR` (`storage/logs` by default),
- journald storage caps for system services,
- retention cleanup of disposable runtime artifacts,
- disk-pressure guardrails that block selected write-heavy API operations when free space is critical.

The goals are deterministic disk usage, safe deletion scope, and clear operational signals during warning/critical pressure.

## 2) Log classes and where they are stored

Project-owned log classes are written as rotating files (plus stdout/stderr to journald):

- `application_logs` → `app.log`
- `worker_runtime_logs` → `workers.log`
- `bot_logs` → `bot.log`
- `uploader_render_logs` → `pipeline.log`
- `recovery_audit_logs` → `recovery.log`
- `smoke_ops_logs` → `ops.log`

Storage tiers:

- project files (`tier_a_project_file`): under `FACTORY_LOG_DIR` (default: `<FACTORY_STORAGE_ROOT>/logs`),
- service stdout/stderr (`tier_b_journald`): managed by systemd journald.

## 3) Project-owned log rotation policy

Rotation is implemented by Python `RotatingFileHandler` and is size-based.

Default per-class caps:

- `application_logs`: rotate at 20 MiB, keep 10 files
- `worker_runtime_logs`: rotate at 20 MiB, keep 10 files
- `bot_logs`: rotate at 10 MiB, keep 7 files
- `uploader_render_logs`: rotate at 25 MiB, keep 8 files
- `recovery_audit_logs`: rotate at 10 MiB, keep 12 files
- `smoke_ops_logs`: rotate at 5 MiB, keep 12 files

Notes:

- active file stays `*.log`, older files are capped by backup count,
- service→class mapping is deterministic,
- stdout logging is retained for journald visibility.

## 4) Journald retention hardening policy

Use deploy snippet `deploy/systemd/journald-retention.conf`:

- copy to `/etc/systemd/journald.conf.d/factory-retention.conf`,
- restart journald,
- verify effective config and disk usage.

Configured caps:

- `SystemMaxUse=512M`
- `RuntimeMaxUse=128M`
- `MaxRetentionSec=30day`

Recommended apply/verify:

```bash
sudo mkdir -p /etc/systemd/journald.conf.d
sudo cp deploy/systemd/journald-retention.conf /etc/systemd/journald.conf.d/factory-retention.conf
sudo systemctl restart systemd-journald
systemd-analyze cat-config systemd/journald.conf
journalctl --disk-usage
```

## 5) Disposable vs protected artifact policy

Retention runner deletes only allowlisted disposable categories:

- `temporary_previews` (default dir: `<storage_root>/previews`)
- `temporary_exported_files` (default dir: `<storage_root>/outbox`)
- `transient_reports_intermediate_files` (default dir: `<storage_root>/qa`)
- `terminal_abandoned_job_workspaces` (default dir: `<storage_root>/workspace`)
- `stale_temp_scratch_dirs` (default dir: `<storage_root>/tmp`)

Optional root overrides (env):

- `FACTORY_RETENTION_PREVIEW_DIR`
- `FACTORY_RETENTION_EXPORT_DIR`
- `FACTORY_RETENTION_TRANSIENT_REPORT_DIR`
- `FACTORY_RETENTION_WORKSPACE_DIR`
- `FACTORY_RETENTION_SCRATCH_DIR`

Protected behavior (never delete):

- any path outside category root (`RETENTION_SKIP_OUTSIDE_SCOPE`),
- protected path tokens: `backup`, `backups`, `snapshot`, `snapshots`, `quarantine`, `config`, `configs`, `media`, `library`, `final_output`,
- active/uncertain workspaces (`.active`, `.lock`, `.pid`, non-terminal DB state, missing/uncertain DB state).

## 6) Retention runner commands

Run from repo root with deployed env loaded.

### scan

Report-only evaluation, no deletion:

```bash
python scripts/ops_retention.py scan
```

### run

Destructive deletion for policy-approved, expired disposable artifacts:

```bash
python scripts/ops_retention.py run
```

### urgent (implemented)

Urgent mode is available only on `run` and only becomes effective when disk pressure is `CRITICAL`:

```bash
python scripts/ops_retention.py run --urgent
```

If disk pressure is not critical, urgent mode is ignored and normal windows apply.

## 7) Disk warning/critical thresholds

Disk pressure is evaluated from available space on `FACTORY_STORAGE_ROOT` using both percentage and absolute GiB thresholds.

Defaults:

- warning when free percent `< 15.0` **or** free GiB `< 20.0`,
- critical when free percent `< 8.0` **or** free GiB `< 10.0`.

Env overrides:

- `FACTORY_SMOKE_DISK_WARN_PERCENT`
- `FACTORY_SMOKE_DISK_WARN_GIB`
- `FACTORY_SMOKE_DISK_FAIL_PERCENT`
- `FACTORY_SMOKE_DISK_FAIL_GIB`

## 8) What operations are blocked under critical disk

When disk pressure is `CRITICAL`, API returns `503 DISK_CRITICAL_WRITE_BLOCKED` for these write-heavy operations:

- `GET /v1/track-catalog/analysis-report.xlsx`
- `POST /v1/track_jobs/analyze`
- `POST /v1/ui/jobs/render_selected`
- `POST /v1/ui/jobs/render_all`
- `POST /v1/ui/jobs/{job_id}/render`
- `POST /v1/ui/jobs/{job_id}/retry`

Other endpoints are not blocked by this specific guard.

## 9) Structured retention log event meanings

Retention emits structured `retention.event` payloads.

Lifecycle events:

- `retention.scan.start`: retention pass started,
- `retention.scan.complete`: pass finished with `deleted/skipped/failed` summary,
- `retention.urgent.start`: urgent requested (`enabled` or `ignored`),
- `retention.urgent.complete`: urgent completion summary.

Per-candidate events:

- `retention.skip` with result `skipped` or `dry_run`,
- `retention.delete.success` with result `deleted`,
- `retention.delete.failure` with result `failed` and `error_code`.

Common `reason_code` values:

- skip reasons: `RETENTION_SKIP_OUTSIDE_SCOPE`, `RETENTION_SKIP_PROTECTED_PATH`, `RETENTION_SKIP_ACTIVE_WORKSPACE`, `RETENTION_SKIP_TOO_RECENT`,
- delete reasons: `RETENTION_DELETE_TEMP_PREVIEW_EXPIRED`, `RETENTION_DELETE_EXPORT_EXPIRED`, `RETENTION_DELETE_TRANSIENT_REPORT_EXPIRED`, `RETENTION_DELETE_TERMINAL_WORKSPACE_EXPIRED`, `RETENTION_DELETE_STALE_SCRATCH_EXPIRED`.

Disk pressure events:

- `disk.warning` and `disk.critical` are emitted with thresholds/free-space details when pressure is not OK.

## 10) Safe rollout guidance

Recommended production rollout:

1. **Configure env and systemd artifacts**
   - set env variables in `deploy/env`,
   - install scheduler units from `deploy/systemd/factory-retention.service` and `deploy/systemd/factory-retention.timer`,
   - apply journald hardening snippet.

   Example scheduler install:

   ```bash
   sudo cp deploy/systemd/factory-retention.service /etc/systemd/system/factory-retention.service
   sudo cp deploy/systemd/factory-retention.timer /etc/systemd/system/factory-retention.timer
   sudo systemctl daemon-reload
   sudo systemctl enable --now factory-retention.timer
   systemctl list-timers factory-retention.timer
   ```
2. **Run scan/report-only first**
   - `python scripts/ops_retention.py scan`
   - inspect `retention.skip` reasons and candidate paths.
3. **Verify outputs before deletion**
   - confirm no protected/active assets are selected,
   - confirm disk pressure events and thresholds are expected,
   - validate allowlist root directories.
4. **Enable destructive run**
   - schedule timer-enabled `python scripts/ops_retention.py run`,
   - use `--urgent` only as emergency reduction under critical pressure.

Do not enable destructive scheduling before a clean scan review in the target environment.
