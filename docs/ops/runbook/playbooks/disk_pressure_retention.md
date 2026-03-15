# Playbook: Disk Pressure and Retention

## Trigger

- smoke `disk_space` warning/fail, or
- retention/disk logs indicate critical pressure.

## Preferred production path

1. Run non-destructive scan first:

```bash
python scripts/ops_retention.py scan
```

2. Run normal retention deletion pass:

```bash
python scripts/ops_retention.py run
```

3. Emergency only: urgent mode (effective only under critical pressure):

```bash
python scripts/ops_retention.py run --urgent
```

4. Re-run smoke gate:

```bash
python scripts/doctor.py production-smoke --profile prod
```

## Scheduled operation

Repository-provided schedule artifacts:

- `deploy/systemd/factory-retention.service`
- `deploy/systemd/factory-retention.timer`

Journald cap artifact:

- `deploy/systemd/journald-retention.conf`

## Logging retention contour (explicit production contract)

- Project file logs are rotated by app-level Python `RotatingFileHandler` policy.
- Service stdout/stderr retention is bounded by journald caps from `deploy/systemd/journald-retention.conf`.
- Retention runner (`scripts/ops_retention.py`) cleans only disposable artifacts under allowlisted roots.

System `logrotate` is intentionally not part of this baseline because it would duplicate management of already-rotated project log files. Introduce logrotate only when a new unmanaged file-log source is proven in deployment.

## Never-delete guardrail

Retention policy must never delete:

- DB, backups/snapshots/quarantine,
- config/policy/canonical seed artifacts,
- media source library and final output library,
- active or uncertain workspaces,
- any path outside allowlisted retention roots.

Enable scheduler using deployment-configured path/command defined in those artifacts.
