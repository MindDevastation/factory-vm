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

Enable scheduler using deployment-configured path/command defined in those artifacts.
