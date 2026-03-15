# Playbook: Low Disk Space

## Symptoms
- Smoke reports `disk_space` warning/fail.
- API returns `503 DISK_CRITICAL_WRITE_BLOCKED` for write-heavy routes.
- Jobs fail/retry operations are blocked under critical pressure.

## Likely causes
- Retention backlog (temporary previews/exports/workspaces not yet cleaned).
- Unexpected storage growth under `FACTORY_STORAGE_ROOT`.
- Journald/runtime logs consuming planned budget.

## Checks to perform
1. Confirm current pressure:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Run retention scan (non-destructive):
   ```bash
   python scripts/ops_retention.py scan
   ```
3. Review journald usage:
   ```bash
   journalctl --disk-usage
   ```

## Actions to take
- **Preferred production path:** Use retention runner in order:
  ```bash
  python scripts/ops_retention.py run
  python scripts/ops_retention.py run --urgent
  ```
  Run urgent mode only when pressure is still critical.
- Follow deployment scheduler policy in `deploy/systemd/factory-retention.service` and `deploy/systemd/factory-retention.timer`.
- **Alternative / debug-only path:** Do not manually delete arbitrary directories or backup/quarantine paths as routine remediation.

## Verification after fix
- Smoke `disk_space` returns PASS/WARN per policy (not FAIL).
- Previously blocked write-heavy API operations are accepted again.
- API health remains reachable.

## Escalation / fallback
- If pressure remains critical after urgent retention pass, escalate with scan/run logs and mount usage details.
