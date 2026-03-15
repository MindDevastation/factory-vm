# SOP: When Disk Warning Triggers

## Purpose
Handle disk warning/critical signals with the preferred retention workflow before service impact grows.

## When to use
- Smoke reports `disk_space` warning/fail.
- Logs show disk warning/critical events.

## Preconditions
- Operator has repo-root shell access.
- Retention policy is configured for target environment.

## Steps
1. Confirm current pressure via smoke:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Run retention scan (non-destructive first):
   ```bash
   python scripts/ops_retention.py scan
   ```
3. Run standard retention deletion pass:
   ```bash
   python scripts/ops_retention.py run
   ```
4. Only if pressure remains critical, run urgent mode:
   ```bash
   python scripts/ops_retention.py run --urgent
   ```
5. Re-run smoke to confirm disk status recovery:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
6. Verify API health after cleanup:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   ```

## Expected success result
- Disk check returns to non-critical state.
- Smoke overall is `OK` (or warning is resolved/accepted per policy).
- API remains reachable after retention actions.

## Escalation / next document if failed
- Follow full disk playbook: `../playbooks/low_disk_space.md`.
- If API degrades during cleanup, also use `../playbooks/api_not_responding.md`.
