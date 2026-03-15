# SOP: When Smoke Fails

## Purpose
Provide a deterministic first-response checklist when `production-smoke` returns warning/fail.

## When to use
- `python scripts/doctor.py production-smoke --profile prod` returns non-zero.
- Smoke output shows `overall_status` of `WARNING` or `FAIL`.

## Preconditions
- Failed smoke output is available in terminal (or captured JSON).
- Operator can access local API and service logs.

## Steps
1. Re-run smoke once to rule out transient runner issues:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Capture failing check IDs from output (`failed_checks` / `warning_checks`).
3. If API-related checks fail, run:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   ```
4. If worker/readiness checks fail, run:
   ```bash
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
   ```
5. If disk check fails/warns, run:
   ```bash
   python scripts/ops_retention.py scan
   ```
6. If DB/access checks fail (`db_access`, `pipeline_readiness`), route to `../post_restore_verification.md` (DB/access symptom path) and execute restore verification/remediation via `./backup_and_restore_cli.md` as directed there.
7. Route immediately to the matching playbook and follow it fully.

## Expected success result
- Root failure class is identified (API, workers, disk, DB/access, etc.).
- Operator is on a specific remediation playbook, not ad-hoc troubleshooting.

## Escalation / next document if failed
- API path: `../playbooks/api_not_responding.md`.
- Worker path: `../playbooks/worker_heartbeat_missing.md`.
- Disk path: `../playbooks/low_disk_space.md`.
- DB/access path: `../post_restore_verification.md` (with `./backup_and_restore_cli.md` for verify/restore procedure).
- If unresolved after playbook completion, escalate as production incident.
