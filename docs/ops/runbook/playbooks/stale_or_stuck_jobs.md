# Playbook: Stale or Stuck Jobs

## Symptoms
- Jobs remain in queued/in-progress state without expected stage movement.
- Dashboard job rows stop progressing for extended periods.
- Smoke may pass API but still show worker/readiness warnings.

## Likely causes
- Worker role responsible for current stage is unavailable/stale.
- Runtime disk pressure blocks write-heavy operations.
- A specific job attempt is wedged while services are otherwise healthy.

## Checks to perform
1. Snapshot readiness:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Check workers:
   ```bash
   curl -fsS http://127.0.0.1:8080/v1/workers
   ```
3. Inspect job details/logs for a stuck job:
   ```bash
   curl -fsS http://127.0.0.1:8080/v1/jobs/<job_id>
   curl -fsS "http://127.0.0.1:8080/v1/jobs/<job_id>/logs?tail=200"
   ```

## Actions to take
- **Preferred production path:** Use dashboard/API recovery controls first:
  - Dashboard jobs table Retry button, or
  - `POST /v1/ui/jobs/<job_id>/retry` through the API.
- Before retrying many jobs, recover root cause using `worker_heartbeat_missing.md` and/or `low_disk_space.md`.
- **Alternative / debug-only path:** direct process-level worker execution is diagnostic only; do not use manual DB edits.

## Verification after fix
- New retry job is enqueued/created successfully.
- Stage transitions resume on retried/new jobs.
- No new stuck-job growth in the next observation window.

## Escalation / fallback
- If retry endpoint is blocked (for example `DISK_CRITICAL_WRITE_BLOCKED`), execute `low_disk_space.md`.
- If repeated retries fail across many jobs, execute `repeated_job_failures.md`.
