# Playbook: Repeated Job Failures

## Symptoms
- Multiple recent jobs fail in the same stage/error class.
- Retry actions create new attempts that fail again quickly.
- Operator sees rising failed counts in dashboard jobs list.

## Likely causes
- Upstream readiness issue (API, workers, disk, dependency/token readiness).
- Consistent integration/auth failure for upload/origin steps.
- Environment drift after deploy/restore.

## Checks to perform
1. Run full smoke and capture failing check IDs:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Validate API + workers endpoints:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
   ```
3. Inspect representative failed jobs:
   ```bash
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" "http://127.0.0.1:8080/v1/jobs?state=FAILED"
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" "http://127.0.0.1:8080/v1/jobs/<job_id>/logs?tail=200"
   ```

## Actions to take
- **Preferred production path:** Classify failures by readiness signal, then route:
  - API unavailable → `api_not_responding.md`
  - Missing/stale workers → `worker_heartbeat_missing.md`
  - Disk pressure / write block → `low_disk_space.md`
  - Token/integration readiness blockers → `token_or_integration_readiness_problem.md`
- Use dashboard/API Retry only after root cause recovery.
- **Alternative / debug-only path:** ad-hoc local reruns are for diagnosis only and must not replace service-managed production flow.

## Verification after fix
- Smoke returns `OK`.
- New attempts stop failing in the previous repeated pattern.
- Failed job accumulation trend flattens.

## Escalation / fallback
- If failures persist after all mapped playbooks, escalate with failing job IDs, stage, and log excerpts.
