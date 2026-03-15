# Recovery Console

Recovery Console is the production operator entrypoint for manual recovery of problematic jobs **without bypassing runtime state-machine guards**.

> Auth model: all `/v1/ops/recovery/*` and `/ui/ops/recovery` routes require API Basic Auth (`FACTORY_BASIC_AUTH_USER` / `FACTORY_BASIC_AUTH_PASS`).

## Real entry points (current code)

- UI page: `GET /ui/ops/recovery`
- Read model: `GET /v1/ops/recovery/jobs`
- Audit feed: `GET /v1/ops/recovery/audit?limit=<n>` (server clamps limit to `1..200`)
- Action endpoints:
  - `POST /v1/ops/recovery/jobs/{job_id}/retry`
  - `POST /v1/ops/recovery/jobs/{job_id}/reclaim`
  - `POST /v1/ops/recovery/jobs/{job_id}/cleanup`
  - `POST /v1/ops/recovery/jobs/{job_id}/cancel`
  - `POST /v1/ops/recovery/jobs/{job_id}/restart`

All write actions require confirmation payload:

```json
{
  "confirm": true,
  "reason": "operator reason"
}
```

If `confirm=false` (or omitted), API returns `409 confirmation is required`.

## What the console shows

`GET /v1/ops/recovery/jobs` response contains:

- `summary`: compact counters for current filtered result set
- `jobs`: recovery-oriented rows with `issue_flags` and `actions`
- `filters`: available `channels` and `states` from current result set

Issue flags (computed server-side):

- `failed` — state in `FAILED|RENDER_FAILED|QA_FAILED|UPLOAD_FAILED`
- `stale_or_stuck` — `FETCHING_INPUTS`/`RENDERING` lock is stale (`locked_at < now - job_lock_ttl_sec`)
- `cleanup_pending` — state `PUBLISHED` with non-null `delete_mp4_at`
- `artifact_issue` — `error_reason` includes `artifact|mp4|missing|cleanup`

Actionability filter values:

- `retryable`
- `cancellable`
- `reclaimable`
- `cleanupable`
- `restartable`

Request filters:

- `channel=<slug>`
- `state=<state>`
- `actionability=<one_of_values_above>`

Invalid `actionability` returns `422 invalid actionability`.

## Guardrails per action

The console delegates to existing runtime APIs/guards (no parallel workflow):

- `retry` → same logic as UI retry endpoint (`api_ui_job_retry`)
- `cancel` → same logic as cancel endpoint (`api_cancel`) and terminal-state checks
- `reclaim` → stale-lock reclaim via `_manual_reclaim_job`
- `cleanup` → hard cleanup only for jobs marked `cleanupable`; otherwise `409 job is not cleanupable`
- `restart` → controlled render restart only when `check_ui_render_guard(...).eligible == true`
  - non-eligible request returns `409 controlled restart is supported only for Draft jobs with no active inputs`

## Operator flow (practical-first)

1. Open `/ui/ops/recovery`.
2. Filter by `channel` / `state` / `actionability` to isolate actionable jobs.
3. Inspect row flags and `error_reason` text.
4. Trigger one action at a time with clear reason text.
5. Verify audit row appeared and result is `ok`.
6. Re-run smoke if incident touched readiness/worker/disk paths:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```

## API-first examples (for incident notes or non-UI operation)

List current recovery queue:

```bash
curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" \
  "http://127.0.0.1:8080/v1/ops/recovery/jobs?actionability=reclaimable"
```

Force cleanup with explicit confirmation:

```bash
curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" \
  -H "Content-Type: application/json" \
  -X POST \
  -d '{"confirm":true,"reason":"cleanup orphaned artifacts after incident"}' \
  "http://127.0.0.1:8080/v1/ops/recovery/jobs/<job_id>/cleanup"
```

Read recent audit entries:

```bash
curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" \
  "http://127.0.0.1:8080/v1/ops/recovery/audit?limit=30"
```

## Audit trail

Each recovery action appends one JSONL record to:

- `<storage_root>/logs/recovery_audit.jsonl`

Record fields:

- `timestamp`
- `actor` (current `FACTORY_BASIC_AUTH_USER`)
- `job_id`
- `action`
- `reason`
- `result` (`ok`/`failed`/`rejected`)
- `details`

## Related runbook paths

- Stale/stuck triage: `playbooks/stale_or_stuck_jobs.md`
- Repeated failures: `playbooks/repeated_job_failures.md`
- Disk pressure path: `playbooks/low_disk_space.md`
- Smoke failure path: `playbooks/smoke_check_failure.md`

## How to validate this doc against code

```bash
python -m unittest tests.integration.test_recovery_console_api -v
python -m unittest tests.e2e.test_ui_pages_slice4 -v
```
