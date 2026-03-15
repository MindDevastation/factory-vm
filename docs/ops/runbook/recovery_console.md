# Recovery Console

Recovery Console is the operator entrypoint for manual recovery of problematic jobs without bypassing runtime state machine rules.

## Entry points

- UI page: `/ui/ops/recovery`
- API read model: `GET /v1/ops/recovery/jobs`
- API audit trail: `GET /v1/ops/recovery/audit`
- API actions:
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

If `confirm=false`, API returns `409 confirmation is required`.

## What is shown

`GET /v1/ops/recovery/jobs` returns:

- `summary` (compact operational counts)
- `jobs` with issue flags and actionability
- filter domains (`channels`, `states`)

Issue flags:

- `failed` — job is in failure state (`FAILED`, `RENDER_FAILED`, `QA_FAILED`, `UPLOAD_FAILED`)
- `stale_or_stuck` — `FETCHING_INPUTS`/`RENDERING` lock is stale per `job_lock_ttl_sec`
- `cleanup_pending` — published job still has cleanup schedule
- `artifact_issue` — error text hints artifact/mp4/missing/cleanup issue

Supported filters:

- `channel=<slug>`
- `state=<state>`
- `actionability=<retryable|cancellable|reclaimable|cleanupable|restartable>`

## Guard rails and safe actions

Console intentionally does **not** introduce a parallel state machine.

- `retry` delegates to existing `retry_failed_ui_job` flow.
- `cancel` delegates to existing cancel API and terminal-state guards.
- `reclaim` applies stale-lock reclaim only for one reclaimable job and follows existing retry/terminal logic.
- `cleanup` removes local job artifacts under storage paths (workspace/outbox/preview/qa) only.
- `restart` is controlled re-enqueue and is allowed only for safe `DRAFT` jobs with no active inputs (`check_ui_render_guard`).

If action is not safe for current state, API returns `409` with explicit message.

## Audit trail

Each recovery action appends a JSON line record into:

- `<storage_root>/logs/recovery_audit.jsonl`

Record fields:

- `timestamp`
- `actor`
- `job_id`
- `action`
- `reason`
- `result`
- `details`

UI reads latest records via `GET /v1/ops/recovery/audit`.

## How to test quickly

```bash
python -m unittest tests.integration.test_recovery_console_api -v
python -m unittest tests.e2e.test_ui_pages_slice4 -v
```
