# FVR-S8 final handoff (PR #210)

## 1) Traceability metadata (verified)

- PR number: **#210**
- Head branch: **`codex/fix-bulk-json-backend-implementation`**
- Base branch: **`codex/implement-backend-for-bulk-jobs`**
- HEAD commit SHA: **`02f1171e47f72534666969f05341740b02886c15`**
- Commit title/message: **`Handle bulk enqueue runtime exceptions per item`**

## 2) Runtime fix summary

- Added runtime-exception-to-item-error handling for bulk enqueue paths so unexpected exceptions are mapped into per-item `UIJ_INTERNAL` errors instead of bubbling to raw request-level failures.
- The conversion is applied in:
  - Mode B execute (`create_and_enqueue`) enqueue phase.
  - Mode C execute (`enqueue_existing_jobs`) per-item enqueue path.
- Exact failure mode fixed: unhandled runtime exception during enqueue/preflight no longer crashes the whole bulk request path; response remains 200 with item-scoped error payload.

## 3) FVR-S8 artifact set (review-ready examples)

### A) Preview response example (`create_and_enqueue`)

```json
{
  "mode": "create_and_enqueue",
  "summary": {"requested": 1, "valid": 1, "failed": 0},
  "results": [{"index": 0, "valid": true}]
}
```

### B) Execute success response example (`create_draft_jobs`)

```json
{
  "mode": "create_draft_jobs",
  "summary": {"requested": 1, "created": 1, "failed": 0},
  "results": [{"index": 0, "job_id": "<created_job_id>"}]
}
```

### C) Execute runtime-failure response example (`create_and_enqueue`)

```json
{
  "mode": "create_and_enqueue",
  "summary": {"requested": 1, "created": 1, "enqueued": 0, "noop": 0, "failed": 1},
  "results": [
    {
      "job_id": "<created_job_id>",
      "enqueue": {
        "job_id": "<created_job_id>",
        "error": {"code": "UIJ_INTERNAL", "message": "Internal error"}
      }
    }
  ]
}
```

### D) Execute mixed-result response example (`enqueue_existing_jobs`)

```json
{
  "mode": "enqueue_existing_jobs",
  "summary": {"requested": 2, "enqueued": 1, "noop": 0, "failed": 1},
  "results": [
    {"job_id": "<ok_job_id>", "enqueued": true},
    {"job_id": "<bad_job_id>", "error": {"code": "UIJ_RENDER_NOT_ALLOWED", "message": "Status not allowed"}}
  ]
}
```

## 4) Semantic evidence

- **Mode A create is atomic**: when payload contains a validation failure, summary reports `created: 0` and no new jobs are persisted.
- **Mode B create phase is atomic**: invalid create payload produces no created jobs (`created: 0`).
- **Mode B enqueue happens only after successful create**: successful Mode B run returns created+enqueued, and created job transitions to `READY_FOR_RENDER`; enqueue failure keeps created job in `DRAFT` and returns item-level enqueue error.
- **Mode C is non-atomic per-item**: mixed valid+invalid/exceptional job IDs return per-item results with partial success and `failed` counts.

## 5) Required test rerun evidence

### Command 1

```bash
python -m unittest tests.integration.test_jobs_bulk_json_api -v
```

Evidence:

- `Ran 4 tests in 1.973s`
- `OK`

### Command 2

```bash
python -m unittest discover -s tests -v
```

Evidence:

- `Ran 442 tests in 42.366s`
- `OK`

## 6) Files changed in this follow-up PR context

- `services/factory_api/app.py`
- `tests/integration/test_jobs_bulk_json_api.py`

(Polish-only update in this slice adds this handoff file for reviewer traceability.)
