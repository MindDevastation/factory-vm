# Production smoke (`doctor.py production-smoke`)

`production-smoke` is a non-destructive operational readiness check for Factory VM.

## Entry points

- Human output:
  - `python scripts/doctor.py production-smoke --profile prod`
- JSON output:
  - `python scripts/doctor.py production-smoke --profile prod --json`

## What it checks

- **Critical**
  - Disk free space under `FACTORY_STORAGE_ROOT`
  - API `/health`
  - Worker heartbeat coverage from `/v1/workers`
- **Warning / readiness-only**
  - Google Drive credential loading
  - YouTube credential loading
  - Telegram client initialization

No smoke check sends Telegram messages, uploads YouTube videos, or changes Google Drive state.

## Runtime service expectations

Required worker roles are derived from the same runtime enablement logic used by `scripts/run_stack.py` (`_worker_roles`).

## Severity and result model

Per-check fields:

- `severity`: `critical | warning | info`
- `result`: `PASS | WARN | FAIL | SKIP`

Overall status rules:

- `FAIL` if any critical check is `FAIL`
- `WARNING` if there is no critical `FAIL`, but:
  - any warning check is `FAIL` or `WARN`, or
  - any info check is `FAIL`
- `OK` otherwise

## Exit codes

- `0` = `OK`
- `1` = `WARNING`
- `2` = `FAIL`
- `3` = `RUNNER_ERROR`

## Disk thresholds

- `WARN` if free disk is `< 15%` or `< 20 GiB`
- `FAIL` if free disk is `< 8%` or `< 10 GiB`

