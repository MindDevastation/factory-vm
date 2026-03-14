# Production smoke (`doctor.py production-smoke`)

## 1) What smoke is for

Production smoke is a **non-destructive readiness gate** for operators. It summarizes core runtime signals so you can decide whether it is safe to start production jobs.

The gate includes a critical `pipeline_readiness` check that combines:
- DB accessibility
- API reachability
- Required worker-role readiness
- Runtime storage path availability
- Render dependency availability (`ffmpeg`)
- Planner readiness signals when planner is enabled
- Track catalog/analyze worker-role impact when that flow is enabled

It does not perform uploads, message sends, or heavy render operations.

## 2) When to run it

Run smoke in these operator moments:
- **post-deploy** (after new code/config rollout)
- **post-reboot** (after VM/service restart)
- **post-restore** (after backup/restore operations)
- **pre-batch-run** (before scheduling/starting production job batches)

## 3) Command examples

Human-readable output:

```bash
python scripts/doctor.py production-smoke --profile prod
```

JSON output to stdout:

```bash
python scripts/doctor.py production-smoke --profile prod --json
```

JSON output to file:

```bash
python scripts/doctor.py production-smoke --profile prod --json --json-out /tmp/production-smoke.json
```

Run a targeted subset of checks:

```bash
python scripts/doctor.py production-smoke --profile prod --checks runner_bootstrap,pipeline_readiness
```

## 4) JSON output example

```json
{
  "schema_version": "factory_production_smoke/1",
  "profile": "prod",
  "overall_status": "OK",
  "exit_code": 0,
  "checks": [
    {
      "check_id": "pipeline_readiness",
      "severity": "critical",
      "result": "PASS",
      "details": {
        "planner_enabled": true,
        "uploader_ready": true,
        "workers_ready": true,
        "db_ready": true,
        "storage_ready": true,
        "render_dependency_ready": true,
        "integration_blockers": []
      }
    }
  ]
}
```

## 5) Exit code meanings

- `0` = `OK`
- `1` = `WARNING`
- `2` = `FAIL`
- `3` = `RUNNER_ERROR`

## 6) Common failure classes and first operator actions

- **DB/access failure** (`db_access`, `pipeline_readiness`):
  - verify DB file path, file existence, and filesystem permissions
  - run DB quick validation / restore procedure if corrupted
- **API unreachable** (`api_health`):
  - verify API process is running and bound to expected host/port
  - check local firewall/bind mismatch
- **Worker role gaps/stale heartbeats** (`required_runtime_roles`, `worker_heartbeat`):
  - confirm all required worker services are running
  - check stale worker logs and restart unhealthy workers
- **Storage/runtime path failures** (`storage_paths`):
  - create/mount missing directories
  - fix ownership/permission problems
- **Render dependency failure** (`ffmpeg_available`):
  - install/repair ffmpeg on host
- **Integration readiness issues** (`youtube_ready`, `gdrive_ready`, `telegram_ready`):
  - validate token/credential files and profile-appropriate configs

## 7) Profile-aware required services explanation

Required worker roles are resolved using the same runtime role logic used by runtime startup.

- **prod profile**: critical roles must be present and fresh (for enabled flows)
- **local profile**: roles are generally optional for smoke strictness

Feature toggles also affect required roles:
- If track catalog/analyze flow is enabled, `track_jobs` contributes to readiness.
- If optional subsystems are disabled, those roles move out of required status.

## 8) Disk threshold policy

`disk_space` is policy-driven:
- `WARN` if free space is `< 15%` **or** `< 20 GiB`
- `FAIL` if free space is `< 8%` **or** `< 10 GiB`

The check evaluates relevant runtime mounts and reports per-path status.

## Smoke runner logging and diagnostics

The smoke CLI logs (without leaking secrets):
- run start
- resolved profile
- total duration
- overall status
- failed check IDs
- warning check IDs
- JSON output path when `--json-out` is used

These diagnostics are emitted to standard logging output and do not alter the smoke JSON schema.
