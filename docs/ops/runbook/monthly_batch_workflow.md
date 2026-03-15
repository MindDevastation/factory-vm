# Monthly Batch Production Checklist

Preferred production path: run the monthly batch only on the deployment-managed runtime path (systemd units from `deploy/systemd/*.service`).

## 1) Preflight (all required before GO)

- [ ] **Backup freshness check (GO/STOP)**
  - [ ] List backups and confirm at least one recent `SUCCESS` snapshot for the environment:
    ```bash
    PYTHONPATH=. python scripts/ops_backup_restore.py backup list
    ```
  - [ ] Verify the selected backup before batch:
    ```bash
    PYTHONPATH=. python scripts/ops_backup_restore.py backup verify --backup-id <backup_id>
    ```
  - **GO**: recent backup exists and verify passes.
  - **STOP/HOLD**: no recent successful backup or verify fails.

- [ ] **Smoke pass (GO/STOP)**
  - [ ] Run:
    ```bash
    python scripts/doctor.py production-smoke --profile prod
    ```
  - **GO**: `exit_code=0`, `overall_status=OK`.
  - **STOP/HOLD**: any FAIL/WARN not explicitly accepted.

- [ ] **Recovery queue / recovery console pass (GO/STOP)**
  - [ ] Check worker and queue freshness signal:
    ```bash
    curl -fsS http://127.0.0.1:8080/v1/workers
    ```
  - [ ] If stale/failed jobs are already accumulating, run triage SOP before launch:
    - `sop/when_failed_or_stale_jobs_accumulate.md`
    - `playbooks/worker_stalled.md`
  - **GO**: required roles are fresh and no unresolved critical stale/failure backlog.
  - **STOP/HOLD**: unresolved stale/failed jobs in critical roles or unknown queue behavior.

- [ ] **Disk / logs / retention check (GO/STOP)**
  - [ ] Run retention scan:
    ```bash
    python scripts/ops_retention.py scan
    ```
  - [ ] If needed, run retention cleanup and re-check smoke:
    ```bash
    python scripts/ops_retention.py run
    python scripts/doctor.py production-smoke --profile prod
    ```
  - **GO**: no critical disk pressure; smoke remains OK.
  - **STOP/HOLD**: critical disk pressure persists.

- [ ] **Planner / release / jobs sanity check (GO/STOP)**
  - [ ] Confirm current release services are active (systemd deployment path):
    ```bash
    systemctl status factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
    ```
  - [ ] Confirm API health is responsive:
    ```bash
    curl -fsS http://127.0.0.1:8080/health
    ```
  - **GO**: required services active and API health responds.
  - **STOP/HOLD**: service instability or unhealthy API.

- [ ] **Integration readiness check (if integration path enabled)**
  - [ ] Verify optional flow services only if used in this batch (for example importer/bot):
    ```bash
    systemctl status factory-importer.service factory-bot.service
    ```
  - **GO**: optional integrations needed for this batch are healthy.
  - **STOP/HOLD**: required optional integration is unhealthy.

## 2) Launch sequence (preferred order)

Use only the real supported launch path for production: deployment-managed services.

1. [ ] Confirm preflight is fully green (all GO criteria met).
2. [ ] Ensure required units are enabled/running (deployment-specific install path; systemd unit names from `deploy/systemd/*.service`).
3. [ ] Start/continue monthly batch through normal production control path (no debug/manual worker one-offs).
4. [ ] Keep `journalctl` and API endpoints open for immediate watch.

Do **not** treat `scripts/run_stack.py` or single worker `--once` commands as standard production launch.

## 3) Post-launch monitoring (first 15-30 minutes)

- [ ] Re-check health quickly after launch:
  ```bash
  curl -fsS http://127.0.0.1:8080/health
  curl -fsS http://127.0.0.1:8080/v1/workers
  python scripts/doctor.py production-smoke --profile prod
  ```
- [ ] Watch service logs for repeated hard failures/timeouts:
  ```bash
  journalctl -u factory-api.service -u factory-orchestrator.service -u factory-qa.service -u factory-uploader.service -u factory-cleanup.service -n 200 --no-pager
  ```

### Early warning signals

- Repeated worker restarts/crashes.
- Smoke moving from OK to WARN/FAIL.
- Growing failed/stale jobs without identified cause.
- Disk pressure increasing during batch.

### Pause and investigate when

- Any smoke FAIL appears.
- Critical disk pressure is detected.
- Critical-role worker heartbeat is stale/missing.
- Repeated job failures continue for >2 cycles with unknown cause.

## 4) Abort / hold criteria (explicit)

Place batch in **HOLD** and investigate before continuing if any of the following occurs:

- [ ] Smoke FAIL.
- [ ] Critical disk pressure not resolved by retention workflow.
- [ ] Unresolved stale jobs in critical roles.
- [ ] Repeated failing jobs without understood root cause.

Use incident paths immediately:
- API issues -> `playbooks/api_unhealthy.md`
- Worker/queue issues -> `playbooks/worker_stalled.md` and `sop/when_failed_or_stale_jobs_accumulate.md`
- Disk pressure -> `playbooks/disk_pressure_retention.md`

## Source anchors

Operator actions above are anchored to current repo sources:

- Smoke command: `scripts/doctor.py`, `docs/ops/production_smoke.md`
- Backup list/verify: `scripts/ops_backup_restore.py`, `docs/ops/backup_restore.md`
- Retention scan/run: `scripts/ops_retention.py`, `docs/ops/logging_retention.md`
- Health/worker endpoints: `README.md`, `docs/ops/runbook/sop/before_batch_run.md`
- Service-managed production path: `deploy/systemd/*.service`, `docs/ops/runbook/initial_setup_and_launch.md`
