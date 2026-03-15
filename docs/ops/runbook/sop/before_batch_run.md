# SOP: Before Batch Run

## Purpose
Provide a repeatable pre-batch gate so operators start batch processing only when runtime is healthy.

## When to use
- Right before monthly/large production batch processing.
- Any time a new batch is about to start after environment/config changes.

## Preconditions
- You are in repo root on the production VM.
- Deployment-managed services are expected to be running (service commands are deployment-specific; confirm names/paths in `deploy/systemd/*.service`).
- Operator has access to local API endpoint `http://127.0.0.1:8080`.

## Steps
1. Run the production smoke gate (preferred path):
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
2. Confirm smoke exits with `exit_code=0` and `overall_status=OK`.
3. Check API health endpoint:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   ```
4. Check worker heartbeat endpoint:
   ```bash
   curl -fsS http://127.0.0.1:8080/v1/workers
   ```
5. If using systemd deployment artifacts, verify core service state:
   ```bash
   systemctl status factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
   ```
6. Start batch operations only after all checks above pass.

## Expected success result
- Smoke reports `OK`.
- `/health` and `/v1/workers` return successfully.
- Required services are active for the enabled production flow.

## Escalation / next document if failed
- For API failures, use `../playbooks/api_not_responding.md`.
- For missing/stale workers, use `../playbooks/worker_heartbeat_missing.md`.
- For disk warnings/critical results, use `../playbooks/low_disk_space.md`.
- For end-to-end scenario context, use `../monthly_batch_workflow.md`.
