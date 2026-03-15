# SOP: After Deploy

## Purpose
Confirm a deployment is production-ready before handing back to normal workload processing.

## When to use
- Immediately after code/config deploy to production VM.

## Preconditions
- Deployment command completed.
- Service restart method/path is known from deployment artifacts (for example `deploy/systemd/*.service`).

## Steps
1. Restart deployed services with deployment-specific service manager commands.
2. Verify services are active (systemd deployment example):
   ```bash
   systemctl status factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
   ```
3. Run production smoke:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
4. Verify API health:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   ```
5. Verify workers endpoint:
   ```bash
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
   ```
6. If checks pass, mark deploy verification complete in operator notes.

## Expected success result
- Smoke returns `OK` with `exit_code=0`.
- API and worker endpoints are reachable.
- Required services are active for enabled flows.

## Escalation / next document if failed
- Use `../playbooks/api_not_responding.md` for API issues.
- Use `../playbooks/worker_heartbeat_missing.md` for worker/readiness issues.
- Use `../post_deploy_verification.md` for full scenario procedure.
