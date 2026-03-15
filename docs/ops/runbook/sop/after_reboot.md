# SOP: After Reboot

## Purpose
Provide a fast, consistent post-reboot validation before returning the VM to normal production use.

## When to use
- After VM reboot, host restart, or service-manager restart event.

## Preconditions
- Reboot is complete and operator shell access is restored.
- Deployment service names/commands are known from deployment artifacts.

## Steps
1. Verify core service status (systemd deployment example):
   ```bash
   systemctl status factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
   ```
2. Run production smoke:
   ```bash
   python scripts/doctor.py production-smoke --profile prod
   ```
3. Verify API endpoint:
   ```bash
   curl -fsS http://127.0.0.1:8080/health
   ```
4. Verify worker heartbeat endpoint:
   ```bash
   curl -fsS http://127.0.0.1:8080/v1/workers
   ```
5. If optional importer/bot flow is enabled, verify related service status using deployment-specific service names.
6. Resume normal operations only after all checks pass.

## Expected success result
- Required services are active.
- Smoke returns `OK`.
- API and worker endpoints are healthy.

## Escalation / next document if failed
- Use `../playbooks/api_not_responding.md` for API failures.
- Use `../playbooks/worker_heartbeat_missing.md` for missing/stale worker roles.
- Use `../playbooks/low_disk_space.md` for disk warning/critical conditions.
- Full scenario reference: `../post_reboot_verification.md`.
