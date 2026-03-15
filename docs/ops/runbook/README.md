# Factory VM Production Runbook (SPEC_OPS_RUNBOOK_SOP_v1.0)

This runbook package is the **operator entrypoint for production operations**.

## Scope

Use this package for routine production operation, verification, and incident response on Factory VM deployments.

## Audience

Primary audience: production operators/on-call maintainers responsible for daily checks, deploy verification, reboot/restore validation, and incident handling.

## Single source of truth (SSOT)

`docs/ops/runbook/` is the documentation **single source of truth for production operations**.

If another doc conflicts with this package, follow this runbook and update/deprecate the older doc.

### Operational truth order

1. Real current code/runtime behavior
2. `docs/ops/runbook/*` (this package)
3. Linked ops references (`docs/ops/*.md`)
4. Legacy/general docs (for example `README.md`, `docs/LAUNCH_GUIDE.md`, handoff docs)

## Preferred production path policy

- **Preferred production path** = scenario flow + SOPs in this runbook package.
- **Alternative/debug-only path** = local debugging or ad-hoc recovery methods outside this runbook.
- Use alternative/debug-only paths only when the preferred path is blocked, and record the reason in incident notes.

## Scenario navigation (start here)

- **First launch (preferred)** → [`initial_setup_and_launch.md`](./initial_setup_and_launch.md)
- **After deploy (preferred)** → [`post_deploy_verification.md`](./post_deploy_verification.md)
- **After reboot (preferred)** → [`post_reboot_verification.md`](./post_reboot_verification.md)
- **After restore (preferred)** → [`post_restore_verification.md`](./post_restore_verification.md)
- **Before batch (preferred)** → [`monthly_batch_workflow.md`](./monthly_batch_workflow.md)
- **Incident handling (preferred)** → [Incident playbooks](#incident-playbooks)

## SOPs

- [`sop/preflight_environment.md`](./sop/preflight_environment.md)
- [`sop/service_control_and_logs.md`](./sop/service_control_and_logs.md)
- [`sop/backup_and_restore_cli.md`](./sop/backup_and_restore_cli.md)
- [`sop/before_batch_run.md`](./sop/before_batch_run.md)
- [`sop/after_restore.md`](./sop/after_restore.md)
- [`sop/after_reboot.md`](./sop/after_reboot.md)
- [`sop/after_deploy.md`](./sop/after_deploy.md)
- [`sop/before_risky_maintenance.md`](./sop/before_risky_maintenance.md)
- [`sop/when_smoke_fails.md`](./sop/when_smoke_fails.md)
- [`sop/when_disk_warning_triggers.md`](./sop/when_disk_warning_triggers.md)
- [`sop/when_failed_or_stale_jobs_accumulate.md`](./sop/when_failed_or_stale_jobs_accumulate.md)

## Incident playbooks

- [`playbooks/smoke_check_failure.md`](./playbooks/smoke_check_failure.md)
- [`playbooks/api_not_responding.md`](./playbooks/api_not_responding.md)
- [`playbooks/worker_heartbeat_missing.md`](./playbooks/worker_heartbeat_missing.md)
- [`playbooks/stale_or_stuck_jobs.md`](./playbooks/stale_or_stuck_jobs.md)
- [`playbooks/repeated_job_failures.md`](./playbooks/repeated_job_failures.md)
- [`playbooks/low_disk_space.md`](./playbooks/low_disk_space.md)
- [`playbooks/token_or_integration_readiness_problem.md`](./playbooks/token_or_integration_readiness_problem.md)
- [`playbooks/backup_restore_verification_failure.md`](./playbooks/backup_restore_verification_failure.md)

## Linked ops references

These references are part of the operational doc set, but the runbook package above remains canonical for operator flow.

- Backup/restore docs: [`docs/ops/backup_restore.md`](../backup_restore.md)
- Smoke docs: [`docs/ops/production_smoke.md`](../production_smoke.md)
- Recovery console docs: [`recovery_console.md`](./recovery_console.md)
- Logging/retention docs: [`docs/ops/logging_retention.md`](../logging_retention.md)

## Deployment-specific command policy

Do not treat deployment-specific service commands/paths as globally canonical unless sourced from deployment artifacts.

Canonical sources in this repo:

- `deploy/systemd/*.service`
- `deploy/systemd/*.timer`
- `README.md` runtime command examples (only where explicit)
