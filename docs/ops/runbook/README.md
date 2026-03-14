# Factory VM Production Runbook / SOP (SPEC_OPS_RUNBOOK_SOP_v1.0)

This package is the operational single source of truth for production operator workflows on VPS.

## Operational truth order

1. Real current code/runtime behavior
2. `docs/ops/runbook/*`
3. Other linked ops docs
4. `README.md`, `docs/LAUNCH_GUIDE.md`, and older docs

If a document outside this package conflicts with this runbook, follow this runbook and update/deprecate the older doc.

## Scenario index (preferred production path)

- Initial setup + launch: [`initial_setup_and_launch.md`](./initial_setup_and_launch.md)
- Daily checks (standard operator start of day): [`daily_checks.md`](./daily_checks.md)
- Post-deploy verification: [`post_deploy_verification.md`](./post_deploy_verification.md)
- Post-reboot verification: [`post_reboot_verification.md`](./post_reboot_verification.md)
- Post-restore verification: [`post_restore_verification.md`](./post_restore_verification.md)
- Monthly batch workflow: [`monthly_batch_workflow.md`](./monthly_batch_workflow.md)

## SOPs

- [`sop/preflight_environment.md`](./sop/preflight_environment.md)
- [`sop/service_control_and_logs.md`](./sop/service_control_and_logs.md)
- [`sop/backup_and_restore_cli.md`](./sop/backup_and_restore_cli.md)

## Incident playbooks

- [`playbooks/api_unhealthy.md`](./playbooks/api_unhealthy.md)
- [`playbooks/worker_stalled.md`](./playbooks/worker_stalled.md)
- [`playbooks/disk_pressure_retention.md`](./playbooks/disk_pressure_retention.md)

## No-invented-commands policy

Only use commands/routes/paths that exist in this repository or deployment artifacts.

For deployment-specific service control paths, use the deployment-configured path/command defined in:

- `deploy/systemd/*.service`
- `deploy/systemd/*.timer`

## Notes on current implementation limits

- Dedicated systemd unit files for `track_jobs` worker are not present in `deploy/systemd/` at the time of this runbook; use the runtime command from `README.md` (`python -m services.workers --role track_jobs`) when needed.
- Automatic stop/start orchestration around backup restore is not implemented; restore requires manual service stop/start and guard-file handling.
