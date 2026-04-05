# Epic 7 MF5-S3 — Recovery Runbook

Owner: Documentation Governance (Epic 7)
Primary doc class: RUNBOOK
Lifecycle/status: CANONICAL
Canonical-for: Recovery mode runbook for post-incident restoration
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Recovery mode only; does not redefine deployment process canon.

## Recovery-mode trigger

Use when incident containment is insufficient and service restoration requires explicit recovery flow.

## Ordered recovery steps

1. Confirm recovery prerequisites and data safety checkpoints.
2. Execute recovery action set (service/data/system as applicable).
3. Run post-recovery verification checks.
4. Re-enable routine operations only after verification pass.
5. Record recovery evidence and unresolved follow-ups.

## Cross-links

- Existing post-restore verification: `docs/ops/runbook/post_restore_verification.md`
- Routine start/stop runbook: `docs/runbooks/MF5_S1_START_STOP_AND_ENV_RUNBOOK.md`
- Deploy/update runbook: `docs/runbooks/MF5_S2_DEPLOY_UPDATE_RUNBOOK.md`
