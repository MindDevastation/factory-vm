# Epic 7 MF5-S2 — Deploy/Update Runbook (Ordered)

Owner: Documentation Governance (Epic 7)
Primary doc class: RUNBOOK
Lifecycle/status: CANONICAL
Canonical-for: Ordered deploy/update operations and monitoring checklist linkage
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Deploy/update mode only; incident/recovery handled in MF5-S3.

## Mode boundary

This runbook covers deploy/update operations and immediate ordered checks.
It does not cover incident response or deep recovery procedures.

## Ordered deploy/update sequence

1. Pre-deploy preflight: confirm env/config integrity and target revision.
2. Apply deployment/update package.
3. Restart/roll services in controlled order.
4. Run immediate smoke checks.
5. Run monitoring checklist and log outcomes.
6. If failure detected, halt progression and switch to incident/recovery runbooks.

## Ordered actionable checks

- API health endpoint responds.
- Worker heartbeat visibility is present.
- Critical queue/read endpoints are reachable.
- Authentication path works for operator access.

## Cross-links

- Existing post-deploy verification: `docs/ops/runbook/post_deploy_verification.md`
- Routine mode runbook: `docs/runbooks/MF5_S1_START_STOP_AND_ENV_RUNBOOK.md`
- Workflow canon: `docs/workflows/MF2_S2_STAGE_CONTRACTS.md`
