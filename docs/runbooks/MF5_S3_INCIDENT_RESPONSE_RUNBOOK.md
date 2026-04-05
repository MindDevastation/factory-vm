# Epic 7 MF5-S3 — Incident Response Runbook

Owner: Documentation Governance (Epic 7)
Primary doc class: RUNBOOK
Lifecycle/status: CANONICAL
Canonical-for: Incident response mode runbook
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Incident mode only; distinct from routine/deploy verification modes.

## Incident-mode trigger

Use this runbook when routine/deploy checks fail or service behavior indicates active incident risk.

## Ordered incident response steps

1. Declare incident mode and capture incident start context.
2. Stabilize intake (pause risky mutations if required).
3. Identify impacted surfaces (API/workers/operator channels).
4. Apply lowest-risk containment action.
5. Re-check health signals.
6. Escalate to recovery runbook if containment fails.

## Cross-links

- Recovery runbook: `docs/runbooks/MF5_S3_RECOVERY_RUNBOOK.md`
- Existing incident playbooks: `docs/ops/runbook/playbooks/*`
- Workflow handoff reference: `docs/workflows/MF2_S4_HANDOFF_MATRIX.md`
