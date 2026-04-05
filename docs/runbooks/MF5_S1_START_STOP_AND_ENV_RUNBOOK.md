# Epic 7 MF5-S1 — Start/Stop and Environment Configuration Runbook

Owner: Documentation Governance (Epic 7)
Primary doc class: RUNBOOK
Lifecycle/status: CANONICAL
Canonical-for: Routine start/stop operations and env/config reference baseline
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Routine-operation runbook only; verification/incident/recovery are separate modes.

## Mode boundary

This runbook covers **routine operation mode** only:
- service start
- service stop
- baseline environment/config reference

Not covered here:
- post-change verification (MF5-S2+)
- incident response (MF5-S3)
- recovery flows (MF5-S3)

## Ordered routine start sequence

1. Confirm environment file availability (`deploy/env` or equivalent deployment env source).
2. Start API/service manager unit for factory API.
3. Start required worker roles for routine operation.
4. Start bot/service integration components used in normal operation.
5. Confirm basic service responsiveness before routine workload intake.

## Ordered routine stop sequence

1. Pause new workload intake if supported.
2. Stop bot/integration components to prevent new operator-triggered actions.
3. Stop worker roles gracefully.
4. Stop API/service manager unit.
5. Record stop reason and timestamp in operator notes.

## Environment/config reference (ops)

- Runtime env source: deployment-managed env file (`deploy/env` or equivalent).
- API auth variables: `FACTORY_BASIC_AUTH_USER`, `FACTORY_BASIC_AUTH_PASS`.
- Optional runtime toggles and feature flags should be documented in deployment config and reviewed before applying.

## Routine-mode clarity

- Routine mode assumes no active incident or recovery condition.
- If any incident/recovery trigger appears, switch to incident/recovery runbooks instead of extending routine steps.

## Cross-links

- Existing ops entrypoint: `docs/ops/runbook/README.md`
- Workflow stage contracts: `docs/workflows/MF2_S2_STAGE_CONTRACTS.md`
- Operator guide baseline: `docs/guides/MF4_S1_OPERATOR_GUIDE_TEMPLATE.md`
