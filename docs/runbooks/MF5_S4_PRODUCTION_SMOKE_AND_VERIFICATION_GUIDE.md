# Epic 7 MF5-S4 — Production Smoke and Verification Guide

Owner: Documentation Governance (Epic 7)
Primary doc class: RUNBOOK
Lifecycle/status: CANONICAL
Canonical-for: Production smoke/verification mode guidance with explicit runtime-tail honesty
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Verification mode guide only; does not claim unresolved runtime tails are closed.

## Mode separation (explicit)

- **Routine mode:** normal start/stop and regular operation (`MF5_S1`).
- **Verification mode:** smoke + validation after change/maintenance (`this guide`).
- **Incident mode:** active failure handling (`MF5_S3 incident`).
- **Recovery mode:** restoration after incident (`MF5_S3 recovery`).

## Ordered verification sequence

1. Run production smoke checks for API/worker/operator surfaces.
2. Confirm critical path readiness signals.
3. Compare observed behavior with expected verification outcomes.
4. If mismatch appears, stop verification path and switch to incident mode.
5. Record verification evidence and unresolved runtime tails.

## Runtime-tail honesty pass

- Verification pass indicates observed checks passed at verification time only.
- Verification pass does **not** prove all latent runtime defects are closed.
- Any unresolved or uncertain behavior must be recorded explicitly as open runtime tail.

## Cross-links

- Routine runbook: `docs/runbooks/MF5_S1_START_STOP_AND_ENV_RUNBOOK.md`
- Deploy/update runbook: `docs/runbooks/MF5_S2_DEPLOY_UPDATE_RUNBOOK.md`
- Incident/recovery runbooks: `docs/runbooks/MF5_S3_INCIDENT_RESPONSE_RUNBOOK.md`, `docs/runbooks/MF5_S3_RECOVERY_RUNBOOK.md`
- Existing ops smoke reference: `docs/ops/production_smoke.md`
