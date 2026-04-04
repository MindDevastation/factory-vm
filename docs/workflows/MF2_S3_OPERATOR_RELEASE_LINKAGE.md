# Epic 7 MF2-S3 — Operator/Release Workflow Linkage and Handoff Package Normalization

Owner: Documentation Governance (Epic 7)
Primary doc class: WORKFLOW_DOC
Lifecycle/status: CANONICAL
Canonical-for: MF2-S3 linkage of core workflow stages to operator/release execution paths
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Linkage-only slice; does not replace MF4 operator guides or MF5 ops runbooks.

## Scope boundary (MF2-S3 only)

This slice defines:
- operator workflow linkage,
- release/publish workflow linkage,
- normalized handoff package shape.

This slice does not define full handoff matrix (MF2-S4) or competing-canon cleanup closure (MF2-S5).

## Operator workflow linkage (explicit)

Canonical process stages connect to operator surfaces as follows:

- DEV/REVIEW/QA outcomes that require operator action link to operator execution surfaces in `docs/ops/runbook/*`.
- Operator-facing usage guidance itself remains in MF4 guide artifacts (out of scope for this slice).

## Release/publish workflow linkage (explicit)

- Workflow stages that produce publish/release decisions must reference publish controls and queue semantics as workflow-linked artifacts.
- This linkage is structural only; detailed product semantics remain in canonical reference/spec artifacts.

## Normalized handoff package shape

Every cross-stage handoff package should include:

- `handoff_id`
- `from_stage`
- `to_stage`
- `sender_role`
- `receiver_role`
- `artifact_bundle` (paths)
- `decision_or_request`
- `entry_gate_evidence`
- `exit_expectation`
- `notes`

## Cross-links (avoid SSOT duplication)

- MF1 ownership map: `docs/ssot/MF1_S3_OWNERSHIP_MAP.md`
- MF1 SSOT map: `docs/ssot/MF1_S3_SSOT_MAP.md`
- MF2-S2 stage contracts: `docs/workflows/MF2_S2_STAGE_CONTRACTS.md`
