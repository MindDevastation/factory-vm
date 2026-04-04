# Epic 7 MF2-S1 — Canonical Workflow Structure (Baseline)

Owner: Documentation Governance (Epic 7)
Primary doc class: WORKFLOW_DOC
Lifecycle/status: CANONICAL
Canonical-for: MF2-S1 workflow structure, stage inventory, and expectation matrix skeleton
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Structure-only baseline for MF2; detailed stage contracts deferred to MF2-S2.

## Scope boundary (MF2-S1 only)

This slice defines:
- canonical workflow artifact structure,
- stage inventory,
- artifact expectation matrix skeleton.

This slice does not define full stage contracts or full handoff matrix.

## Canonical workflow artifact structure

- `docs/workflows/MF2_S1_CANONICAL_WORKFLOW_STRUCTURE.md` (this baseline structure doc)
- `docs/workflows/MF2_S1_ARTIFACT_EXPECTATION_MATRIX.json` (machine-readable matrix skeleton)
- `docs/workflows/MF2_S2_STAGE_CONTRACTS.md` (next-slice contract detail target)

## Stage inventory (normative order)

1. DISCOVERY
2. REQ
3. SPEC
4. DEV
5. REVIEW
6. QA

## Artifact expectation matrix skeleton (normative, minimal)

The matrix skeleton uses one row per stage with fields:

- `stage`
- `required_inputs`
- `required_outputs`
- `entry_gate`
- `exit_gate`
- `notes`

Values are intentionally skeletal in MF2-S1 and become contract-level in MF2-S2.

## Non-duplication rule

MF2 workflow artifacts must reference MF1 outputs instead of duplicating ownership/SSOT logic:

- `docs/ssot/MF1_S3_OWNERSHIP_MAP.md`
- `docs/ssot/MF1_S3_SSOT_MAP.md`
- `docs/ssot/MF1_S4_COMPLETENESS_AND_GAP_REPORT.md`

## Anti-conflict rule

No additional “main workflow canon” may be introduced in parallel to this structure baseline.
