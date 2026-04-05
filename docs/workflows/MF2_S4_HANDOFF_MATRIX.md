# Epic 7 MF2-S4 — Handoff Matrix and Gate Rules

Owner: Documentation Governance (Epic 7)
Primary doc class: WORKFLOW_DOC
Lifecycle/status: CANONICAL
Canonical-for: MF2-S4 sender/receiver matrix with entry/exit gating and artifact package explicitness
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Matrix-level handoff formalization; does not include competing-canon cleanup (MF2-S5).

## Scope boundary (MF2-S4 only)

This slice defines:
- handoff matrix,
- gating / entry / exit rules,
- sender / receiver / artifact package explicitness.

## Handoff matrix

| Handoff ID | From | To | Sender role | Receiver role | Required artifact package |
|---|---|---|---|---|---|
| H1 | DISCOVERY | REQ | Discovery Owner | Requirement Owner | discovery summary, candidate scope, open questions |
| H2 | REQ | SPEC | Requirement Owner | Spec Owner | canonical req artifact, acceptance criteria |
| H3 | SPEC | DEV | Spec Owner | Implementer | canonical spec artifact, scope boundary, non-goals |
| H4 | DEV | REVIEW | Implementer | Reviewer | scoped diff, validation evidence, known limitations |
| H5 | REVIEW | QA | Reviewer | QA Owner | review verdict, fix list or waiver note |
| H6 | QA | RELEASE_DECISION | QA Owner | Release/Operator Owner | QA verdict, evidence bundle, release recommendation |

## Gate rules (normative)

- Entry gate: receiver stage may begin only when required artifact package is complete and linked.
- Exit gate: sender stage is complete only when handoff package is acknowledged by receiver.
- Missing mandatory artifact item blocks handoff progression.
- Handoff package must include sender/receiver role identity explicitly.

## Cross-links

- MF2-S2 contracts: `docs/workflows/MF2_S2_STAGE_CONTRACTS.md`
- MF2-S3 package schema: `docs/workflows/MF2_S3_HANDOFF_PACKAGE_SCHEMA.json`
- MF1 ownership baseline: `docs/ssot/MF1_S3_OWNERSHIP_MAP.md`
