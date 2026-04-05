# Epic 7 MF3-S1 — Entity Ownership Reference

Owner: Documentation Governance (Epic 7)
Primary doc class: REFERENCE_DOC
Lifecycle/status: CANONICAL
Canonical-for: Entity ownership lookup baseline for Factory VM documentation set
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Reference-grade ownership map; does not redefine runtime semantics.

## Scope boundary (MF3-S1 only)

This artifact provides entity-to-owner lookup references.
It does not define state machine semantics (MF3-S2) or API/glossary references (MF3-S3).

## Entity ownership map

| Entity / Domain Surface | Primary owner team | Owner role | Canonical references |
|---|---|---|---|
| Epic 7 documentation canon | Documentation Governance | Spec Custodian | `docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt` |
| Epic 7 requirement bundle | Documentation Governance | Requirement Custodian | `docs/req/REQ_Epic_7_Full_Pack.txt` |
| Workflow canon (MF2) | Documentation Governance | Workflow Steward | `docs/workflows/MF2_S1_CANONICAL_WORKFLOW_STRUCTURE.md`, `docs/workflows/MF2_S2_STAGE_CONTRACTS.md` |
| Production ops runbook entry | Operations | On-call Maintainer | `docs/ops/runbook/README.md` |
| Runtime entry/overview docs | Platform Backend | Runtime Maintainer | `README.md` |
| Test process reference | QA Engineering | Test Process Owner | `TESTING.md` |

## Boundary clarifications

- This ownership table is documentation-reference ownership, not org chart HR authority.
- Where runtime code ownership differs, runtime truth prevails and should be documented in later MF3 slices without reopening accepted canon.

## Cross-links

- MF1 ownership baseline: `docs/ssot/MF1_S3_OWNERSHIP_MAP.md`
- MF1 SSOT map: `docs/ssot/MF1_S3_SSOT_MAP.md`
- MF2 workflow canon: `docs/workflows/MF2_S1_CANONICAL_WORKFLOW_STRUCTURE.md`
