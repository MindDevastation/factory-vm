# Epic 7 MF3-S3 — API / Reference Surface Consolidation

Owner: Documentation Governance (Epic 7)
Primary doc class: REFERENCE_DOC
Lifecycle/status: CANONICAL
Canonical-for: Consolidated API/reference surface index for project-level lookup
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Reference index only; does not redefine API behavior.

## Scope boundary (MF3-S3 only)

This artifact indexes API/reference surfaces and naming vocabulary anchors.
It does not modify runtime API contracts.

## Reference surfaces

| Surface | Canonical anchor | Notes |
|---|---|---|
| Runtime/API behavior truth | `services/factory_api/app.py` | runtime truth anchor for operational behavior |
| Workflow contract reference | `docs/workflows/MF2_S2_STAGE_CONTRACTS.md` | stage contract semantics |
| Ops runbook operational entry | `docs/ops/runbook/README.md` | operator flow execution reference |
| Documentation status/governance | `docs/governance/MF6_S1_GOVERNANCE_STATUS_BASELINE.md` | doc lifecycle/status baseline |
| SSOT / ownership registry | `docs/ssot/MF1_S3_SSOT_MAP.md` and `docs/ssot/MF1_S3_OWNERSHIP_MAP.md` | canonical doc source mapping |

## API/reference naming posture

- Keep naming stable where accepted canon already exists.
- Where aliases exist across docs, map aliases to canonical terms in glossary artifact.
- Avoid introducing new semantic meaning through naming-only edits.

## Cross-link

- Glossary/naming artifact: `docs/reference/MF3_S3_GLOSSARY_AND_NAMING.md`
