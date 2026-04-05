# Epic 7 MF3-S3 — Glossary and Naming Reference

Owner: Documentation Governance (Epic 7)
Primary doc class: REFERENCE_DOC
Lifecycle/status: CANONICAL
Canonical-for: Documentation/reference naming normalization map
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Maps documented terms without changing accepted product semantics.

## Canonical terms

| Canonical term | Known aliases in repo docs | Notes |
|---|---|---|
| CANONICAL | ACTIVE_CANONICAL (legacy label) | v1.1 normalization uses CANONICAL |
| SUBORDINATE | secondary/reference | subordinate to canonical source |
| LEGACY_CANDIDATE | legacy-doc-candidate | explicit candidate marker before deprecation/archive |
| workflow canon | main workflow doc | single canonical workflow set under `docs/workflows/*` |
| runbook | ops playbook/SOP | runbook execution docs remain distinct from reference docs |

## Naming rules

- Use frozen Epic 7 status literals for lifecycle/status fields.
- Keep primary doc class taxonomy distinct from lifecycle/status terms.
- When legacy alias appears, prefer canonical term and retain traceable alias note if needed.

## Cross-links

- Status model: `docs/governance/MF6_S1_GOVERNANCE_STATUS_BASELINE.md`
- Taxonomy/status schema: `docs/ssot/MF1_S2_TAXONOMY_STATUS_OWNERSHIP_SCHEMA.md`
