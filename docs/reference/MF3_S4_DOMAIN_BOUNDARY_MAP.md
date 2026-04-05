# Epic 7 MF3-S4 — Domain Boundary Map and Conflict Resolution Notes

Owner: Documentation Governance (Epic 7)
Primary doc class: REFERENCE_DOC
Lifecycle/status: CANONICAL
Canonical-for: Domain boundary lookup and explicit conflict resolution notes
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Boundary map documents accepted boundaries and ambiguity notes without reopening canon.

## Scope boundary (MF3-S4 only)

This slice provides:
- domain boundary map artifact,
- explicit boundary conflict resolution notes.

This slice does not redefine workflow canon, operator guide content, or ops runbook procedures.

## Domain boundary map

| Boundary pair | Primary canonical side | Secondary side | Boundary decision |
|---|---|---|---|
| Documentation governance vs workflow contracts | `docs/governance/*` + `docs/ssot/*` define policy/status/SSOT | `docs/workflows/*` define process contracts | Governance/SSOT own policy vocabulary; workflows consume it via cross-link |
| Workflow contracts vs operator guides | `docs/workflows/*` define stage contracts/handoffs | `docs/guides/*` (future MF4) defines task execution guidance | Workflow docs stay normative for process flow; guides remain task-oriented usage |
| Workflow contracts vs ops runbooks | `docs/workflows/*` define process-stage contracts | `docs/ops/runbook/*` defines ops execution playbooks | Runbooks are execution artifacts; workflow docs are process-contract artifacts |
| Reference docs vs runtime truth | runtime code anchors (e.g., `services/factory_api/app.py`) | `docs/reference/*` lookup layer | Runtime/code truth wins when mismatch appears; reference docs must align |

## Explicit conflict resolution notes

1. **Potential conflict:** docs claiming “main workflow” outside `docs/workflows/*`.
   - **Resolution:** treat as subordinate narrative; canonical workflow remains MF2 set.
2. **Potential conflict:** operator/procedure detail embedded into reference docs.
   - **Resolution:** keep reference docs lookup-oriented; move/point procedural detail to guides/runbooks.
3. **Potential conflict:** legacy status aliases (e.g., ACTIVE_CANONICAL) in older docs.
   - **Resolution:** canonical status literals follow Epic 7 normalized model; aliases remain traceable only.

## Cross-links

- MF1 SSOT map: `docs/ssot/MF1_S3_SSOT_MAP.md`
- MF2 workflow canon: `docs/workflows/MF2_S1_CANONICAL_WORKFLOW_STRUCTURE.md`
- MF3 reference anchors: `docs/reference/MF3_S3_API_REFERENCE_SURFACE.md`
