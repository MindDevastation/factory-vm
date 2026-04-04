# Epic 7 MF4-S1 — Operator Guide Template (Read / Action / Risky Action)

Owner: Documentation Governance (Epic 7)
Primary doc class: GUIDE
Lifecycle/status: CANONICAL
Canonical-for: MF4-S1 operator guide template and task-oriented structure
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Template-level guide structure; scenario-specific guides are future MF4 slices.

## Scope boundary (MF4-S1 only)

This slice defines the operator guide template structure only.
It does not provide full scenario guides (MF4-S2+), nor deep ops runbook procedures (MF5).

## Guide mode taxonomy

- **READ**: informational lookup, no system mutation.
- **ACTION**: operator step that mutates system state but is routine/reversible.
- **RISKY_ACTION**: operator step with elevated blast radius requiring explicit checks and rollback note.

## Task-oriented guide structure (template)

1. **Purpose and trigger**
2. **Mode** (`READ` / `ACTION` / `RISKY_ACTION`)
3. **Preconditions**
4. **Inputs/artifacts required**
5. **Steps**
6. **Expected result**
7. **Failure signals**
8. **Rollback / safe-stop notes** (mandatory for `RISKY_ACTION`)
9. **Cross-links to canonical references/runbooks/workflow artifacts**

## Minimal template block

```text
Guide ID:
Mode: READ|ACTION|RISKY_ACTION
Purpose:
Trigger:
Preconditions:
Inputs:
Steps:
Expected result:
Failure signals:
Rollback/Safe-stop:
Canonical cross-links:
```

## Cross-links

- Workflow contract baseline: `docs/workflows/MF2_S2_STAGE_CONTRACTS.md`
- Domain boundary map: `docs/reference/MF3_S4_DOMAIN_BOUNDARY_MAP.md`
- Ops execution entrypoint: `docs/ops/runbook/README.md`
