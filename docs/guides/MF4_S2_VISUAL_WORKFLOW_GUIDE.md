# Epic 7 MF4-S2 — Visual Workflow Operator Guide

Owner: Documentation Governance (Epic 7)
Primary doc class: GUIDE
Lifecycle/status: CANONICAL
Canonical-for: Visual workflow operator tasks
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Usage-oriented operator view, distinct from workflow canon.

Mode taxonomy:
- READ: inspect visual pipeline status.
- ACTION: trigger allowed visual task operation.
- RISKY_ACTION: wide-impact visual operations requiring rollback/safe-stop note.

Task flow:
1) READ current visual state.
2) ACTION targeted visual operation.
3) RISKY_ACTION only with explicit blast-radius acknowledgment.

Cross-links: `docs/workflows/MF2_S1_CANONICAL_WORKFLOW_STRUCTURE.md`, `docs/reference/MF3_S2_STATE_LIFECYCLE_REFERENCE.md`.
