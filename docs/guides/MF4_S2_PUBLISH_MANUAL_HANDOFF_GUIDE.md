# Epic 7 MF4-S2 — Publish / Manual Handoff Guide

Owner: Documentation Governance (Epic 7)
Primary doc class: GUIDE
Lifecycle/status: CANONICAL
Canonical-for: Publish/manual handoff operator tasks
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Operator-facing task path; not a workflow contract replacement.

Mode taxonomy:
- READ: inspect publish queue state and handoff readiness.
- ACTION: execute approved publish/manual handoff action.
- RISKY_ACTION: batch publish/manual override requiring explicit rollback note.

Task flow:
1) READ queue + handoff readiness.
2) ACTION single approved handoff.
3) RISKY_ACTION batch/manual override only with explicit confirmation + recovery path.

Cross-links: `docs/workflows/MF2_S4_HANDOFF_MATRIX.md`, `docs/reference/MF3_S4_DOMAIN_BOUNDARY_MAP.md`.
