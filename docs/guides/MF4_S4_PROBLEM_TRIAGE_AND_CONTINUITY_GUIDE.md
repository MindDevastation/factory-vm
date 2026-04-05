# Epic 7 MF4-S4 — Problem Triage and Next-Step Continuity Guide

Owner: Documentation Governance (Epic 7)
Primary doc class: GUIDE
Lifecycle/status: CANONICAL
Canonical-for: Problem triage and operator continuity links across scenarios
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Triage-oriented operator continuity guidance; does not replace runbook incident procedures.

## Triage mode taxonomy

- READ: gather evidence and classify problem type.
- ACTION: execute scoped operator response.
- RISKY_ACTION: broad-impact response requiring explicit rollback/safe-stop path.

## Triage decision map

| Problem signal | First action | Next-step continuity |
|---|---|---|
| Workflow handoff blocked | Check MF2 handoff requirements | Continue to publish/manual handoff guide |
| Publish/manual ambiguity | Check channel split and risk level | Continue to Telegram or Web path guide |
| Analytics anomaly | Validate context in analytics guide | Continue to planner/visual guide as needed |
| Unknown boundary conflict | Check MF3 domain boundary map | Escalate through canonical workflow review path |

## Task-to-task continuity links

- Planner continuity: `docs/guides/MF4_S2_PLANNER_GUIDE.md`
- Publish/manual continuity: `docs/guides/MF4_S2_PUBLISH_MANUAL_HANDOFF_GUIDE.md`
- Visual workflow continuity: `docs/guides/MF4_S2_VISUAL_WORKFLOW_GUIDE.md`
- Analytics continuity: `docs/guides/MF4_S2_ANALYTICS_CENTER_GUIDE.md`
- Telegram continuity: `docs/guides/MF4_S3_TELEGRAM_OPERATOR_GUIDE.md`

## Cross-links

- Workflow canon: `docs/workflows/MF2_S2_STAGE_CONTRACTS.md`
- Handoff matrix: `docs/workflows/MF2_S4_HANDOFF_MATRIX.md`
- Boundary map: `docs/reference/MF3_S4_DOMAIN_BOUNDARY_MAP.md`
