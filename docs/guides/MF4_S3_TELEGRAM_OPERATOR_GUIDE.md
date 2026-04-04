# Epic 7 MF4-S3 — Telegram Operator Guide (Web vs Telegram Split)

Owner: Documentation Governance (Epic 7)
Primary doc class: GUIDE
Lifecycle/status: CANONICAL
Canonical-for: Telegram-specific operator task guidance with explicit channel split
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Channel-specific guidance only; does not replace workflow canon or runbooks.

## Scope boundary (MF4-S3 only)

This guide covers Telegram operator usage and explicit split versus web UI usage.
It does not absorb deep ops recovery/deploy content.

## Mode taxonomy

- READ: view notifications, status summaries, and actionable prompts.
- ACTION: execute single operator command from Telegram controls.
- RISKY_ACTION: batch or high-impact action from Telegram requiring explicit confirmation.

## Web vs Telegram split (explicit)

| Task type | Preferred channel | Why |
|---|---|---|
| Quick status check | Telegram | low-friction read surface |
| Single acknowledged action | Telegram | fast operator response path |
| Bulk/high-risk action | Web UI (preferred) | better context and confirmation depth |
| Deep triage/navigation | Web UI | richer context and multi-surface detail |

Rule: if ambiguity exists for risk level, use Web UI path.

## Task flow

1) READ Telegram notification/context.
2) ACTION only when command scope is clear.
3) RISKY_ACTION requires explicit confirmation and web fallback option.

## Cross-links

- Workflow contracts: `docs/workflows/MF2_S2_STAGE_CONTRACTS.md`
- Handoff matrix: `docs/workflows/MF2_S4_HANDOFF_MATRIX.md`
- Reference boundaries: `docs/reference/MF3_S4_DOMAIN_BOUNDARY_MAP.md`
