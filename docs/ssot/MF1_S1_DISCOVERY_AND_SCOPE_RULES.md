# Epic 7 MF1-S1 — Documentation Discovery and In-Scope Filter Rules

Owner: Documentation Governance (Epic 7)
Primary doc class: NORMATIVE_PRODUCT_DOC
Lifecycle/status: CANONICAL
Canonical-for: Epic 7 MF1-S1 discovery boundary and in-scope filter policy
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Derived directly from Epic 7 bundle normalization and MF1 inventory scope clauses.

## Purpose

Define a deterministic, repo-local discovery rule set for Epic 7 MF1 inventory work.

## Discovery breadth rule (not shallow)

Discovery MUST include repository-tracked documentation artifacts across:

1. `docs/**` recursively (including `docs/spec/**` and `docs/req/**`)
2. Root-level documentation files (`*.md`, `*.txt`, `*.rst`)
3. Checked-in runbooks/guides/reference/audit/handoff docs outside strict taxonomy folders

## In-scope filter rules

An artifact is IN_SCOPE for MF1 inventory when all are true:

- It is stored in this repository.
- It is intended as project knowledge/documentation.
- It is one of:
  - `docs/**` text documentation artifact,
  - root-level project documentation file,
  - checked-in runbook/guide/reference/handoff/audit doc artifact.

## Default exclusions

Exclude by default unless explicitly checked in as documentation artifacts:

- PR descriptions/comments
- Chat transcripts
- External documents not stored in repo
- Generated build outputs

## Required output linkage for next slices

MF1-S1 outputs feed MF1-S2/MF1-S3:

- Path inventory skeleton (`MF1_S1_INVENTORY_MANIFEST.json`)
- Candidate topic coverage seeds for canonical mapping
- Initial signal tags for later owner/doc-class/status assignment

## Non-goals for MF1-S1

- No canonical-vs-subordinate final decisions
- No deprecation/archive final labeling
- No rewrite of existing product/runtime semantics
