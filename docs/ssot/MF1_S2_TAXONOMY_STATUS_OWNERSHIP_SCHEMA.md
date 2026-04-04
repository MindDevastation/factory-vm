# Epic 7 MF1-S2 — Taxonomy, Status, Ownership, and Canonical Header Schema

Owner: Documentation Governance (Epic 7)
Primary doc class: NORMATIVE_PRODUCT_DOC
Lifecycle/status: CANONICAL
Canonical-for: Epic 7 MF1-S2 schema baseline
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Mirrors MF1 + bundle v1.1 normalization clauses.

## 1) Frozen primary doc classes (taxonomy-only)

Use exactly one primary class per artifact:

- NORMATIVE_PRODUCT_DOC
- RUNBOOK
- WORKFLOW_DOC
- REFERENCE_DOC
- GUIDE
- ARCHIVE_RECORD

Rule: lifecycle markers (legacy/deprecated/archived) MUST NOT be encoded as primary class.

## 2) Frozen lifecycle/status literals (status-only)

Use exactly one canonical status per artifact:

- CANONICAL
- SUBORDINATE
- NON_CANONICAL
- LEGACY_CANDIDATE
- DEPRECATED
- ARCHIVED

Rule: status is separate from primary doc class.

## 3) Ownership schema (minimum required)

Every artifact record must include:

- `owner_team` (required)
- `owner_role` (required)
- `owner_contact` (optional)
- `review_cadence` (optional)

Every CANONICAL artifact MUST have non-empty owner_team + owner_role.

## 4) Canonical header block convention (normalized)

Every new/updated canonical Epic 7 artifact should include this header block:

```text
Owner: <team-or-owner>
Primary doc class: <NORMATIVE_PRODUCT_DOC|RUNBOOK|WORKFLOW_DOC|REFERENCE_DOC|GUIDE|ARCHIVE_RECORD>
Lifecycle/status: <CANONICAL|SUBORDINATE|NON_CANONICAL|LEGACY_CANDIDATE|DEPRECATED|ARCHIVED>
Canonical-for: <topic/domain/workflow scope>
Related canonical source: <path-or-none>
Last reviewed: <YYYY-MM-DD>
Evidence note: <brief evidence anchor>
```

## 5) Archive/deprecation handling

- Do not delete legacy docs silently.
- Mark lifecycle/state explicitly via status.
- Retain cross-reference to canonical replacement where applicable.

## 6) Validation expectations for next slices

- doc class value must be from frozen taxonomy set
- status value must be from frozen status set
- class/status separation must hold
- canonical entries must include ownership
