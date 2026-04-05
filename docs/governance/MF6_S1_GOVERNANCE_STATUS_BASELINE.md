# Epic 7 MF6-S1 — Governance/Status Baseline

Owner: Documentation Governance (Epic 7)
Primary doc class: NORMATIVE_PRODUCT_DOC
Lifecycle/status: CANONICAL
Canonical-for: MF6-S1 governance baseline only (not governance closeout)
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Establishes baseline vocabulary and minimum obligations for later MF6 slices.

## Scope boundary

This slice defines baseline governance vocabulary and marking obligations only.
It does **not** close MF6 governance policy.

## Frozen documentation status vocabulary

- CANONICAL
- SUBORDINATE
- NON_CANONICAL
- LEGACY_CANDIDATE
- DEPRECATED
- ARCHIVED

## Drift-control vocabulary baseline

- `canonical_source` — artifact designated as authoritative for a topic/domain/workflow.
- `subordinate_source` — supporting artifact aligned to canonical source.
- `drift_event` — observed inconsistency between canonical source and related artifacts.
- `drift_resolution_needed` — drift event requiring explicit correction path.
- `evidence_anchor` — in-repo pointer supporting a documentation claim.

## Deprecation/archive marking baseline

- Legacy/deprecation/archive state must be explicit in artifact status.
- Legacy artifacts are retained with traceability (no silent deletion).
- Deprecated/archived artifacts should include related canonical replacement when available.

## Canonical update obligation baseline

When a canonical artifact changes in ways affecting subordinate/related docs:

1. update related artifacts in the same slice/PR where practical, or
2. record explicit follow-up with evidence anchor and target artifact list.

## Deferred to later MF6 slices

- full review/approval workflow
- traceability enforcement policy details
- ownership governance closeout
- drift-resolution process closure
