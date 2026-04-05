# Epic 7 MF6-S3 — Deprecation and Archive Policy

Owner: Documentation Governance (Epic 7)
Primary doc class: NORMATIVE_PRODUCT_DOC
Lifecycle/status: CANONICAL
Canonical-for: Explicit deprecation/archive marking and traceability policy
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Makes deprecation/archive explicit and traceable.

## Explicit marking rules

- Deprecation/archive state must be explicit in artifact status.
- `LEGACY_CANDIDATE`, `DEPRECATED`, `ARCHIVED` must never be implicit.
- Deprecated/archived artifacts must point to canonical replacement if available.

## Traceability rules

- Keep artifact path traceable after status transition.
- Record transition reason and date in governing registry artifact.
- Do not silently delete legacy/deprecated artifacts unless explicit archival policy action is recorded.

## Cross-links

- Legacy candidate registry: `docs/ssot/MF1_S4_LEGACY_DEPRECATION_CANDIDATES.md`
- Status baseline: `docs/governance/MF6_S1_GOVERNANCE_STATUS_BASELINE.md`
