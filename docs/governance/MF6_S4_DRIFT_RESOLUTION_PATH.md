# Epic 7 MF6-S4 — Drift Resolution Path (Code vs Frozen REQ vs Docs)

Owner: Documentation Governance (Epic 7)
Primary doc class: NORMATIVE_PRODUCT_DOC
Lifecycle/status: CANONICAL
Canonical-for: Explicit drift-resolution decision path
Related canonical source: docs/spec/EPIC_7_SPEC_BUNDLE_v1.1.txt
Last reviewed: 2026-04-04
Evidence note: Defines decision path; does not reopen accepted canon by default.

## Drift classes

- `CODE_VS_DOC_DRIFT`
- `REQ_VS_DOC_DRIFT`
- `CODE_VS_REQ_DRIFT`

## Resolution path

1. Identify drift class and impacted canonical artifacts.
2. Confirm runtime truth evidence (if code involved).
3. Confirm frozen REQ/spec expectations.
4. Decide authoritative source for correction path.
5. Apply minimal correction in appropriate layer (doc/req/spec/code) with traceable rationale.
6. Update linked artifacts and record unresolved residual drift (if any).

## Non-reopen rule

- Do not reopen accepted canon unless evidence proves real defect.
- If defect is unproven, document as risk/NOT VERIFIED rather than rewriting canon.

## Cross-links

- MF6-S2 policy: `docs/governance/MF6_S2_DOC_UPDATE_POLICY.md`
- MF6-S3 deprecation/archive: `docs/governance/MF6_S3_DEPRECATION_ARCHIVE_POLICY.md`
- MF1 SSOT map: `docs/ssot/MF1_S3_SSOT_MAP.md`
