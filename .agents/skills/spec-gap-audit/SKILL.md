---
name: spec-gap-audit
description: Compare factory-vm implementation state against canonical REQ and SPEC sources, then classify real gaps, blockers, risks, and unverified areas without changing code.
---

# Spec Gap Audit

- Compare the current repo state against the canonical sources for the slice:
  - `docs/spec/`
  - `docs/req/`
  - `docs/ssot/`
  - related workflow or runbook docs when they are the contract source
- Check whether behavior matches the stated requirement, not just the expected implementation shape.
- Separate findings into:
  - `BLOCKER`
  - `RISK`
  - `NOT VERIFIED`
- Distinguish true gaps from scope-complete behavior that is simply out of slice or intentionally deferred.
- Include exact doc or code references for each finding when possible.
- Do not edit files, create commits, or push.
