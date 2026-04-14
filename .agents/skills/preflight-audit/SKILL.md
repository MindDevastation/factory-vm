---
name: preflight-audit
description: Audit scope, blockers, repo fit, and test impact before implementing a factory-vm slice. Use before any edit-heavy task to confirm the work fits one bounded slice.
---

# Preflight Audit

- Restate the task in one bounded slice.
- Check repo fit:
  - workflow/tooling/docs work is in scope
  - product-code changes are out of scope unless explicitly required
- Identify blockers, risks, and the exact files likely to change.
- Produce a test-impact baseline:
  - existing tests impacted
  - existing tests to update
  - new tests to add
  - stale assertions, fixtures, or contracts to correct
- If the task does not fit one slice, write a delta plan and stop.
- Do not edit files, commit, or push.
