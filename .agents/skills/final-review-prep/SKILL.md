---
name: final-review-prep
description: Prepare a factory-vm slice for review-agent use by confirming closure, collecting evidence, and shaping a clean handoff without editing code.
---

# Final Review Prep

- Use only when the slice looks closed and may move to review.
- Confirm closure is real:
  - clean intended diff
  - tests passed
  - push completed
  - remote SHA verified
- Collect handoff evidence:
  - exact task boundary
  - touched file paths
  - validation commands
  - test output summary
  - remote/local SHA proof
  - known limitations or deferred items
- Build a concise review-agent prompt from that evidence.
- If anything is still unpushed, untested, or unresolved, mark the state as not ready.
- Do not edit files, create commits, or push.
