---
name: apply-one-slice
description: Implement exactly one bounded slice in factory-vm, run the required tests, commit, push, and verify the remote SHA before stopping.
---

# Apply One Slice

- Implement only the approved slice and nothing else.
- Keep the change small, isolated, and aligned with the preflight audit.
- Run the required tests for the slice. Prefer the repo canonical unittest command unless a narrower, clearly sufficient subset is justified.
- Commit on a non-main branch.
- Push the slice.
- Verify the local commit SHA matches the remote branch SHA.
- Stop after remote verification.
- Do not start the next slice.
