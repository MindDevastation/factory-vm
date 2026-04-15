---
name: remote-verify
description: Verify a published factory-vm slice after push by comparing branch, local HEAD, and remote HEAD, then report blocker status without making changes.
---

# Remote Verify

- Use after a slice is pushed and before stopping work.
- Verify the current branch and local commit:
  - `git branch --show-current`
  - `git rev-parse HEAD`
  - `git status --short --branch`
- Verify the remote tracking state:
  - `git fetch origin`
  - `git rev-parse @{u}`
  - `git rev-parse origin/$(git branch --show-current)`
- Confirm local `HEAD` equals the remote branch SHA.
- Report one of: `BLOCKER`, `RISK`, or `NOT VERIFIED` if the branch or remote state cannot be proven.
- Do not edit files, create commits, or push.
