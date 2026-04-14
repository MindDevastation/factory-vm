---
name: restore-context
description: Restore canonical repo execution context for factory-vm work. Use at the start of a thread or when resuming work to verify origin, branch, local HEAD, remote HEAD, and worktree state before any edits.
---

# Restore Context

- Read `AGENTS.md`, `docs/workflows/CODEX_WORKFLOW.md`, and `TESTING.md`.
- Verify the repo identity and state:
  - `git remote get-url origin`
  - `git branch -vv`
  - `git status --short --branch`
  - `git rev-parse HEAD`
  - `git rev-parse --abbrev-ref HEAD`
  - `git remote show origin`
- Confirm the current task slice and any known constraints.
- Stop once context is verified.
- Do not edit files, commit, push, or run implementation work.
