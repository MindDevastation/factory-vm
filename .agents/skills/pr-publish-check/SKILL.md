---
name: pr-publish-check
description: Check whether a factory-vm branch is ready to publish or review as a clean PR slice, including branch visibility, history hygiene, slice purity, and push verification status.
---

# PR Publish Check

- Use after a slice is implemented and before opening or updating a PR.
- Keep the check read-only. Do not edit files, commit, or push.

## Check List

- Confirm the current branch is visible and the workspace is on the expected branch.
- Confirm `origin/main` is visible and can be compared against the branch.
- Inspect history relative to `origin/main`.
  - Prefer `git log --oneline origin/main..HEAD`.
  - Use `git log --oneline --graph --decorate origin/main..HEAD` when history shape matters.
- Check for merge noise from `origin/main`.
  - A merge commit from `origin/main` in a new microfeature branch is a blocker.
- Judge whether the branch contains only the intended slice commits.
  - Extra unrelated commits or cross-slice drift are blockers.
- Check push and remote SHA proof.
  - If the branch is unpushed, or local `HEAD` does not match the remote branch SHA, report it as not verified.

## Branch Type

- New microfeature branch:
  - should start from fresh `origin/main`
  - should stay isolated to one slice
  - should not carry merge commits from `origin/main`
- Follow-up work on an existing PR branch:
  - may add narrow follow-up commits on the same slice
  - should stay readable and reviewable without merge noise
  - rebase if the branch needs the latest `origin/main`

## Immediate PR Blockers

- Branch visibility cannot be proven.
- `origin/main` visibility or comparison is unavailable.
- Branch history is dirty relative to `origin/main`.
- A merge commit from `origin/main` is present on a branch that should remain clean.
- The branch includes unrelated or cross-slice commits.
- Push status or remote SHA proof is missing.

## Output

- Report the state truthfully as `BLOCKER`, `RISK`, or `NOT VERIFIED`.
- State whether the branch looks like a new microfeature branch or follow-up work.
- Cross-link the workflow canon:
  - `docs/workflows/CODEX_WORKFLOW.md`
  - `docs/workflows/PR_HYGIENE.md`
  - `docs/workflows/TESTING.md`
