# PR Hygiene

This repo keeps PR history simple on purpose. Use this as the short branch-and-PR hygiene rule set for Codex slices.

## Branch Rules

- New branches must start from a fresh `origin/main`.
- Do not merge `origin/main` into feature branches.
- If a feature branch needs the latest `main`, rebase onto `origin/main` instead.

## Before Opening A PR

- Rebase the branch onto fresh `origin/main` before opening the PR.
- Inspect the branch history relative to `origin/main`.
- Preferred check:
  - `git log --oneline origin/main..HEAD`
- Optional extra check:
  - `git log --oneline --graph --decorate origin/main..HEAD`

## What Clean PR History Means Here

- The branch contains only the commits needed for the slice.
- The history is readable without merge noise from `origin/main`.
- Commit order tells the story of the work instead of hiding it in a merge commit.
- Dirty PR history is a workflow problem and should be fixed before review whenever practical.
- Follow-up edits on the same PR may add commits, but they should stay on the same slice and remain easy to review.

## New Branch vs Follow-Up Work

- New microfeature branch: create it from fresh `origin/main`, then keep it isolated to one slice.
- Follow-up work on an existing PR branch: continue the same slice, and rebase if the branch needs the latest `origin/main`.

## Immediate PR Blockers

- A merge commit from `origin/main` landed in the feature branch.
- The branch was not started from fresh `origin/main` for a new microfeature.
- The branch contains unrelated commits or cross-slice drift.
- The branch history cannot be checked cleanly against `origin/main`.

## Push And Remote SHA Proof

- PR hygiene does not replace publish verification.
- A slice is still accepted only after push plus remote SHA proof.
- The remote branch SHA must match the local commit SHA before the PR is considered closed.

## Existing PRs

- This rule is prospective only.
- Older PRs already in flight are not automatically rewritten retroactively unless explicitly chosen.

## Cross-Links

- Workflow canon: `docs/workflows/CODEX_WORKFLOW.md`
- Testing canon: `docs/workflows/TESTING.md`
