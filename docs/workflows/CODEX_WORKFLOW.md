# Codex Workflow

This doc holds the fuller repo workflow so `AGENTS.md` can stay short.

## Core rules

- 1 thread = 1 task.
- Keep long context out of a single thread.
- If a workflow repeats, move it into a skill.
- Light tasks should prefer `GPT-5.4-mini`.
- Fast mode is off by default.
- Repo-only tasks should keep web search off.

## Mandatory mode flow

1. Restore
   - recover the current branch state
   - load the relevant canonical docs
   - confirm the task slice and any known constraints
2. Preflight
   - check scope, risks, and test impact
   - identify exact files to change
   - stop if the task is bigger than one slice
3. Apply
   - make only the approved bounded change
   - if new scope appears, stop and write a delta plan

## Publication Flow

- For new work, create a branch from fresh `origin/main`, switch to it, and push it to `origin` immediately so the branch exists on GitHub before the slice is treated as active.
- Make the intended slice changes on that branch, then commit and push the slice.
- Verify the remote HEAD SHA before creating the PR.
- Create the PR only after the branch is published, and populate the PR title and body as part of publication.
- For follow-up fixes on an existing PR branch, stay on the same branch/PR chain rather than creating a fresh PR branch.

## Slice acceptance

- A slice is not accepted until it is pushed.
- Remote SHA proof is required.
- The local commit SHA and remote branch SHA must match.

## Review workflow

- Use the review-agent only when the state is truly closed.
- Do not use it while more edits are still expected.

## Canonical references

- Repo workflow guidance: `AGENTS.md`
- Environment readiness: `docs/workflows/ENVIRONMENT_HEALTHCHECK.md`
- Ops workflow: `docs/ops/runbook/README.md`
- Testing instructions: `docs/workflows/TESTING.md`
