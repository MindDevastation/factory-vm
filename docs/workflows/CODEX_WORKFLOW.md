# Codex Workflow

This is the umbrella Codex workflow for this repo. Keep details in dedicated workflow docs.

## Core rules

- 1 thread = 1 task.
- Keep long context out of a single thread.
- If a workflow repeats, move it into a skill.
- Light tasks should prefer `GPT-5.4-mini`.
- Fast mode is off by default.
- Repo-only tasks should keep web search off.

## Mode policy (minimal)

- Cloud is the default execution mode.
- Local mode is fallback-only for repair/local-artifact/unpublished-state cases.
- Canonical cloud policy and publication sequence live in `docs/workflows/CLOUD_WORKFLOW.md`, including Cloud fallback verification when `origin` is unavailable.

## Mandatory execution flow

1. Restore
   - recover branch/repo state
   - load relevant canonical docs
   - confirm bounded task scope
2. Preflight
   - check scope, risks, and test impact
   - identify exact files to change
   - stop if the task is bigger than one slice
3. Apply
   - implement only the approved bounded change
   - if new scope appears, stop and write a delta plan

## Publication requirement

- Publication actions are opt-in only. Unless the task prompt explicitly requests publication, do not automatically create branches, push, or open PRs.
- If publication is not explicitly requested, complete only the requested analysis/editing/reporting scope and stop before publication actions.
- If publication is explicitly requested, publish from a non-`main` branch with push + remote SHA verification before review.
- When publication is explicitly requested, do not create new branches with the `codex/` prefix; use functional prefixes (for example, `feature/` or `fix/`).
- When publication is explicitly requested, after creating the branch from fresh `origin/main` (and rebasing if needed), open the PR immediately with prepared title/body, then continue implementation on the same branch/PR chain.
- When a PR is explicitly requested, creation is valid only with a numeric PR URL/PR number; `/pull/new/...` is not created-PR proof and missing numeric confirmation is a BLOCKER.
- Cloud verification must not rely only on `origin`-based checks; when `origin` is unavailable, use GitHub-integrated branch/PR state per Cloud canon.
- Use `docs/workflows/CLOUD_WORKFLOW.md` as canonical publication flow.

## Reporting requirement

- Final reporting must explicitly separate: (1) content changes completed, (2) publication not requested, and (3) publication requested but blocked.
- Do not imply that a branch, push, or PR exists unless that action was explicitly requested and completed with proof.

## Review workflow

- Use the review-agent only when the state is truly closed.
- Do not use it while more edits are still expected.

## Canonical references

- AGENTS quick rules: `AGENTS.md`
- Cloud workflow canon: `docs/workflows/CLOUD_WORKFLOW.md`
- PR hygiene: `docs/workflows/PR_HYGIENE.md`
- Environment readiness: `docs/workflows/ENVIRONMENT_HEALTHCHECK.md`
- Ops workflow: `docs/ops/runbook/README.md`
- Testing instructions: `docs/workflows/TESTING.md`
