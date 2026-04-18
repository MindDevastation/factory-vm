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
- Canonical cloud policy and publication sequence live in `docs/workflows/CLOUD_WORKFLOW.md`.

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

- Publish from a non-`main` branch and follow Cloud verification/proof rules in `docs/workflows/CLOUD_WORKFLOW.md`.
- PR creation is only confirmed by a numeric PR URL/PR number (not by `/pull/new/...` branch-open links).
- Use `docs/workflows/CLOUD_WORKFLOW.md` as canonical publication flow.

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
