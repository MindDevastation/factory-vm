# Cloud Workflow (Canonical)

This is the canonical workflow for cloud-first Codex development in this repo.

## Mode policy

- Cloud is the default mode for normal development, review, and publication work.
- In Cloud mode, the source of truth is the GitHub branch/PR state (not a local dirty checkout).
- Cloud repo-backed checkouts may exist without exposing a normal local `origin` remote.
- Local mode is fallback-only, limited to:
  - local environment or git repair
  - producing local-only artifacts
  - handling unpublished local state that cannot be resolved in Cloud first

## Canonical cloud publication flow

1. Start a new work branch from fresh `origin/main` when `origin` is available in the cloud checkout.
2. Push the new branch immediately.
3. Open the PR immediately with prepared title/body.
4. Treat PR creation as successful only when a numeric PR URL/PR number exists.
5. Treat branch-open links such as `/pull/new/...` as not-created proof (they do not prove a PR exists).
6. Make the intended bounded slice on that branch/PR chain.
7. Commit and push the slice.
8. Verify publication using one of these valid paths:
   - `origin` available: verify remote branch HEAD SHA matches local `HEAD`.
   - `origin` unavailable in Cloud: verify GitHub-integrated branch/PR state for the same branch/PR chain.
9. Keep follow-up fixes on the same branch/PR chain.

## Guardrails

- One branch/PR chain should carry one bounded slice.
- Do not mix unrelated workstreams in one branch.
- Re-verify SHA after each follow-up push before declaring the slice published.
- If numeric PR confirmation cannot be obtained, that is a BLOCKER and the task is not complete.

## Related canon

- Umbrella workflow: `docs/workflows/CODEX_WORKFLOW.md`
- PR hygiene and history checks: `docs/workflows/PR_HYGIENE.md`
- Testing workflow: `docs/workflows/TESTING.md`
