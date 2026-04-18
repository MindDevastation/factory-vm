# Cloud Workflow (Canonical)

This is the canonical workflow for cloud-first Codex development in this repo.

## Mode policy

- Cloud is the default mode for normal development, review, and publication work.
- In Cloud mode, the source of truth is the GitHub branch/PR state (not a local dirty checkout).
- Cloud checkouts may be repo-backed while not exposing a normal local git remote named `origin`.
- Local mode is fallback-only, limited to:
  - local environment or git repair
  - producing local-only artifacts
  - handling unpublished local state that cannot be resolved in Cloud first

## Canonical cloud publication flow

1. Start from fresh `origin/main` and create a non-`main` work branch when `origin` is available.
2. If Cloud does not expose a normal local `origin`, use the GitHub-integrated branch/PR flow as the canonical fallback.
3. Push/publish the new branch immediately.
4. Open the PR immediately with prepared title/body.
5. PR creation is considered successful only when a numeric PR URL/PR number exists (for example, `/pull/123`).
6. A branch-open URL such as `/pull/new/...` is not created-PR proof.
7. If numeric PR confirmation cannot be obtained, treat it as a BLOCKER and do not mark the task complete.
8. Make the intended bounded slice on that branch/PR chain.
9. Commit and push the slice.
10. Verify published branch state:
    - use git local/remote SHA match when `origin` is available
    - otherwise use GitHub-integrated branch/PR commit state in Cloud
11. Keep follow-up fixes on the same branch/PR chain.

## Guardrails

- One branch/PR chain should carry one bounded slice.
- Do not create new branches with the `codex/` prefix; use functional prefixes like `feature/` and `fix/` (or other repo-appropriate functional prefixes).
- Do not mix unrelated workstreams in one branch.
- Re-verify SHA after each follow-up push before declaring the slice published.
- If numeric PR confirmation cannot be obtained, that is a BLOCKER and the task is not complete.

## Related canon

- Umbrella workflow: `docs/workflows/CODEX_WORKFLOW.md`
- PR hygiene and history checks: `docs/workflows/PR_HYGIENE.md`
- Testing workflow: `docs/workflows/TESTING.md`
