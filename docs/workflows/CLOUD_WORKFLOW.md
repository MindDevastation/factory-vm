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

0. Publication is opt-in only. Do not create branches, push, or open PRs unless the task prompt explicitly requests publication.
1. If publication is not explicitly requested, stop after requested analysis/editing/reporting work; no publication actions.
2. If publication is explicitly requested, start from fresh `origin/main` and create a non-`main` work branch when `origin` is available.
3. If Cloud does not expose a normal local `origin`, use the GitHub-integrated branch/PR flow as the canonical fallback.
4. Push/publish the new branch immediately.
5. Open the PR immediately with prepared title/body.
6. If a PR is explicitly requested, creation is successful only when a numeric PR URL/PR number exists (for example, `/pull/123`).
7. A branch-open URL such as `/pull/new/...` is not created-PR proof.
8. If numeric PR confirmation cannot be obtained, treat it as a BLOCKER and do not mark the task complete.
9. Make the intended bounded slice on that branch/PR chain.
10. Commit and push the slice.
11. Verify published branch state (Cloud verification must not depend only on `origin`-based git checks):
    - use git local/remote SHA match when `origin` is available
    - otherwise use GitHub-integrated branch/PR commit state in Cloud
12. Keep follow-up fixes on the same branch/PR chain.

## Reporting contract

- Final reports must state publication status explicitly:
  - `content changes completed`
  - `publication not requested`
  - `publication requested but blocked`
- Never imply branch/push/PR creation when publication was not explicitly requested.

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
