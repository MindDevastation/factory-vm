# Cloud Workflow (Canonical)

This is the canonical workflow for cloud-first Codex development in this repo.

## Mode policy

- Cloud is the default mode for normal development, review, and publication work.
- In Cloud mode, the source of truth is the GitHub branch/PR state (not a local dirty checkout).
- Local mode is fallback-only, limited to:
  - local environment or git repair
  - producing local-only artifacts
  - handling unpublished local state that cannot be resolved in Cloud first

## Canonical cloud publication flow

1. Sync with `origin/main` and create a new work branch from fresh `origin/main`.
2. Rebase onto fresh `origin/main` if needed before publication.
3. Push the new branch to `origin` immediately.
4. Open the PR immediately with prepared title/body.
5. Make the intended bounded slice on that branch/PR chain.
6. Commit and push the slice.
7. Verify remote branch HEAD SHA matches local `HEAD`.
8. Keep follow-up fixes on the same branch/PR chain.

## Guardrails

- One branch/PR chain should carry one bounded slice.
- Do not create new branches with the `codex/` prefix; use functional prefixes like `feature/` and `fix/` (or other repo-appropriate functional prefixes).
- Do not mix unrelated workstreams in one branch.
- Re-verify SHA after each follow-up push before declaring the slice published.

## Related canon

- Umbrella workflow: `docs/workflows/CODEX_WORKFLOW.md`
- PR hygiene and history checks: `docs/workflows/PR_HYGIENE.md`
- Testing workflow: `docs/workflows/TESTING.md`
