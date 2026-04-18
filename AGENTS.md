# AGENTS.md - Codex repo rules

Keep this file short. Put detailed process guidance in `docs/`.

## Canonical docs

- Umbrella workflow: `docs/workflows/CODEX_WORKFLOW.md`
- Cloud workflow (default mode): `docs/workflows/CLOUD_WORKFLOW.md`
- Ops canon: `docs/ops/runbook/README.md`
- Testing canon: `docs/workflows/TESTING.md`

## Branch / slice guardrails

- 1 thread = 1 task.
- Keep one bounded slice per branch/PR chain.
- Start new work from fresh `origin/main` on a non-`main` branch only when publication is explicitly requested in the task prompt.
- Do not create new branches with the `codex/` prefix; use functional prefixes such as `feature/` or `fix/`.
- Repo-only tasks should keep web search off.

## Mode policy

- Cloud is the default for normal dev/review/publish work.
- Local is fallback-only; see `docs/workflows/CLOUD_WORKFLOW.md` for scope.

## Execution flow

- Restore -> preflight -> apply.
- If scope grows, stop and write a delta plan.

## Tests

- Canonical command: `PYTHONPATH=. python -m unittest discover -s tests -v`

## Publish requirements

- Publication actions are opt-in only: do not create branches, push branches, or open PRs unless the task prompt explicitly requests publication.
- If publication is not explicitly requested, stop after requested analysis/editing/reporting work and do not imply branch/PR creation.
- If publication is explicitly requested, never commit or push directly to `main`.
- For explicitly requested publication, after branch creation (and rebase when needed), open the PR immediately using prepared title/body, then continue implementation on the same branch/PR chain.
- For explicitly requested publication, push the slice and verify local SHA equals remote SHA before review/publication.
