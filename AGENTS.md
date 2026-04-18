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
- Start new work from fresh `origin/main` on a non-`main` branch.
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

- Never commit or push directly to `main`.
- Push the slice and verify local SHA equals remote SHA before review/publication.
