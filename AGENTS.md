# AGENTS.md - Codex repo rules

This repo keeps Codex guidance short. If a rule needs detail, put it in `docs/`, not here.

## Canonical docs

- Workflow canon: `docs/workflows/CODEX_WORKFLOW.md`
- Ops canon: `docs/ops/runbook/README.md`
- Testing canon: `docs/workflows/TESTING.md`

## Branch / workstream rules

- 1 thread = 1 task.
- Keep one slice per branch and keep slices small.
- New branches must start from fresh `origin/main`.
- Do not mix unrelated workstreams in one thread.
- Long context must not live in one Codex thread; move it to `docs/` or handoff files.
- Repeated workflows should move to skills, not repeated thread prose.
- Light tasks should prefer `GPT-5.4-mini`.
- Fast mode is off by default.
- Repo-only tasks should keep web search off.

## Restore -> preflight -> apply

- Restore: recover the current branch, task scope, and canonical docs.
- Preflight: check scope, risks, and test impact before editing.
- Apply: make only the bounded change from the preflight.
- If scope grows, stop and write a delta plan.

## Tests

- Run: `python -m unittest discover -s tests -v`

## Commit / push / remote verification

- Never commit or push directly to `main`.
- Before opening a PR, rebase the branch onto fresh `origin/main`.
- Before review or publication, inspect branch history relative to `origin/main`.
- Dirty PR history is a workflow problem and should be fixed before review whenever practical.
- A slice is accepted only after push plus remote SHA proof.
- The pushed remote SHA must match the local commit SHA for the slice.

## Review workflow

- Use the review-agent only when the state is truly closed.
- Existing in-flight PRs are not rewritten retroactively unless that is explicitly chosen.
- If more edits are likely, keep the thread local and finish the slice first.

## Definition of done

- Scope matches the slice.
- Tests pass.
- The slice is pushed and the remote SHA is verified.
- Any process detail too long for AGENTS lives in `docs/`.

## Do not

- Do not start feature work before restore and preflight are done.
- Do not create broad refactors, interface renames, or unrelated formatting changes.
- Do not bypass tests, logging, or upload mocking rules.
- Do not use web search for repo-only tasks.
- Do not directly push to `main`.
