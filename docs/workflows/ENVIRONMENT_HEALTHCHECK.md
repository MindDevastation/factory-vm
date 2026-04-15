# Environment Health-Check

This is the canonical local readiness check for Codex work in this repo. It is a quick gate before restore, preflight, or apply when the environment may be stale, broken, or ambiguous.

## What It Means

An environment health-check answers one question: "Can this workspace reliably support repo work right now?"

It is about local readiness, not product behavior. A clean health-check does not prove the code is correct; it only shows the local shell, repo access, Python, and test invocation path are usable.

## Minimum Checks

Before serious Codex work, confirm:

1. `git` is available and responds normally.
2. The `origin` remote is visible and reachable.
3. The current branch is visible and matches the intended slice.
4. The Python launcher/interpreter is available.
5. The virtual environment is active when the slice expects one.
6. The canonical test command can be invoked from the repo root:
   - `PYTHONPATH=. python -m unittest discover -s tests -v`

If the repo uses a different active shell on Windows, the command form may differ, but the repo-root test target stays the same.

## Failure Types

Classify the failure first, then decide whether work can continue:

- Repo/code failure: the command reaches repo logic and fails because of code, docs, fixtures, or tracked repo state.
- Local environment failure: missing `git`, missing Python, bad PATH, broken venv, missing dependencies, or an unusable shell.
- Remote/auth failure: the remote cannot be reached or authenticated, or the remote SHA cannot be read.

## What Blocks Work

Block immediately when:

- the current branch cannot be identified
- `origin` cannot be queried or trusted
- Python cannot be launched
- the venv/dependency state is required but unavailable
- the canonical test command cannot be invoked at all
- the failure prevents you from distinguishing repo issues from environment issues

## What Is Tolerable

For docs/process-only slices, a missing runtime dependency is usually tolerable if the task does not require execution and the doc can still be updated safely.

Even then, report the limitation clearly if the environment prevented any required verification.

## How It Fits The Workflow

- Restore: run the health-check when repo state or shell readiness is uncertain before relying on branch or remote context.
- Preflight: use it to separate environment problems from task scope and test impact.
- Apply: if the environment degrades during the slice, stop and report the block before making further changes.

## Reporting A Blocked State

When the environment is unhealthy, report only what was actually observed:

- the exact command(s) you ran
- the exact failure or blocker message
- whether the problem looks like repo/code, local environment, or remote/auth
- whether work is fully blocked or only partially blocked

Do not claim success for checks that did not run.

## Minimum Evidence In A Codex Report

Include:

- current branch
- whether `origin` was reachable
- Python/venv status
- the canonical test command result or blocker
- the narrow reason work stopped

## Guardrails

- Environment health-check does not replace remote SHA verification after push.
- Machine-specific fixes stay out of portable repo docs and config unless they are intentionally generalized and reusable.
- For deeper runtime/operator environment guidance, use the canonical ops runbook instead of expanding this doc into a troubleshooting guide.
