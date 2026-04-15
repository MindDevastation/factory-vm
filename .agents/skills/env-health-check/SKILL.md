---
name: env-health-check
description: Verify local workspace readiness for factory-vm before restore, preflight, or apply when shell, git, Python, or test invocation may be stale or uncertain.
---

# Environment Health-Check

- Use before serious Codex work when local readiness is uncertain.
- Confirm, at minimum:
  - `git` is available.
  - `origin` is visible and queryable.
  - the current branch is visible.
  - the Python launcher/interpreter is available.
  - the virtual environment is active when the slice expects one.
  - the canonical test command can be invoked from the repo root:
    - `PYTHONPATH=. python -m unittest discover -s tests -v`
- Classify failures first:
  - `repo/code failure`: the command reaches repo logic and fails because of tracked repo state, docs, fixtures, or code.
  - `local environment failure`: missing tools, broken PATH, bad shell, missing dependencies, or unusable venv.
  - `remote/auth failure`: `origin` cannot be reached, authenticated, or read.
- Report blocked state truthfully with the exact command and result. Do not claim success for checks that did not run.
- This check helps decide whether work can start; it does not replace remote SHA verification after push.
- Do not edit product code, create commits, or push.
- Keep the check short and practical; use the canonical ops runbook for deeper environment troubleshooting.
