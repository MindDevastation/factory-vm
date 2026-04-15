# Testing Workflow

This is the canonical testing workflow for Codex slices in this repo. Keep it short, practical, and tied to the restore -> preflight -> apply flow.

## Canonical Commands

- Full suite: `PYTHONPATH=. python -m unittest discover -s tests -v`
- Targeted suite: `PYTHONPATH=. python -m unittest discover -s tests/<area> -v`
- Windows PowerShell: `$env:PYTHONPATH="."; python -m unittest discover -s tests -v`

## Environment Assumptions

- Run from the repo root.
- Install `requirements.txt` and `requirements-dev.txt` before test execution.
- Set `PYTHONPATH=.` so repo modules resolve consistently.
- Tests mock external services and should not require Google credentials.

## Slice Scope

- Docs/process-only slice: usually no runtime test impact. Run tests only if the doc changes testing rules, commands, or references that affect execution.
- Repo-tooling slice: run the minimum targeted tests that cover the touched tooling, workflow, or test harness. Escalate to full-suite only if shared bootstrap or harness behavior changes.
- Product-code slice: run targeted tests for the touched feature area. Escalate to full-suite when the change is cross-cutting, fixture-heavy, or not safely localizable.

## When Full Suite Is Required

- Shared test harness, fixtures, bootstrap, or dependency setup changes.
- CI/test workflow changes that alter how tests are discovered, isolated, or executed.
- Broad cross-subsystem changes, or any slice that cannot reasonably localize impact.

## When Targeted Tests Are Enough

- A bounded product change with a clear test area.
- A repo-tooling change with a known, narrow blast radius.
- A docs-only slice that does not alter execution rules or test behavior.

## Reporting Blocked Tests

- Report a test as blocked when it did not run, and say why.
- Distinguish blocked from passed; do not imply success if the command never executed.
- Call out missing dependencies, permissions, environment setup, or unavailable services explicitly.

## Failure Attribution

- Slice-caused failure: the failing test exercises touched code, workflow, or fixtures, or the failure disappears when the slice is removed.
- Environment/setup failure: missing packages, wrong `PYTHONPATH`, interpreter mismatch, permissions, or unavailable external services.
- Unrelated existing repo failure: the failing area is outside the slice and reproduces on an unchanged or clean baseline.

## Minimum Evidence In Codex Reports

- Exact command(s) run.
- Pass, fail, or blocked result.
- The narrow test scope covered.
- The key failing test names or the blocker reason.
- A note when the full suite was not run, with the reason.

## Remote Verification Stays Separate

- Passing tests do not satisfy publish verification.
- After push, the local commit SHA must still match the remote branch SHA.

## Workflow Fit

- Restore: load this doc with the repo state before editing.
- Preflight: decide the smallest justified test scope and expected evidence before applying changes.
- Apply: run the minimum necessary tests for the slice, record the result truthfully, and stop if the scope widens.
