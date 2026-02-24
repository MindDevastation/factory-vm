# AGENTS.md — Codex Rules (Fast, Controlled, PR-Slice Workflow)

This repo is developed with Codex. The priority is **accuracy + control**, while keeping changes **small and mergeable**.
Codex must follow these rules strictly.

---

## 0) Default mode: Small PR Slices (mandatory)

- Every task must be delivered as a **small PR slice**.
- Target PR size:
  - **≤ 300 changed lines** total (soft cap)
  - **≤ 10 files changed** (soft cap)
- If the task is larger:
  - split into multiple PRs (P0 first),
  - each PR must be independently mergeable.

---

## 1) Branch & PR policy (mandatory)

- Never commit or push directly to `main`.
- Always create a new branch:
  - `feature/<short-scope>`
  - `fix/<short-scope>`
  - `chore/<short-scope>`
- Push branch and open a Pull Request.
- PR description must include:
  - What changed (2–5 bullets)
  - How to test (exact commands)
  - Test result summary (pass/fail)

---

## 2) Two-phase execution: PLAN → APPLY (mandatory)

For every task:

### Phase A: PLAN
- Produce a short plan:
  - Files to edit (exact paths)
  - Minimal steps (3–7 bullets)
  - Risks (if any)
- Keep plan short. No long essays.

### Phase B: APPLY
- Implement only what is in the plan.
- If new information appears requiring extra scope:
  - stop and write a **delta plan** (1–3 bullets),
  - do not proceed beyond scope.

---

## 3) Tests (mandatory)

Before pushing PR:

Run:

    python -m unittest discover -s tests -v

If tests fail:
- Fix properly (no disabling, no weakening assertions).
- Do not “work around” failures by removing test coverage.

Coverage:
- Must not intentionally decrease.
- If it decreases due to legitimate refactor, explain why in PR.

---

## 4) Scope control (hard constraints)

Codex must:
- Change only files required for the task.
- Avoid refactors not demanded by the task.
- Avoid renaming public interfaces unless explicitly requested.
- Avoid changing formatting across unrelated files.

If multiple solutions exist:
- choose the **most deterministic and minimal** option.

---

## 5) Rendering pipeline safety (mandatory)

- Never perform real external uploads during tests.
- Upload must be mocked in tests.
- Heavy ffmpeg renders must not run in tests.
- Tests may use very short dummy media only (seconds).

When editing render logic, preserve:
- FFMPEG_CMD logging
- stderr logging
- watchdog logic ("file not growing")
- CPU fallback logic

---

## 6) DB/state changes (mandatory)

If DB schema or job state rules change:
- update/extend tests,
- keep backward compatibility unless explicitly instructed.

---

## 7) Dependencies (strict)

- Runtime deps → `requirements.txt`
- Dev/test deps → `requirements-dev.txt`
- Do not mix them.

If adding a dependency:
- justify it in PR description.

---

## 8) Logging (strict)

- Do not remove existing logging.
- Do not silence exceptions.
- Prefer structured, actionable logs (include job_id, stage, cmd).

---

## 9) Forbidden actions (hard)

- Direct push to `main`
- Disabling/removing tests
- Removing coverage tooling
- Hardcoding secrets/tokens/paths specific to one machine
- Large “cleanup/refactor” PRs without explicit request
- Changing git setup scripts unless explicitly requested

---

## 10) Completion checklist (mandatory)

A task is done only if:
- Changes are committed on a non-main branch
- Tests pass (`python -m unittest discover -s tests -v`)
- PR is opened to `main`
- PR includes summary + test results + exact test commands

Minimal, deterministic changes win.
