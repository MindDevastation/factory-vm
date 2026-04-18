# Review Readiness

This is the canonical review-readiness guide for Codex work in `factory-vm`. Use it with `AGENTS.md`, `docs/workflows/CODEX_WORKFLOW.md`, `docs/workflows/PR_HYGIENE.md`, and `docs/workflows/TESTING.md`.

## What Review Readiness Means

Review readiness means the slice is closed enough that another reviewer or review-agent can assess it without guessing about missing implementation, missing publication proof, or hidden follow-up work.

## Minimum Conditions Before Review-Agent

Before handing a slice to review-agent, confirm:

1. The slice scope is complete and no further product or workflow edits are expected.
2. The branch is identified and tied to the slice.
3. The changed files are known and match the intended scope.
4. The commit SHA is known.
5. Published branch/PR commit state is known when the branch is published.
6. Local HEAD matches remote HEAD when `origin`-based verification is used.
7. Numeric PR URL/PR number exists as PR-created proof (not `/pull/new/...`).
8. Test evidence is available, or the blocker is stated truthfully.
9. The remote HEAD SHA is known when the branch is published.

## Review States

- `ready for review`: the slice is complete, published when relevant, and supported by enough evidence to review now.
- `not ready for review`: work is still in progress, publication is missing, or the slice is not closed.
- `partially verified / not verified`: some evidence exists, but the slice still lacks either proof of publication, proof of test coverage, or proof that the final state is the intended state.

## Required Evidence

When applicable, record:

- branch and PR context
- changed files
- commit SHA
- remote HEAD SHA when `origin` verification is used
- whether local HEAD equals remote HEAD when `origin` verification is used
- numeric PR URL/PR number as PR-created proof
- test evidence, or a truthful blocker explanation

Docs-only or process-only slices may need lighter evidence, but they still need truthful publication state and an honest note about what was and was not verified.

## Evidence Classification

- `BLOCKER`: prevents review now, such as unfinished implementation, missing publish proof, branch mismatch, or a known failing prerequisite.
- `RISK`: review can proceed, but the slice carries a real concern that should be called out.
- `NOT VERIFIED`: evidence is missing or incomplete, and the missing piece matters to the review decision.

## What Blocks Review Immediately

Block review immediately when any of these are true:

- implementation is unfinished
- the branch is not published when publication proof is expected
- remote HEAD cannot be confirmed
- local HEAD does not match remote HEAD when that match is required
- numeric PR confirmation cannot be obtained
- only a `/pull/new/...` link exists instead of a numeric PR URL/PR number
- the changed-file set is unclear
- test status is unknown and cannot be stated honestly

## Workflow Fit

- `restore`: establish branch, repo state, and current slice before editing.
- `preflight`: decide whether the task is one bounded slice and what evidence will be needed.
- `apply`: make only the approved slice and capture the evidence needed for closure.
- `publish`: push the slice, verify publish state (git remote SHA when `origin` exists, GitHub-integrated state otherwise), and only then treat it as reviewable when the slice requires publication proof.

## Guardrails

- Review-agent is not a substitute for unfinished implementation.
- Review-agent is not a substitute for missing publication proof.
- Use review-agent only when the slice is truly closed or reviewable.

## Cross-Links

- Workflow canon: `docs/workflows/CODEX_WORKFLOW.md`
- PR hygiene: `docs/workflows/PR_HYGIENE.md`
- Testing canon: `docs/workflows/TESTING.md`
- Repo rules: `AGENTS.md`
