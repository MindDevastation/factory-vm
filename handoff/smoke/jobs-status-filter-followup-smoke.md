# Jobs status-filter follow-up smoke note

## Reviewer traceability (actual PR head)
- PR head branch (verify at review time): run `git branch --show-current`
- PR head commit (verify at review time): run `git rev-parse --short HEAD`

## Route used
- `GET /`

## Normal flow (operator path)
- Open the **Status filter** dropdown from the Jobs table controls.
- Select two statuses (example: `DRAFT` and `FAILED`) via checkboxes.
- Result: dropdown opens normally, both statuses remain selected, and the Jobs rows are filtered to matching states only.

## Unavailable/error flow (stronger evidence)
- Jobs page loaded at `GET /` with the browser authenticated as `admin:change_me`.
- During page load, `/v1/ui/jobs/statuses` was explicitly intercepted and returned HTTP `503` to force the unavailable state.
- Result: **Status filter unavailable** note is shown and the dropdown control remains unavailable/non-interactive.
- Screenshot evidence (stable reviewer workflow):
  1. Attach the captured screenshot to this follow-up PR (GitHub-hosted upload in the PR description or a PR comment).
  2. Reference that GitHub attachment URL in the PR under an "Unavailable-state evidence" heading.
  3. Do not use `browser:/tmp/...` paths, because they are local ephemeral paths and are not reviewer-accessible.
