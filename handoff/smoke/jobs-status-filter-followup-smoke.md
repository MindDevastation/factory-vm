# Jobs status-filter follow-up smoke note

## Reviewer traceability (actual PR head)
- Head branch: `fix/jobs-status-unavailable-evidence`
- Head commit: run `git rev-parse --short HEAD` on this branch (must match the current PR head at review time).

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
- Screenshot artifact:

![Jobs page status-filter unavailable state](browser:/tmp/codex_browser_invocations/889215ebcbd6f34e/artifacts/handoff/smoke/jobs-status-filter-unavailable-get-root.png)
