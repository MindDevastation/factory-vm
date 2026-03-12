# Jobs status-filter follow-up smoke note

## Route used
- `GET /`

## Normal flow (operator path)
- Open the **Status filter** dropdown from the Jobs table controls.
- Select two statuses (example: `DRAFT` and `FAILED`) via checkboxes.
- Result: dropdown opens normally, both statuses remain selected, and the Jobs rows are filtered to matching states only.

## Unavailable/error flow
- Simulate statuses endpoint unavailable/error for `/v1/ui/jobs/statuses`.
- Result: **Status filter unavailable** note is shown and the dropdown control is visibly unavailable/non-interactive (summary trigger does not open the dropdown).
