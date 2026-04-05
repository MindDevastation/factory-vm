# Slice 2 Verification — monthly planning templates preview-apply engine

## Scope checked
- PR artifact in local branch `work` with Slice 2 commits:
  - `a8a879c` — Add monthly template preview-apply engine
  - `2830b36` — Fix provenance duplicate preview lookup
- Base used for diff inspection: `af2dc30`.
- Files inspected:
  - `services/common/db.py`
  - `services/planner/monthly_planning_template_service.py`
  - `services/factory_api/planner.py`
  - `tests/unit/test_monthly_planning_template_service.py`
  - `tests/integration/test_monthly_planning_templates_api.py`

## Evidence summary
- `git diff --name-status af2dc30..HEAD` confirms only the five required files changed.
- Unresolved merge conflict markers are present in service and both targeted test files.
- Python compilation/import fails with `SyntaxError` on these markers.

## Verdict
- **FAIL** (code blocker)

## Blockers
1. **Unresolved merge conflict markers in production service code**
   - File: `services/planner/monthly_planning_template_service.py`.
   - Markers present around provenance duplicate logic (`<<<<<<<`, `=======`, `>>>>>>>`).
   - This breaks import and prevents API startup.

2. **Unresolved merge conflict markers in targeted tests**
   - Files: `tests/unit/test_monthly_planning_template_service.py`, `tests/integration/test_monthly_planning_templates_api.py`.
   - Tests cannot even be imported due to syntax errors.

## Spec compliance status (Slice 2)
- Preview read-only path: **NOT VERIFIED** (service module not importable).
- Month/date validation and invalid date outcome: **NOT VERIFIED**.
- Hard duplicates (slot/provenance): **NOT VERIFIED**.
- Soft overlap informational behavior: **NOT VERIFIED**.
- Fingerprint determinism/change semantics: **NOT VERIFIED**.
- API contract + planner_error envelope: **PARTIALLY VERIFIED** in router code, but end-to-end behavior **NOT VERIFIED** due import failure.
- No hidden scope drift: **PARTIALLY VERIFIED** by file list only; runtime behavior **NOT VERIFIED**.

## Reproduction commands
- `rg -n "<<<<<<<|>>>>>>>|=======" services/planner/monthly_planning_template_service.py tests/unit/test_monthly_planning_template_service.py tests/integration/test_monthly_planning_templates_api.py`
- `python -m py_compile services/planner/monthly_planning_template_service.py tests/unit/test_monthly_planning_template_service.py tests/integration/test_monthly_planning_templates_api.py`
- `python -m unittest -v tests.unit.test_monthly_planning_template_service tests.integration.test_monthly_planning_templates_api`

