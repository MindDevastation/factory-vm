# CTUX-S9 follow-up handoff (P1 UI/help reviewer verification)

## Root cause of previous "Not Found"
- Route wiring in this branch chain is already correct (`/ui/track-catalog/custom-tags` plus legacy alias `/ui/tags`).
- The blocking reviewer issue was an invalid prior evidence capture (the screenshot did not reflect the reachable authenticated Custom Tags page), not a missing backend/UI route.

## Reachable reviewer URL (verified)
- Canonical page: `http://127.0.0.1:8091/ui/track-catalog/custom-tags`
- Legacy alias (also reachable): `http://127.0.0.1:8091/ui/tags`

## Reviewer-facing smoke evidence
- Screenshot note:
  - Prior capture path `browser:/tmp/codex_browser_invocations/0d2dce8ecbf856fe/artifacts/artifacts/ctux_s9_custom_tags_smoke.png` was session-local and is not durable for other reviewers/CI clones.
  - Durable reviewer evidence for this slice is the reproducible URL + curl checks + UI checklist below.
- Visible in screenshot:
  - `Usage` column header in the Custom Tags table.
  - `Track Catalog → Custom Tags → Assignment Tools` section.
  - `Preview rule matches` UI entry point in Rule editor.

### Durable reproducible evidence (primary)
- Reachable URL (authenticated):
  - `http://127.0.0.1:8091/ui/track-catalog/custom-tags`
- Curl checks (exact):
  - `curl -u admin:change_me http://127.0.0.1:8091/ui/track-catalog/custom-tags`
  - `curl -u admin:change_me "http://127.0.0.1:8091/v1/track-catalog/custom-tags?include_usage=true"`
- Exact UI elements verified:
  - `Usage` column header in Custom Tags table.
  - `Track Catalog → Custom Tags → Assignment Tools` helper text/section.
  - `Preview rule matches` entry point in the rule editor flow.
- Manual smoke checklist result:
  - PASS: page route reachable, usage stats visible, dry-run preview executed, and preview response summary returned.

## Smoke notes
- Usage stats smoke:
  - Page renders the `Usage` column and successfully calls the listing endpoint with `include_usage=true`.
- Preview/recompute smoke:
  - `Dry-run reassign preview` button executed from UI and returned:
    - `ops_note= Preview completed.`
    - `ops_summary= Summary: {"new_assignments":0,"removed_assignments":0,"unchanged_tracks":0}`

## Explicit pages with added hints/tooltips (CTUX-S9 handoff checklist)
- Dashboard / Jobs bulk editor
- Track Catalog / Custom Tags
- Track Catalog Analysis Report
- DB Viewer
- Planner / Bulk Releases
- Job create/edit page

## Commands run for this handoff slice
```bash
python scripts/init_db.py
python -m uvicorn services.factory_api.app:app --host 0.0.0.0 --port 8091
curl -u admin:change_me http://127.0.0.1:8091/ui/track-catalog/custom-tags
curl -u admin:change_me "http://127.0.0.1:8091/v1/track-catalog/custom-tags?include_usage=true"
python -m unittest discover -s tests -v
```
