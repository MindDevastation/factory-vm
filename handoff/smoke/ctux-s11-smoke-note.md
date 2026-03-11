# CTUX-S11 smoke note (seeded local run)

## Environment prep
- `python scripts/init_db.py`
- `python scripts/setup_cta_s8_smoke.py`
- `python -m uvicorn services.factory_api.app:app --host 0.0.0.0 --port 18080`

## URLs used
- Main Custom Tags control center: `http://127.0.0.1:18080/ui/track-catalog/custom-tags`
- Channel dashboard: `http://127.0.0.1:18080/ui/track-catalog/custom-tags/dashboard/darkwood-reverie`

## Manual smoke checklist
- [x] Main Custom Tags page loaded with catalog table and no page-level internal/parse error.
- [x] Clone / bulk controls visible on the main page.
- [x] Taxonomy export button populated JSON in the textarea.
- [x] Taxonomy import preview completed without JS/runtime failure.
- [x] Taxonomy import confirm completed without JS/runtime failure.
- [x] Channel dashboard loaded without JSON parse failure.

## Screenshots
- `handoff/smoke/ctux-s11-tags-main-clean.png`
- `handoff/smoke/ctux-s11-tags-dashboard-clean.png`
