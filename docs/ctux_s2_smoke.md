# CTUX-S2 (P0-A UI) Smoke Evidence

## Reachable URL
- Canonical Custom Tags UI URL: `/ui/track-catalog/custom-tags`
- Legacy alias preserved: `/ui/tags`

## Local smoke environment
- Server: `uvicorn services.factory_api.app:app --host 0.0.0.0 --port 8091`
- Basic auth: `admin:testpass`
- API endpoint used by page: `/v1/track-catalog/custom-tags`

## Seeded local smoke data
- VISUAL `neon-city` (with channel binding `darkwood-reverie` and one active rule)
- VISUAL `no-bindings` (no channel bindings, no rules)
- MOOD `calm` (one active rule)
- THEME `cyberpunk` (no rules)

## Verified checks
- Page loads at `/ui/track-catalog/custom-tags` with populated rows.
- Category filter: `category=VISUAL` returns only VISUAL rows.
- Exact ID filter: `tag_id=<existing visual id>` returns a single exact row.
- `q` filter: `q=neon` returns matching code/label rows.
- Bound Channels rendering:
  - VISUAL tag without bindings -> `No bindings`
  - MOOD/THEME rows -> `—`
- Rules rendering:
  - tag with no active rules -> `No rules`
