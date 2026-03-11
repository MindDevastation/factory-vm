# CTUX-S6 handoff addendum (preview/confirm evidence)

## Scope confirmation
- Cosmetic, PR-facing handoff text only.
- No code, backend, tests, routes, or runtime behavior changes.
- Existing testing text remains intact.

## Preview response sample
`POST /v1/track-catalog/custom-tags/bulk-rules/preview`

```json
{
  "can_confirm": true,
  "summary": {
    "total": 2,
    "create": 2,
    "invalid": 0
  },
  "items": [
    {
      "index": 0,
      "action": "CREATE",
      "summary": "active: track_features.payload_json.voice_flag equals false (match=ALL, priority=100)",
      "errors": []
    },
    {
      "index": 1,
      "action": "CREATE",
      "summary": "active: track_features.payload_json.energy gte 0.6 (match=ANY, priority=80)",
      "errors": []
    }
  ]
}
```

## Confirm response sample
`POST /v1/track-catalog/custom-tags/bulk-rules/confirm`

```json
{
  "ok": true,
  "summary": {
    "total": 2,
    "created": 2,
    "invalid": 0
  },
  "results": [
    {
      "index": 0,
      "action": "CREATE"
    },
    {
      "index": 1,
      "action": "CREATE"
    }
  ]
}
```

## Preview/confirm policy note
- Preview returns only `CREATE` or `INVALID` actions.
- Confirm is allowed only when `can_confirm=true`.
- Any invalid preview outcome causes fail-all / no writes.
- Confirm in this slice is atomic create-only.

## Testing context (kept intact)
- Existing CTUX-S6 testing commands and result text are unchanged.
