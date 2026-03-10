# FVR-S7 Follow-up Handoff: Preview/Confirm Response Examples

This follow-up adds concrete response artifacts only; runtime/API behavior is unchanged.

## 1) Preview response example
Endpoint: `POST /v1/track-catalog/custom-tags/bulk/preview`

```json
{
  "summary": {
    "total": 4,
    "valid": 3,
    "errors": 0,
    "duplicates_in_payload": 1,
    "upserts_against_db": 2
  },
  "can_confirm": true,
  "items": [
    {
      "normalized": {
        "category": "MOOD",
        "slug": "uplifting",
        "name": "Uplifting",
        "description": "Positive, energetic mood"
      },
      "action": "insert",
      "errors": [],
      "warnings": []
    },
    {
      "normalized": {
        "category": "MOOD",
        "slug": "uplifting",
        "name": "Uplifting",
        "description": "Positive, energetic mood"
      },
      "action": "duplicate_ignored",
      "errors": [],
      "warnings": [
        "duplicate payload row ignored (identical normalized values)"
      ]
    },
    {
      "normalized": {
        "category": "THEME",
        "slug": "retro-wave",
        "name": "Retrowave",
        "description": null
      },
      "action": "update",
      "errors": [],
      "warnings": [
        "existing tag matched by natural key (category, slug); will upsert"
      ]
    }
  ]
}
```

## 2) Confirm response example
Endpoint: `POST /v1/track-catalog/custom-tags/bulk/confirm`

```json
{
  "summary": {
    "total": 3,
    "inserted": 1,
    "updated": 1,
    "unchanged": 1
  },
  "inserted": 1,
  "updated": 1,
  "unchanged": 1
}
```

## 3) Atomicity confirmation
- Invalid/conflicting preview rows block confirm writes.
- Confirm remains atomic.
- No partial DB writes occur on invalid confirm.
