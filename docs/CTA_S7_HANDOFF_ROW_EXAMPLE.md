# CTA-S7 Follow-up Handoff: Concrete Report Row Example

This handoff artifact closes the CTA-S7 acceptance gap by providing a concrete row fragment from the report API integration fixture.

## Example report row fragment (concrete)

```json
{
  "custom_tags_visual": "A Forest, Z Forest",
  "custom_tags_mood": "Calm",
  "custom_tags_theme": "Night"
}
```

## Behavior confirmation

- Values are **effective tags only** (includes `AUTO` and `MANUAL`; excludes `SUPPRESSED`).
- Labels are joined by `", "`.
- Labels are alphabetically ordered inside each category cell.
- Suppressed tags are excluded from the flattened output.

## XLSX fidelity confirmation

The XLSX export uses the same flattened values as the report API for these custom tag columns. This is verified by integration test coverage that compares API and XLSX row values for:

- `custom_tags_visual`
- `custom_tags_mood`
- `custom_tags_theme`
