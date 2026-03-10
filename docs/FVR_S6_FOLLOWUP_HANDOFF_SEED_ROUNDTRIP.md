# FVR-S6 follow-up handoff (seed roundtrip)

## Scope confirmation
- Runtime behavior is unchanged in this follow-up slice.
- This handoff only adds concrete evidence artifacts for the existing custom-tags seed/export/import lifecycle.

## Concrete seed JSON example (script-backed)
Source execution: seed roundtrip flow matching `tests/integration/test_custom_tags_seed_roundtrip.py::test_export_import_roundtrip_and_creates_missing_seed_dir`.

```json
{
  "schema_version": "custom_tags_seed/1",
  "category": "VISUAL",
  "exported_at": "2026-03-10T08:17:11.574534+00:00",
  "tags": [
    {
      "slug": "aurora",
      "name": "Aurora",
      "description": "northern lights",
      "is_active": true
    }
  ]
}
```

## Export create-dir/files confirmation
On first export to an absent seed path:
- `data/seeds/custom_tags` is created.
- `visual_tags.json`, `mood_tags.json`, and `theme_tags.json` are created.

## Concrete roundtrip result (script-backed)

```text
export_created_dir=True
export_files=["visual_tags.json","mood_tags.json","theme_tags.json"]
roundtrip_imported=2
db_rows_after_import=2
```
