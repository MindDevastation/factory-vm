# Track Analyzer Output Notes

## Texture metadata (P0 observability)

`track_features.payload_json` keeps `dominant_texture` for backward compatibility and now always includes:

- `texture_backend`: one of `none`, `heuristic`, `model`, `fallback`
- `texture_confidence`: float in `[0,1]` or `null`
- `texture_reason`: one of `not_implemented`, `disabled`, `missing_model`, `low_confidence`, `exception`, `unknown`

Current default path is a non-classifying placeholder:

- `dominant_texture = "unknown texture"`
- `texture_backend = "none"`
- `texture_confidence = null`
- `texture_reason = "not_implemented"`

If texture analysis raises an exception, payload becomes:

- `dominant_texture = "unknown texture"`
- `texture_backend = "none"` (current backend)
- `texture_confidence = null`
- `texture_reason = "exception"`

## `missing_fields` semantics

`missing_fields` tracks **required scalar metrics only**.
Texture is treated as optional enrichment for now, so `dominant_texture` is not added to `missing_fields` when texture backend is `none`.
