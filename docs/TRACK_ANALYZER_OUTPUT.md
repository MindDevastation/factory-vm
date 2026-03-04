# Track Analyzer Output Notes

## Texture metadata

`track_features.payload_json` keeps `dominant_texture` for backward compatibility and includes:

- `texture_backend`: one of `none`, `heuristic`, `model`, `fallback`
- `texture_confidence`: float in `[0,1]` or `null`
- `texture_reason`: one of `ok`, `low_confidence`, `exception`, `disabled`, `missing_model`, `unknown`

Current default implementation uses a lightweight waveform heuristic backend.

### Heuristic texture labels (playlisting-safe)

`dominant_texture` is one of:

- `tonal_sustained`
- `percussive_rhythmic`
- `noisy_distorted`
- `mixed`

These labels are intentionally small/stable for playlist rules and automation. Suggested usage:

- Prefer `tonal_sustained` for ambient/bed playlists.
- Prefer `percussive_rhythmic` for motion/trailer playlists.
- Route `noisy_distorted` to aggressive/experimental bins.
- Keep `mixed` as a fallback bucket when tracks do not present a clear dominant texture.

If confidence is low (`< 0.35`), analyzer returns `dominant_texture = "mixed"` with `texture_reason = "low_confidence"`.

If texture analysis raises an exception, payload becomes:

- `dominant_texture = "unknown texture"`
- `texture_backend = "heuristic"`
- `texture_confidence = null`
- `texture_reason = "exception"`

## `missing_fields` semantics

`missing_fields` tracks **required scalar metrics only**.
Texture is treated as optional enrichment for now, so `dominant_texture` is not added to `missing_fields`.
