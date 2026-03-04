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

## Voice/speech aggregation (`yamnet_agg`)

`track_features.payload_json` now includes a machine-readable YAMNet aggregation block:

- `yamnet_agg.voice_prob`: sum of YAMNet class scores for configurable `VOICE_LABELS` (includes `Singing`)
- `yamnet_agg.speech_prob`: sum of class scores for configurable `SPEECH_LABELS`
- `yamnet_agg.singing_prob`: score for `Singing` class (or `0` when absent)
- `yamnet_agg.voice_labels_used`: labels that contributed to `voice_prob`
- `yamnet_agg.speech_labels_used`: labels that contributed to `speech_prob`
- `yamnet_agg.source`: `full_vector` when full per-label scores are available, otherwise `top_classes`
- `yamnet_agg.top_classes_count`: number of entries in `yamnet_top_classes` used for readability
- `yamnet_agg.total_labels_count`: present when `source = full_vector`

`yamnet_top_classes` remains backward compatible and now stores top `YAMNET_TOP_N = 20` labels by default.
Legacy `yamnet_probabilities` and `yamnet_tags` are unchanged.

Automation helpers:

- `voice_flag` (`bool`)
- `voice_flag_reason` (`str`) with explicit threshold explanation
- `speech_flag` (`bool`)
- `speech_flag_reason` (`str`) with explicit threshold explanation

Current thresholds are constants in analyzer code:

- `VOICE_MIN_PROB = 0.20`
- `SINGING_MIN_PROB = 0.08`
- `SPEECH_MIN_PROB = 0.10`

## Prohibited cues structured output

`track_tags.payload_json` keeps `prohibited_cues_notes` and now also includes:

- `prohibited_cues.backend`: `fallback` (or `primary` in future backends)
- `prohibited_cues.checks_run`: ordered list of checks run
- `prohibited_cues.flags`: boolean flags
- `prohibited_cues.metrics`: numeric values for automation/debugging

Current fallback checks include:

- existing metrics: `true_peak_dbfs`, `spikes_found`
- clipping detection (`clipping_detected`)
- silence gap detection (`silence_gaps`)
- abrupt frame-RMS jump detection (`abrupt_gain_jumps`)

All checks are deterministic and frame-based with lightweight numpy math.

## DSP score (`dsp_score`) v1

`track_scores.payload_json` keeps legacy `dsp_score` and now includes:

- `dsp_score_version = "v1"`
- `dsp_components` (all normalized `0..1`)
- `dsp_notes` (short explanation)

`v1` components:

- `headroom_component`: derived from `true_peak_dbfs` (more headroom -> higher)
- `stability_component`: derived from frame RMS standard deviation (lower variance -> higher)
- `spikes_component`: penalized when spikes are detected
- `clipping_component`: penalized when clipping is detected
- `silence_component`: penalized when silence gaps are detected

`dsp_score` is a weighted sum of components, clamped to `[0,1]`.

## `missing_fields` semantics

`missing_fields` tracks **required scalar metrics only**.
Texture is treated as optional enrichment for now, so `dominant_texture` is not added to `missing_fields`.
