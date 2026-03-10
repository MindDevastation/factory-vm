# Track Analyzer Output Schema (Canonical)

## Scope and compatibility note

This document defines the analyzer payload contract written by `services/track_analyzer/analyze.py`.

Compatibility contract:

- Legacy top-level keys remain present.
- `advanced_v1` is additive and does not remove or rename legacy keys.
- When `advanced_v1` is present, version markers are required in `advanced_v1.meta`:
  - `analyzer_version`
  - `schema_version`

Current runtime marker values:

- `advanced_v1.meta.analyzer_version = "advanced_track_analyzer_v1.1"`
- `advanced_v1.meta.schema_version = "advanced_v1"`

---

## `track_features.payload_json`

### Preserved legacy keys (runtime-emitted)

- `duration_sec`
- `true_peak_dbfs`
- `spikes_found`
- `yamnet_top_classes`
- `yamnet_probabilities`
- `yamnet_agg`
- `voice_flag`
- `voice_flag_reason`
- `speech_flag`
- `speech_flag_reason`
- `dominant_texture`
- `texture_backend`
- `texture_confidence`
- `texture_reason`
- `analysis_status`
- `missing_fields`

### Legacy compatibility-read keys (not analyzer-emitted in current runtime)

- `scene`
- `mood`

### Additive `advanced_v1` keys

- `advanced_v1.meta`
  - `analyzer_version`
  - `schema_version`
  - `analyzed_at`
  - `rollout_tier`
  - `segment_policy`
- `advanced_v1.profiles`
  - `LONG_INSTRUMENTAL_AMBIENT` (object)
  - `LONG_LYRICAL` (object)
- `advanced_v1.quality`
- `advanced_v1.dynamics`
- `advanced_v1.timbre`
- `advanced_v1.structure`
- `advanced_v1.voice`
- `advanced_v1.similarity`

---

## `track_tags.payload_json`

### Preserved legacy keys (runtime-emitted)

- `yamnet_tags`
- `prohibited_cues_notes`
- `prohibited_cues`
- `analysis_status`
- `missing_fields`

### Legacy compatibility-read keys (not analyzer-emitted in current runtime)

- `scene`
- `mood`

### Additive `advanced_v1` keys

- `advanced_v1.meta` (same required markers as above)
- `advanced_v1.profiles`
- `advanced_v1.semantic`
  - `mood_tags`
  - `theme_tags`
- `advanced_v1.voice_tags`
- `advanced_v1.classifier_evidence`
  - `yamnet_top_classes`

---

## `track_scores.payload_json`

### Preserved legacy keys (runtime-emitted)

- `dsp_score`
- `dsp_score_version`
- `dsp_components`
- `dsp_notes`
- `analysis_status`
- `missing_fields`

### Legacy compatibility-read keys (not analyzer-emitted in current runtime)

- `safety`
- `scene_match`

### Additive `advanced_v1` keys

- `advanced_v1.meta` (same required markers as above)
- `advanced_v1.profiles`
- `advanced_v1.semantic.functional_scores`
  - `focus`
  - `energy`
  - `narrative`
  - `background_compatibility`
- `advanced_v1.playlist_fit`
- `advanced_v1.transition`
- `advanced_v1.suitability`
  - `content_type_fit_score`
  - `channel_fit_score`
  - `context_scores`
- `advanced_v1.rule_trace`
- `advanced_v1.final_decisions`
  - `hard_veto`
  - `soft_penalty_total`
  - `warning_codes`

---

## Required version markers

- For every payload that includes `advanced_v1`, `advanced_v1.meta.analyzer_version` is required.
- For every payload that includes `advanced_v1`, `advanced_v1.meta.schema_version` is required.

---

## Compatibility guarantees

- Existing legacy top-level keys remain available.
- `advanced_v1` remains additive over legacy payloads.
- Analyzer write path emits `analyzer_version` and `schema_version` whenever `advanced_v1` is present.
- This document reflects actual runtime payload construction, not an aspirational future schema.

---

## Source-path compatibility notes

- Existing source-path compatibility coverage remains valid for both:
  - legacy top-level paths (for example, `track_features.payload_json.voice_flag`)
  - additive `advanced_v1` paths (for example, `track_features.payload_json.advanced_v1.voice.human_presence_score`)
- Historical legacy keys used by some downstream/report flows (`scene`, `mood`, `safety`, `scene_match`) remain documented as compatibility-read keys above.
