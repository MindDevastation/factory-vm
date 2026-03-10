# Track Analyzer Output Schema (Canonical)

This document defines the canonical analyzer payload contract currently written by `services/track_analyzer/analyze.py`.

Compatibility policy:

- Top-level legacy keys remain present.
- `advanced_v1` is additive and must not remove/rename legacy keys.
- If `advanced_v1` exists, version markers are required at `advanced_v1.meta`:
  - `analyzer_version`
  - `schema_version`

Current values:

- `advanced_v1.meta.analyzer_version = "advanced_track_analyzer_v1.1"`
- `advanced_v1.meta.schema_version = "advanced_v1"`

---

## 1) `track_features.payload_json`

### Legacy top-level keys (preserved)

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

## 2) `track_tags.payload_json`

### Legacy top-level keys (preserved)

- `yamnet_tags`
- `prohibited_cues_notes`
- `prohibited_cues`
- `analysis_status`
- `missing_fields`

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

## 3) `track_scores.payload_json`

### Legacy top-level keys (preserved)

- `dsp_score`
- `dsp_score_version`
- `dsp_components`
- `dsp_notes`
- `analysis_status`
- `missing_fields`

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

## 4) Compatibility assertions covered by tests

- Legacy keys remain readable/present for features, tags, and scores.
- `advanced_v1` is additive and does not replace legacy keys.
- `advanced_v1.meta.analyzer_version` and `advanced_v1.meta.schema_version` are written when `advanced_v1` is present.
- Custom-tag `source_path` resolution supports both legacy paths and additive `advanced_v1` paths.

