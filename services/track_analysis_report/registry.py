from __future__ import annotations

from typing import Final, TypedDict


class ColumnEntry(TypedDict):
    key: str
    group: str
    source: str
    path: str
    flatten: str


COLUMN_GROUPS: Final[tuple[str, ...]] = (
    "track_identity",
    "analysis_meta",
    "audio_levels",
    "speech",
    "tags",
    "scores",
    "other_analysis_fields",
    "Quality",
    "Dynamics",
    "Timbre / Texture",
    "Structure",
    "Voice",
    "Semantic",
    "Playlist Fit",
    "Transition",
    "Suitability",
    "Metadata / Versioning",
    "Advanced JSON Fields",
)

VALID_FLATTEN_RULES: Final[frozenset[str]] = frozenset(
    {"direct", "join_csv", "json_string", "unix_ts_iso"}
)

# Ordered to match Appendix A (SPEC_TRACK_ANALYSIS_REPORT_v1.1).
COLUMN_REGISTRY: Final[tuple[ColumnEntry, ...]] = (
    # track_identity
    {"key": "track_pk", "group": "track_identity", "source": "tracks", "path": "id", "flatten": "direct"},
    {"key": "channel_slug", "group": "track_identity", "source": "tracks", "path": "channel_slug", "flatten": "direct"},
    {"key": "track_id", "group": "track_identity", "source": "tracks", "path": "track_id", "flatten": "direct"},
    {"key": "gdrive_file_id", "group": "track_identity", "source": "tracks", "path": "gdrive_file_id", "flatten": "direct"},
    {"key": "source", "group": "track_identity", "source": "tracks", "path": "source", "flatten": "direct"},
    {"key": "filename", "group": "track_identity", "source": "tracks", "path": "filename", "flatten": "direct"},
    {"key": "title", "group": "track_identity", "source": "tracks", "path": "title", "flatten": "direct"},
    {"key": "artist", "group": "track_identity", "source": "tracks", "path": "artist", "flatten": "direct"},
    {"key": "duration_sec", "group": "track_identity", "source": "tracks", "path": "duration_sec", "flatten": "direct"},
    # analysis_meta
    {"key": "discovered_at", "group": "analysis_meta", "source": "tracks", "path": "discovered_at", "flatten": "unix_ts_iso"},
    {"key": "analyzed_at", "group": "analysis_meta", "source": "tracks", "path": "analyzed_at", "flatten": "unix_ts_iso"},
    {"key": "features_computed_at", "group": "analysis_meta", "source": "features", "path": "computed_at", "flatten": "unix_ts_iso"},
    {"key": "tags_computed_at", "group": "analysis_meta", "source": "tags", "path": "computed_at", "flatten": "unix_ts_iso"},
    {"key": "scores_computed_at", "group": "analysis_meta", "source": "scores", "path": "computed_at", "flatten": "unix_ts_iso"},
    {"key": "analysis_status", "group": "analysis_meta", "source": "features", "path": "payload_json.analysis_status", "flatten": "direct"},
    # audio_levels
    {"key": "true_peak_dbfs", "group": "audio_levels", "source": "features", "path": "payload_json.true_peak_dbfs", "flatten": "direct"},
    {"key": "spikes_found", "group": "audio_levels", "source": "features", "path": "payload_json.spikes_found", "flatten": "direct"},
    {"key": "prohibited_cues_flags_clipping_detected", "group": "audio_levels", "source": "tags", "path": "payload_json.prohibited_cues.flags.clipping_detected", "flatten": "direct"},
    {"key": "prohibited_cues_flags_abrupt_gain_jumps", "group": "audio_levels", "source": "tags", "path": "payload_json.prohibited_cues.flags.abrupt_gain_jumps", "flatten": "direct"},
    {"key": "prohibited_cues_metrics_frame_rms_std", "group": "audio_levels", "source": "tags", "path": "payload_json.prohibited_cues.metrics.frame_rms_std", "flatten": "direct"},
    {"key": "prohibited_cues_metrics_max_rms_delta", "group": "audio_levels", "source": "tags", "path": "payload_json.prohibited_cues.metrics.max_rms_delta", "flatten": "direct"},
    # speech
    {"key": "voice_flag", "group": "speech", "source": "features", "path": "payload_json.voice_flag", "flatten": "direct"},
    {"key": "voice_flag_reason", "group": "speech", "source": "features", "path": "payload_json.voice_flag_reason", "flatten": "direct"},
    {"key": "speech_flag", "group": "speech", "source": "features", "path": "payload_json.speech_flag", "flatten": "direct"},
    {"key": "speech_flag_reason", "group": "speech", "source": "features", "path": "payload_json.speech_flag_reason", "flatten": "direct"},
    {"key": "yamnet_agg_voice_prob", "group": "speech", "source": "features", "path": "payload_json.yamnet_agg.voice_prob", "flatten": "direct"},
    {"key": "yamnet_agg_speech_prob", "group": "speech", "source": "features", "path": "payload_json.yamnet_agg.speech_prob", "flatten": "direct"},
    {"key": "yamnet_agg_singing_prob", "group": "speech", "source": "features", "path": "payload_json.yamnet_agg.singing_prob", "flatten": "direct"},
    {"key": "yamnet_agg_voice_labels_used", "group": "speech", "source": "features", "path": "payload_json.yamnet_agg.voice_labels_used", "flatten": "join_csv"},
    {"key": "yamnet_agg_speech_labels_used", "group": "speech", "source": "features", "path": "payload_json.yamnet_agg.speech_labels_used", "flatten": "join_csv"},
    # tags
    {"key": "yamnet_tags", "group": "tags", "source": "tags", "path": "payload_json.yamnet_tags", "flatten": "join_csv"},
    {"key": "dominant_texture", "group": "tags", "source": "features", "path": "payload_json.dominant_texture", "flatten": "direct"},
    {"key": "texture_backend", "group": "tags", "source": "features", "path": "payload_json.texture_backend", "flatten": "direct"},
    {"key": "texture_confidence", "group": "tags", "source": "features", "path": "payload_json.texture_confidence", "flatten": "direct"},
    {"key": "texture_reason", "group": "tags", "source": "features", "path": "payload_json.texture_reason", "flatten": "direct"},
    {"key": "prohibited_cues_notes", "group": "tags", "source": "tags", "path": "payload_json.prohibited_cues_notes", "flatten": "direct"},
    {"key": "custom_tags_visual", "group": "tags", "source": "effective_custom_tags", "path": "visual", "flatten": "join_csv"},
    {"key": "custom_tags_mood", "group": "tags", "source": "effective_custom_tags", "path": "mood", "flatten": "join_csv"},
    {"key": "custom_tags_theme", "group": "tags", "source": "effective_custom_tags", "path": "theme", "flatten": "join_csv"},
    # scores
    {"key": "dsp_score", "group": "scores", "source": "scores", "path": "payload_json.dsp_score", "flatten": "direct"},
    {"key": "dsp_score_version", "group": "scores", "source": "scores", "path": "payload_json.dsp_score_version", "flatten": "direct"},
    {"key": "dsp_components", "group": "scores", "source": "scores", "path": "payload_json.dsp_components", "flatten": "json_string"},
    {"key": "dsp_notes", "group": "scores", "source": "scores", "path": "payload_json.dsp_notes", "flatten": "direct"},
    # other_analysis_fields
    {"key": "yamnet_top_classes", "group": "other_analysis_fields", "source": "features", "path": "payload_json.yamnet_top_classes", "flatten": "json_string"},
    {"key": "yamnet_probabilities", "group": "other_analysis_fields", "source": "features", "path": "payload_json.yamnet_probabilities", "flatten": "json_string"},
    {"key": "prohibited_cues", "group": "other_analysis_fields", "source": "tags", "path": "payload_json.prohibited_cues", "flatten": "json_string"},
    {"key": "missing_fields", "group": "other_analysis_fields", "source": "features", "path": "payload_json.missing_fields", "flatten": "join_csv"},
    # Quality
    {"key": "quality_integrated_lufs", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.integrated_lufs", "flatten": "direct"},
    {"key": "quality_loudness_range_lra", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.loudness_range_lra", "flatten": "direct"},
    {"key": "quality_true_peak_dbfs", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.true_peak_dbfs", "flatten": "direct"},
    {"key": "quality_clipping_ratio", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.clipping_ratio", "flatten": "direct"},
    {"key": "quality_noise_floor_estimate", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.noise_floor_estimate", "flatten": "direct"},
    {"key": "quality_silence_ratio", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.silence_ratio", "flatten": "direct"},
    {"key": "quality_intro_silence_ratio", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.intro_silence_ratio", "flatten": "direct"},
    {"key": "quality_outro_silence_ratio", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.outro_silence_ratio", "flatten": "direct"},
    {"key": "quality_stereo_width", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.stereo_width", "flatten": "direct"},
    {"key": "quality_mono_compatibility", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.mono_compatibility", "flatten": "direct"},
    {"key": "quality_sample_rate", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.sample_rate", "flatten": "direct"},
    {"key": "quality_channels_count", "group": "Quality", "source": "features", "path": "payload_json.advanced_v1.quality.channels_count", "flatten": "direct"},
    # Dynamics
    {"key": "dynamics_energy_mean", "group": "Dynamics", "source": "features", "path": "payload_json.advanced_v1.dynamics.energy_mean", "flatten": "direct"},
    {"key": "dynamics_energy_variance", "group": "Dynamics", "source": "features", "path": "payload_json.advanced_v1.dynamics.energy_variance", "flatten": "direct"},
    {"key": "dynamics_dynamic_stability", "group": "Dynamics", "source": "features", "path": "payload_json.advanced_v1.dynamics.dynamic_stability", "flatten": "direct"},
    {"key": "dynamics_transient_density", "group": "Dynamics", "source": "features", "path": "payload_json.advanced_v1.dynamics.transient_density", "flatten": "direct"},
    {"key": "dynamics_pulse_strength", "group": "Dynamics", "source": "features", "path": "payload_json.advanced_v1.dynamics.pulse_strength", "flatten": "direct"},
    {"key": "dynamics_tempo_estimate", "group": "Dynamics", "source": "features", "path": "payload_json.advanced_v1.dynamics.tempo_estimate", "flatten": "direct"},
    {"key": "dynamics_tempo_confidence", "group": "Dynamics", "source": "features", "path": "payload_json.advanced_v1.dynamics.tempo_confidence", "flatten": "direct"},
    {"key": "dynamics_event_density", "group": "Dynamics", "source": "features", "path": "payload_json.advanced_v1.dynamics.event_density", "flatten": "direct"},
    # Timbre / Texture
    {"key": "timbre_brightness", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.brightness", "flatten": "direct"},
    {"key": "timbre_warmth", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.warmth", "flatten": "direct"},
    {"key": "timbre_darkness", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.darkness", "flatten": "direct"},
    {"key": "timbre_spectral_centroid_mean", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.spectral_centroid_mean", "flatten": "direct"},
    {"key": "timbre_spectral_rolloff_mean", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.spectral_rolloff_mean", "flatten": "direct"},
    {"key": "timbre_low_end_weight", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.low_end_weight", "flatten": "direct"},
    {"key": "timbre_high_end_sharpness", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.high_end_sharpness", "flatten": "direct"},
    {"key": "timbre_harmonic_density", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.harmonic_density", "flatten": "direct"},
    {"key": "timbre_tonal_stability", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.tonal_stability", "flatten": "direct"},
    {"key": "timbre_drone_presence", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.drone_presence", "flatten": "direct"},
    {"key": "timbre_pad_presence", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.pad_presence", "flatten": "direct"},
    {"key": "timbre_percussion_presence", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.percussion_presence", "flatten": "direct"},
    {"key": "timbre_melodic_prominence", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.melodic_prominence", "flatten": "direct"},
    {"key": "timbre_texture_smoothness", "group": "Timbre / Texture", "source": "features", "path": "payload_json.advanced_v1.timbre.texture_smoothness", "flatten": "direct"},
    # Structure
    {"key": "structure_intro_energy", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.intro_energy", "flatten": "direct"},
    {"key": "structure_early_section_energy", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.early_section_energy", "flatten": "direct"},
    {"key": "structure_middle_section_energy", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.middle_section_energy", "flatten": "direct"},
    {"key": "structure_late_section_energy", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.late_section_energy", "flatten": "direct"},
    {"key": "structure_outro_energy", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.outro_energy", "flatten": "direct"},
    {"key": "structure_intro_smoothness", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.intro_smoothness", "flatten": "direct"},
    {"key": "structure_outro_smoothness", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.outro_smoothness", "flatten": "direct"},
    {"key": "structure_structural_stability", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.structural_stability", "flatten": "direct"},
    {"key": "structure_climax_presence", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.climax_presence", "flatten": "direct"},
    {"key": "structure_abruptness_score", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.abruptness_score", "flatten": "direct"},
    {"key": "structure_loop_friendliness", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.loop_friendliness", "flatten": "direct"},
    {"key": "structure_fade_friendliness", "group": "Structure", "source": "features", "path": "payload_json.advanced_v1.structure.fade_friendliness", "flatten": "direct"},
    # Voice
    {"key": "voice_speech_probability", "group": "Voice", "source": "features", "path": "payload_json.advanced_v1.voice.speech_probability", "flatten": "direct"},
    {"key": "voice_vocal_probability", "group": "Voice", "source": "features", "path": "payload_json.advanced_v1.voice.vocal_probability", "flatten": "direct"},
    {"key": "voice_spoken_word_density", "group": "Voice", "source": "features", "path": "payload_json.advanced_v1.voice.spoken_word_density", "flatten": "direct"},
    {"key": "voice_human_presence_score", "group": "Voice", "source": "features", "path": "payload_json.advanced_v1.voice.human_presence_score", "flatten": "direct"},
    {"key": "voice_tags_csv", "group": "Voice", "source": "tags", "path": "payload_json.advanced_v1.voice_tags", "flatten": "join_csv"},
    # Semantic
    {"key": "semantic_focus", "group": "Semantic", "source": "scores", "path": "payload_json.advanced_v1.semantic.functional_scores.focus", "flatten": "direct"},
    {"key": "semantic_energy", "group": "Semantic", "source": "scores", "path": "payload_json.advanced_v1.semantic.functional_scores.energy", "flatten": "direct"},
    {"key": "semantic_narrative", "group": "Semantic", "source": "scores", "path": "payload_json.advanced_v1.semantic.functional_scores.narrative", "flatten": "direct"},
    {"key": "semantic_background_compatibility", "group": "Semantic", "source": "scores", "path": "payload_json.advanced_v1.semantic.functional_scores.background_compatibility", "flatten": "direct"},
    {"key": "mood_tags_csv", "group": "Semantic", "source": "tags", "path": "payload_json.advanced_v1.semantic.mood_tags", "flatten": "join_csv"},
    {"key": "theme_tags_csv", "group": "Semantic", "source": "tags", "path": "payload_json.advanced_v1.semantic.theme_tags", "flatten": "join_csv"},
    # Playlist Fit
    {"key": "playlist_continuity_score", "group": "Playlist Fit", "source": "scores", "path": "payload_json.advanced_v1.playlist_fit.continuity_score", "flatten": "direct"},
    {"key": "playlist_mixability_score", "group": "Playlist Fit", "source": "scores", "path": "payload_json.advanced_v1.playlist_fit.mixability_score", "flatten": "direct"},
    {"key": "playlist_variety_support_score", "group": "Playlist Fit", "source": "scores", "path": "payload_json.advanced_v1.playlist_fit.variety_support_score", "flatten": "direct"},
    # Transition
    {"key": "transition_intro_profile", "group": "Transition", "source": "scores", "path": "payload_json.advanced_v1.transition.intro_profile", "flatten": "direct"},
    {"key": "transition_outro_profile", "group": "Transition", "source": "scores", "path": "payload_json.advanced_v1.transition.outro_profile", "flatten": "direct"},
    {"key": "transition_risk_score", "group": "Transition", "source": "scores", "path": "payload_json.advanced_v1.transition.transition_risk_score", "flatten": "direct"},
    # Suitability
    {"key": "suitability_content_type_fit_score", "group": "Suitability", "source": "scores", "path": "payload_json.advanced_v1.suitability.content_type_fit_score", "flatten": "direct"},
    {"key": "suitability_channel_fit_score", "group": "Suitability", "source": "scores", "path": "payload_json.advanced_v1.suitability.channel_fit_score", "flatten": "direct"},
    {"key": "suitability_selected_content_context", "group": "Suitability", "source": "scores", "path": "payload_json.advanced_v1.suitability.selected_content_context", "flatten": "direct"},
    # Metadata / Versioning
    {"key": "analyzer_version", "group": "Metadata / Versioning", "source": "features", "path": "payload_json.advanced_v1.meta.analyzer_version", "flatten": "direct"},
    {"key": "schema_version", "group": "Metadata / Versioning", "source": "features", "path": "payload_json.advanced_v1.meta.schema_version", "flatten": "direct"},
    # Advanced JSON Fields
    {"key": "similarity_diversity_penalty_base", "group": "Advanced JSON Fields", "source": "features", "path": "payload_json.advanced_v1.similarity.diversity_penalty_base", "flatten": "direct"},
    {"key": "intensity_curve_summary_json", "group": "Advanced JSON Fields", "source": "features", "path": "payload_json.advanced_v1.dynamics.intensity_curve_summary", "flatten": "json_string"},
    {"key": "section_summary_json", "group": "Advanced JSON Fields", "source": "features", "path": "payload_json.advanced_v1.structure.section_summary", "flatten": "json_string"},
    {"key": "normalized_feature_vector_json", "group": "Advanced JSON Fields", "source": "features", "path": "payload_json.advanced_v1.similarity.normalized_feature_vector", "flatten": "json_string"},
    {"key": "warning_codes_json", "group": "Advanced JSON Fields", "source": "scores", "path": "payload_json.advanced_v1.final_decisions.warning_codes", "flatten": "json_string"},
    {"key": "hard_veto", "group": "Advanced JSON Fields", "source": "scores", "path": "payload_json.advanced_v1.final_decisions.hard_veto", "flatten": "direct"},
    {"key": "soft_penalty_total", "group": "Advanced JSON Fields", "source": "scores", "path": "payload_json.advanced_v1.final_decisions.soft_penalty_total", "flatten": "direct"},
    {"key": "rule_trace_json", "group": "Advanced JSON Fields", "source": "scores", "path": "payload_json.advanced_v1.rule_trace", "flatten": "json_string"},
    {"key": "suitability_content_type_fit_by_context_json", "group": "Advanced JSON Fields", "source": "scores", "path": "payload_json.advanced_v1.suitability.content_type_fit_by_context", "flatten": "json_string"},
    {"key": "classifier_evidence_yamnet_top_classes_json", "group": "Advanced JSON Fields", "source": "tags", "path": "payload_json.advanced_v1.classifier_evidence.yamnet_top_classes", "flatten": "json_string"},
)
