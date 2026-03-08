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
)
