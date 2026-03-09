from __future__ import annotations

import logging
import re
import shutil
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from services.common import db as dbm
from services.common import ffmpeg
from services.custom_tags.auto_assign import apply_auto_custom_tags
from services.track_analyzer.advanced_metrics import compute_dynamics_metrics, compute_quality_metrics
import services.track_analyzer.yamnet as yamnet
from services.track_analyzer.texture_heuristics import classify_texture
from services.track_analyzer.yamnet_buckets import SPEECH_LABELS, VOICE_LABELS


class AnalyzeError(RuntimeError):
    pass


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AnalyzeStats:
    selected: int
    processed: int
    failed: int


_TRUE_PEAK_RE = re.compile(r"(?:true\s+peak|peak)\s*[:=]\s*(-?\d+(?:\.\d+)?)\s*dB", re.IGNORECASE)
VOICE_MIN_PROB = 0.2
SINGING_MIN_PROB = 0.08
SPEECH_MIN_PROB = 0.10
DSP_SCORE_VERSION = "v1"
ADVANCED_ANALYZER_VERSION = "advanced_track_analyzer_v1.1"
ADVANCED_SCHEMA_VERSION = "advanced_v1"
ADVANCED_ROLLOUT_TIER = "s1"
ADVANCED_SEGMENT_POLICY = "track_full"
P0_CONTEXTS = ("LONG_INSTRUMENTAL_AMBIENT", "LONG_LYRICAL")
P0_CHANNEL_ARCHETYPE_BY_SLUG = {
    "darkwood-reverie": "LONG_INSTRUMENTAL_AMBIENT",
}
SILENCE_RMS_THRESHOLD = 0.01
SILENCE_GAP_MIN_MS = 1000
SILENCE_IGNORE_EDGE_MS = 2000
SIMILARITY_VECTOR_ORDER = [
    "timbre.brightness",
    "timbre.warmth",
    "timbre.darkness",
    "timbre.spectral_centroid_mean",
    "timbre.spectral_rolloff_mean",
    "timbre.low_end_weight",
    "timbre.high_end_sharpness",
    "timbre.harmonic_density",
    "timbre.tonal_stability",
    "timbre.drone_presence",
    "timbre.pad_presence",
    "timbre.percussion_presence",
    "timbre.melodic_prominence",
    "timbre.texture_smoothness",
    "structure.intro_energy",
    "structure.early_section_energy",
    "structure.middle_section_energy",
    "structure.late_section_energy",
    "structure.outro_energy",
    "structure.intro_smoothness",
    "structure.outro_smoothness",
    "structure.structural_stability",
    "structure.climax_presence",
    "structure.abruptness_score",
    "structure.loop_friendliness",
    "structure.fade_friendliness",
    "voice.speech_probability",
    "voice.vocal_probability",
    "voice.spoken_word_density",
    "voice.human_presence_score",
]

FEATURES_REQUIRED_ADVANCED_SCALAR_PATHS = (
    "quality.duration_sec",
    "quality.integrated_lufs",
    "quality.loudness_range_lra",
    "quality.true_peak_dbfs",
    "quality.clipping_ratio",
    "quality.noise_floor_estimate",
    "quality.silence_ratio",
    "quality.intro_silence_ratio",
    "quality.outro_silence_ratio",
    "quality.stereo_width",
    "quality.mono_compatibility",
    "quality.sample_rate",
    "quality.channels_count",
    "dynamics.energy_mean",
    "dynamics.energy_variance",
    "dynamics.dynamic_stability",
    "dynamics.transient_density",
    "dynamics.pulse_strength",
    "dynamics.tempo_estimate",
    "dynamics.tempo_confidence",
    "dynamics.event_density",
    "dynamics.intensity_curve_summary.start_mean",
    "dynamics.intensity_curve_summary.middle_mean",
    "dynamics.intensity_curve_summary.end_mean",
    "dynamics.intensity_curve_summary.linear_slope",
    "dynamics.intensity_curve_summary.peak_position_ratio",
    "dynamics.intensity_curve_summary.convexity_hint",
    "timbre.brightness",
    "timbre.warmth",
    "timbre.darkness",
    "timbre.spectral_centroid_mean",
    "timbre.spectral_rolloff_mean",
    "timbre.low_end_weight",
    "timbre.high_end_sharpness",
    "timbre.harmonic_density",
    "timbre.tonal_stability",
    "timbre.drone_presence",
    "timbre.pad_presence",
    "timbre.percussion_presence",
    "timbre.melodic_prominence",
    "timbre.texture_smoothness",
    "structure.intro_energy",
    "structure.early_section_energy",
    "structure.middle_section_energy",
    "structure.late_section_energy",
    "structure.outro_energy",
    "structure.intro_smoothness",
    "structure.outro_smoothness",
    "structure.structural_stability",
    "structure.climax_presence",
    "structure.abruptness_score",
    "structure.loop_friendliness",
    "structure.fade_friendliness",
    "voice.speech_probability",
    "voice.vocal_probability",
    "voice.spoken_word_density",
    "voice.human_presence_score",
    "similarity.diversity_penalty_base",
)

FEATURES_REQUIRED_ADVANCED_OBJECT_PATHS = ("structure.section_summary", "dynamics.intensity_curve_summary")
FEATURES_REQUIRED_ADVANCED_LIST_PATHS = ("similarity.normalized_feature_vector",)

SCORES_REQUIRED_ADVANCED_SCALAR_PATHS = (
    "semantic.functional_scores.focus",
    "semantic.functional_scores.energy",
    "semantic.functional_scores.narrative",
    "semantic.functional_scores.background_compatibility",
    "playlist_fit",
    "transition",
    "suitability.content_type_fit_score",
    "final_decisions.hard_veto",
    "final_decisions.soft_penalty_total",
)
SCORES_REQUIRED_ADVANCED_OBJECT_PATHS = ()
SCORES_REQUIRED_ADVANCED_LIST_PATHS = ("final_decisions.warning_codes", "rule_trace")

TAGS_REQUIRED_ADVANCED_OBJECT_PATHS = ("semantic", "classifier_evidence")
TAGS_REQUIRED_ADVANCED_LIST_PATHS = (
    "semantic.mood_tags",
    "semantic.theme_tags",
    "classifier_evidence.yamnet_top_classes",
    "voice_tags",
)


def analyze_tracks(
    conn: Any,
    drive: Any,
    *,
    channel_slug: str,
    storage_root: str,
    job_id: int,
    scope: str = "pending",
    force: bool = False,
    max_tracks: int = 200,
) -> AnalyzeStats:
    _require_thresholds(conn, channel_slug)

    tracks = _select_tracks(conn, channel_slug=channel_slug, scope=scope, force=force, max_tracks=max_tracks)
    selected = len(tracks)
    processed = 0
    failed = 0

    for row in tracks:
        track_pk = int(row["id"])
        file_id = str(row["gdrive_file_id"])
        track_tmp_dir = Path(storage_root) / "tmp" / "track_analyzer" / str(job_id) / str(track_pk)
        local_path = track_tmp_dir / f"{file_id}.wav"
        try:
            drive.download_to_path(file_id, local_path)
            duration_sec = _extract_duration_sec(local_path)
            waveform, sample_rate, channels_count, stereo_waveform = _load_wav_pcm(local_path)
            true_peak_dbfs = _extract_true_peak_dbfs(local_path)
            spikes_found = _detect_spikes(true_peak_dbfs)
            try:
                yamnet_payload = yamnet.analyze_with_yamnet(local_path)
            except yamnet.YAMNetUnavailableError as exc:
                raise AnalyzeError("YAMNET_NOT_INSTALLED: install via UI button and retry") from exc

            try:
                texture_meta = _analyze_texture(local_path)
            except Exception as exc:
                log.exception(
                    "texture analysis failed: job_id=%s track_pk=%s backend=%s error_class=%s error_message=%s",
                    job_id,
                    track_pk,
                    "heuristic",
                    exc.__class__.__name__,
                    str(exc),
                )
                texture_meta = {
                    "dominant_texture": "unknown texture",
                    "texture_backend": "heuristic",
                    "texture_confidence": None,
                    "texture_reason": "exception",
                }

            yamnet_agg = _aggregate_yamnet_probabilities(yamnet_payload)
            voice_flag, voice_flag_reason = _derive_voice_flag(yamnet_agg)
            speech_flag, speech_flag_reason = _derive_speech_flag(yamnet_agg)
            prohibited_cues = _analyze_prohibited_cues(
                waveform,
                sample_rate,
                true_peak_dbfs=true_peak_dbfs,
                spikes_found=spikes_found,
            )
            prohibited_cues_notes = _build_prohibited_cues_notes(prohibited_cues)
            dsp_score, dsp_components, dsp_notes = _derive_dsp_score(
                true_peak_dbfs=true_peak_dbfs,
                spikes_found=spikes_found,
                prohibited_cues=prohibited_cues,
            )

            dominant_texture = str(texture_meta["dominant_texture"])
            missing_fields: list[str] = []
            if not dominant_texture.strip():
                missing_fields.append("dominant_texture")
            if not prohibited_cues_notes.strip():
                missing_fields.append("prohibited_cues_notes")
            if dsp_score is None:
                missing_fields.append("dsp_score")

            analysis_status = "COMPLETE" if not missing_fields else "REVIEW"
            computed_at = dbm.now_ts()
            advanced_v1_meta = {
                "analyzer_version": ADVANCED_ANALYZER_VERSION,
                "schema_version": ADVANCED_SCHEMA_VERSION,
                "analyzed_at": computed_at,
                "rollout_tier": ADVANCED_ROLLOUT_TIER,
                "segment_policy": ADVANCED_SEGMENT_POLICY,
            }
            quality_metrics = compute_quality_metrics(
                mono_waveform=waveform,
                stereo_waveform=stereo_waveform,
                sample_rate=sample_rate,
                channels_count=channels_count,
                duration_sec=duration_sec,
                true_peak_dbfs=true_peak_dbfs,
            )
            dynamics_metrics = compute_dynamics_metrics(mono_waveform=waveform, sample_rate=sample_rate)
            timbre_metrics = _compute_timbre_metrics(waveform, sample_rate, dominant_texture=dominant_texture)
            structure_metrics = _compute_structure_metrics(waveform, sample_rate)
            voice_metrics = _compute_voice_metrics(yamnet_agg)
            similarity_metrics = _compute_similarity_metrics(
                timbre=timbre_metrics,
                structure=structure_metrics,
                voice=voice_metrics,
            )
            derived_outputs = _compute_advanced_derived_outputs(
                quality=quality_metrics,
                dynamics=dynamics_metrics,
                timbre=timbre_metrics,
                structure=structure_metrics,
                voice=voice_metrics,
                similarity=similarity_metrics,
                channel_slug=channel_slug,
            )

            features_payload = {
                "duration_sec": duration_sec,
                "true_peak_dbfs": true_peak_dbfs,
                "spikes_found": spikes_found,
                "yamnet_top_classes": yamnet_payload.get("top_classes") or [],
                "yamnet_probabilities": yamnet_payload.get("probabilities") or {},
                "yamnet_agg": yamnet_agg,
                "voice_flag": voice_flag,
                "voice_flag_reason": voice_flag_reason,
                "speech_flag": speech_flag,
                "speech_flag_reason": speech_flag_reason,
                "dominant_texture": dominant_texture,
                "texture_backend": texture_meta["texture_backend"],
                "texture_confidence": texture_meta["texture_confidence"],
                "texture_reason": texture_meta["texture_reason"],
                "analysis_status": analysis_status,
                "missing_fields": missing_fields,
                "advanced_v1": {
                    "meta": advanced_v1_meta,
                    "profiles": _build_p0_profiles(),
                    "quality": quality_metrics,
                    "dynamics": dynamics_metrics,
                    "timbre": timbre_metrics,
                    "structure": structure_metrics,
                    "voice": voice_metrics,
                    "similarity": similarity_metrics,
                },
            }
            tags_payload = {
                "yamnet_tags": [entry.get("label") for entry in (yamnet_payload.get("top_classes") or []) if entry.get("label")],
                "prohibited_cues_notes": prohibited_cues_notes,
                "prohibited_cues": prohibited_cues,
                "analysis_status": analysis_status,
                "missing_fields": missing_fields,
                "advanced_v1": {
                    "meta": advanced_v1_meta,
                    "profiles": _build_p0_profiles(),
                    "semantic": {
                        "mood_tags": derived_outputs["semantic"]["mood_tags"],
                        "theme_tags": derived_outputs["semantic"]["theme_tags"],
                    },
                    "voice_tags": derived_outputs["voice_tags"],
                    "classifier_evidence": {
                        "yamnet_top_classes": yamnet_payload.get("top_classes") or [],
                    },
                },
            }
            scores_payload = {
                "dsp_score": dsp_score,
                "dsp_score_version": DSP_SCORE_VERSION,
                "dsp_components": dsp_components,
                "dsp_notes": dsp_notes,
                "analysis_status": analysis_status,
                "missing_fields": missing_fields,
                "advanced_v1": {
                    "meta": advanced_v1_meta,
                    "profiles": _build_p0_profiles(),
                    "semantic": {
                        "functional_scores": derived_outputs["semantic"]["functional_scores"],
                    },
                    "playlist_fit": derived_outputs["playlist_fit"],
                    "transition": derived_outputs["transition"],
                    "suitability": derived_outputs["suitability"],
                    "rule_trace": derived_outputs["rule_trace"],
                    "final_decisions": derived_outputs["final_decisions"],
                },
            }
            _validate_advanced_v1_payload(
                features_payload,
                required_scalar_paths=("duration_sec",),
                allow_none_scalar_paths=("duration_sec",),
                required_advanced_scalar_paths=FEATURES_REQUIRED_ADVANCED_SCALAR_PATHS,
                required_advanced_object_paths=FEATURES_REQUIRED_ADVANCED_OBJECT_PATHS,
                required_advanced_list_paths=FEATURES_REQUIRED_ADVANCED_LIST_PATHS,
            )
            _validate_advanced_v1_payload(
                tags_payload,
                required_advanced_object_paths=TAGS_REQUIRED_ADVANCED_OBJECT_PATHS,
                required_advanced_list_paths=TAGS_REQUIRED_ADVANCED_LIST_PATHS,
            )
            _validate_advanced_v1_payload(
                scores_payload,
                required_scalar_paths=("dsp_score",),
                required_advanced_scalar_paths=SCORES_REQUIRED_ADVANCED_SCALAR_PATHS,
                required_advanced_object_paths=SCORES_REQUIRED_ADVANCED_OBJECT_PATHS,
                required_advanced_list_paths=SCORES_REQUIRED_ADVANCED_LIST_PATHS,
            )

            conn.execute("BEGIN")
            try:
                conn.execute(
                    """
                    INSERT INTO track_features(track_pk, payload_json, computed_at)
                    VALUES(?,?,?)
                    ON CONFLICT(track_pk) DO UPDATE SET payload_json=excluded.payload_json, computed_at=excluded.computed_at
                    """,
                    (track_pk, dbm.json_dumps(features_payload), computed_at),
                )
                conn.execute(
                    """
                    INSERT INTO track_tags(track_pk, payload_json, computed_at)
                    VALUES(?,?,?)
                    ON CONFLICT(track_pk) DO UPDATE SET payload_json=excluded.payload_json, computed_at=excluded.computed_at
                    """,
                    (track_pk, dbm.json_dumps(tags_payload), computed_at),
                )
                conn.execute(
                    """
                    INSERT INTO track_scores(track_pk, payload_json, computed_at)
                    VALUES(?,?,?)
                    ON CONFLICT(track_pk) DO UPDATE SET payload_json=excluded.payload_json, computed_at=excluded.computed_at
                    """,
                    (track_pk, dbm.json_dumps(scores_payload), computed_at),
                )
                analyzer_payload = {
                    "track_features": {"payload_json": features_payload},
                    "track_tags": {"payload_json": tags_payload},
                    "track_scores": {"payload_json": scores_payload},
                }
                try:
                    apply_auto_custom_tags(conn, track_pk, analyzer_payload)
                except Exception as exc:
                    log.exception(
                        "custom tag auto-assign failed: job_id=%s track_pk=%s error_class=%s error_message=%s",
                        job_id,
                        track_pk,
                        exc.__class__.__name__,
                        str(exc),
                    )
                    raise AnalyzeError("CTA_AUTO_ASSIGN_FAILED") from exc
                conn.execute(
                    "UPDATE tracks SET analyzed_at=?, duration_sec=? WHERE id=?",
                    (computed_at, duration_sec, track_pk),
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")
            processed += 1
        except Exception:
            failed += 1
            raise
        finally:
            shutil.rmtree(track_tmp_dir, ignore_errors=True)

    return AnalyzeStats(selected=selected, processed=processed, failed=failed)


def _select_tracks(conn: Any, *, channel_slug: str, scope: str, force: bool, max_tracks: int) -> list[dict[str, Any]]:
    normalized_scope = scope.strip().lower()
    if normalized_scope not in {"pending", "all"}:
        raise AnalyzeError("invalid scope")

    where = ["channel_slug = ?"]
    args: list[Any] = [channel_slug]
    if normalized_scope == "pending" and not force:
        where.append("analyzed_at IS NULL")

    args.append(int(max_tracks))
    return conn.execute(
        f"""
        SELECT id, gdrive_file_id
        FROM tracks
        WHERE {' AND '.join(where)}
        ORDER BY id ASC
        LIMIT ?
        """,
        tuple(args),
    ).fetchall()


def _build_p0_profiles() -> dict[str, dict[str, Any]]:
    return {context: {} for context in P0_CONTEXTS}


def _validate_advanced_v1_payload(
    payload: dict[str, Any],
    *,
    required_scalar_paths: tuple[str, ...] = (),
    allow_none_scalar_paths: tuple[str, ...] = (),
    required_advanced_scalar_paths: tuple[str, ...] = (),
    required_advanced_object_paths: tuple[str, ...] = (),
    required_advanced_list_paths: tuple[str, ...] = (),
) -> None:
    advanced = payload.get("advanced_v1")
    if not isinstance(advanced, dict):
        raise AnalyzeError("ADVANCED_V1_INVALID: missing advanced_v1 object")

    profiles = advanced.get("profiles")
    if not isinstance(profiles, dict):
        raise AnalyzeError("ADVANCED_V1_INVALID: profiles must be an object")
    for context in P0_CONTEXTS:
        profile_obj = profiles.get(context)
        if not isinstance(profile_obj, dict):
            raise AnalyzeError(f"ADVANCED_V1_INVALID: missing profile object {context}")

    meta = advanced.get("meta")
    if not isinstance(meta, dict):
        raise AnalyzeError("ADVANCED_V1_INVALID: missing meta object")
    for meta_key in ("analyzer_version", "schema_version", "analyzed_at", "rollout_tier", "segment_policy"):
        if meta.get(meta_key) is None:
            raise AnalyzeError(f"ADVANCED_V1_INVALID: meta.{meta_key} must be non-null")

    similarity = advanced.get("similarity")
    if isinstance(similarity, dict):
        vector = similarity.get("normalized_feature_vector")
        if not isinstance(vector, list) or len(vector) != len(SIMILARITY_VECTOR_ORDER):
            raise AnalyzeError("ADVANCED_V1_INVALID: normalized_feature_vector length mismatch")

    for path in required_scalar_paths:
        value: Any = payload
        for part in path.split("."):
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(part)
        if value is None and path not in allow_none_scalar_paths:
            raise AnalyzeError(f"ADVANCED_V1_INVALID: {path} must be non-null")

    for path in required_advanced_scalar_paths:
        value = _resolve_nested_path(advanced, path)
        if value is None:
            raise AnalyzeError(f"ADVANCED_V1_INVALID: advanced_v1.{path} must be non-null")

    for path in required_advanced_object_paths:
        value = _resolve_nested_path(advanced, path)
        if not isinstance(value, dict):
            raise AnalyzeError(f"ADVANCED_V1_INVALID: advanced_v1.{path} must be an object")

    for path in required_advanced_list_paths:
        value = _resolve_nested_path(advanced, path)
        if not isinstance(value, list):
            raise AnalyzeError(f"ADVANCED_V1_INVALID: advanced_v1.{path} must be a list")


def _resolve_nested_path(root: dict[str, Any], path: str) -> Any:
    value: Any = root
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _require_thresholds(conn: Any, channel_slug: str) -> None:
    row = conn.execute("SELECT 1 FROM canon_thresholds WHERE value = ? LIMIT 1", (channel_slug,)).fetchone()
    if row is None:
        raise AnalyzeError("CHANNEL_NOT_IN_CANON")


def _extract_duration_sec(path: Path) -> float | None:
    data = ffmpeg.ffprobe_json(path)
    raw_duration = (data.get("format") or {}).get("duration")
    if raw_duration is None:
        for stream in data.get("streams") or []:
            raw_duration = stream.get("duration")
            if raw_duration is not None:
                break
    if raw_duration is None:
        return None
    try:
        return float(raw_duration)
    except Exception:
        return None


def _extract_true_peak_dbfs(path: Path) -> float | None:
    cmd = ["ffmpeg", "-hide_banner", "-nostats", "-i", str(path), "-af", "ebur128=peak=true", "-f", "null", "-"]
    code, out, err = ffmpeg.run(cmd)
    text = out + "\n" + err
    if code == 0:
        peak = _parse_true_peak(text)
        if peak is not None:
            return peak

    _mean_db, max_db, _warn = ffmpeg.volumedetect(path)
    return max_db


def _parse_true_peak(text: str) -> float | None:
    m = _TRUE_PEAK_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _detect_spikes(true_peak_dbfs: float | None) -> bool:
    if true_peak_dbfs is None:
        return False
    return true_peak_dbfs > -1.0


def _analyze_prohibited_cues(
    waveform: np.ndarray,
    sample_rate: int,
    *,
    true_peak_dbfs: float | None,
    spikes_found: bool,
) -> dict[str, Any]:
    clipping_detected = _detect_clipping(waveform, true_peak_dbfs)
    silence_gaps, silence_max_ms = _detect_silence_gap(waveform, sample_rate)
    abrupt_gain_jumps, rms_std, max_rms_delta = _detect_abrupt_gain_jumps(waveform, sample_rate)

    checks_run = ["true_peak", "spikes", "clipping", "silence_gaps", "abrupt_gain_jumps"]
    flags = {
        "spikes_found": spikes_found,
        "clipping_detected": clipping_detected,
        "silence_gaps": silence_gaps,
        "abrupt_gain_jumps": abrupt_gain_jumps,
    }
    metrics = {
        "true_peak_dbfs": float(true_peak_dbfs) if true_peak_dbfs is not None else -120.0,
        "spikes_found": 1.0 if spikes_found else 0.0,
        "sample_peak": float(np.max(np.abs(waveform))) if waveform.size else 0.0,
        "silence_max_gap_ms": float(silence_max_ms),
        "frame_rms_std": float(rms_std),
        "max_rms_delta": float(max_rms_delta),
    }
    return {
        "backend": "fallback",
        "checks_run": checks_run,
        "flags": flags,
        "metrics": metrics,
        "thresholds": {
            "silence_rms_threshold": float(SILENCE_RMS_THRESHOLD),
            "silence_gap_min_ms": float(SILENCE_GAP_MIN_MS),
            "silence_ignore_edge_ms": float(SILENCE_IGNORE_EDGE_MS),
        },
    }


def _build_prohibited_cues_notes(prohibited_cues: dict[str, Any]) -> str:
    active = [name for name, enabled in (prohibited_cues.get("flags") or {}).items() if bool(enabled)]
    if not active:
        return "No prohibited cues detected by fallback analyzer."
    return "Fallback analyzer flags: " + ", ".join(sorted(active))


def _aggregate_yamnet_probabilities(yamnet_payload: dict[str, Any]) -> dict[str, Any]:
    top_classes = yamnet_payload.get("top_classes") or []
    class_probabilities = yamnet_payload.get("class_probabilities") or {}
    label_scores: dict[str, float] = {}
    source = "top_classes"
    total_labels_count: int | None = None

    if isinstance(class_probabilities, dict) and class_probabilities:
        for label, score in class_probabilities.items():
            if not isinstance(label, str):
                continue
            try:
                label_scores[label] = float(score)
            except Exception:
                continue
        source = "full_vector"
        total_labels_count = len(label_scores)
    else:
        for entry in top_classes:
            label = entry.get("label")
            score = entry.get("score")
            if not isinstance(label, str):
                continue
            try:
                label_scores[label] = float(score)
            except Exception:
                continue

    voice_labels_used = sorted([label for label in VOICE_LABELS if label in label_scores])
    speech_labels_used = sorted([label for label in SPEECH_LABELS if label in label_scores])

    voice_prob = float(sum(label_scores[label] for label in voice_labels_used))
    speech_prob = float(sum(label_scores[label] for label in speech_labels_used))
    singing_prob = float(label_scores.get("Singing", 0.0))

    out = {
        "voice_prob": voice_prob,
        "speech_prob": speech_prob,
        "singing_prob": singing_prob,
        "voice_labels_used": voice_labels_used,
        "speech_labels_used": speech_labels_used,
        "source": source,
        "top_classes_count": len(top_classes),
    }
    if total_labels_count is not None:
        out["total_labels_count"] = total_labels_count
    return out


def _derive_voice_flag(yamnet_agg: dict[str, Any]) -> tuple[bool, str]:
    voice_prob = float(yamnet_agg.get("voice_prob") or 0.0)
    singing_prob = float(yamnet_agg.get("singing_prob") or 0.0)
    flag = voice_prob >= VOICE_MIN_PROB or singing_prob >= SINGING_MIN_PROB
    reason = (
        f"voice_prob={voice_prob:.3f} (min={VOICE_MIN_PROB:.2f}), "
        f"singing_prob={singing_prob:.3f} (min={SINGING_MIN_PROB:.2f})"
    )
    return flag, reason


def _derive_speech_flag(yamnet_agg: dict[str, Any]) -> tuple[bool, str]:
    speech_prob = float(yamnet_agg.get("speech_prob") or 0.0)
    flag = speech_prob >= SPEECH_MIN_PROB
    reason = f"speech_prob={speech_prob:.3f} (min={SPEECH_MIN_PROB:.2f})"
    return flag, reason


def _derive_dsp_score(
    *,
    true_peak_dbfs: float | None,
    spikes_found: bool,
    prohibited_cues: dict[str, Any],
) -> tuple[float, dict[str, float], str]:
    metrics = prohibited_cues.get("metrics") or {}
    flags = prohibited_cues.get("flags") or {}

    headroom_component = _headroom_component(true_peak_dbfs)
    rms_std = float(metrics.get("frame_rms_std") or 0.0)
    stability_component = float(np.clip(1.0 - (rms_std / 0.12), 0.0, 1.0))
    spikes_component = 0.4 if spikes_found else 1.0
    clipping_component = 0.2 if bool(flags.get("clipping_detected")) else 1.0
    silence_max_gap_ms = float(metrics.get("silence_max_gap_ms") or 0.0)
    silence_component = _silence_component_from_gap(silence_max_gap_ms)

    components = {
        "headroom_component": headroom_component,
        "stability_component": stability_component,
        "spikes_component": spikes_component,
        "clipping_component": clipping_component,
        "silence_component": silence_component,
    }
    weights = {
        "headroom_component": 0.3,
        "stability_component": 0.25,
        "spikes_component": 0.15,
        "clipping_component": 0.2,
        "silence_component": 0.1,
    }
    dsp_score = float(np.clip(sum(components[k] * weights[k] for k in components), 0.0, 1.0))
    notes = "weighted components: headroom/stability/spikes/clipping/silence"
    return dsp_score, components, notes


def _headroom_component(true_peak_dbfs: float | None) -> float:
    if true_peak_dbfs is None:
        return 0.5
    if true_peak_dbfs <= -6.0:
        return 1.0
    if true_peak_dbfs >= -0.1:
        return 0.0
    return float(np.clip((-0.1 - true_peak_dbfs) / 5.9, 0.0, 1.0))


def _detect_clipping(waveform: np.ndarray, true_peak_dbfs: float | None) -> bool:
    if waveform.size == 0:
        return False
    sample_peak = float(np.max(np.abs(waveform)))
    if sample_peak >= 0.999:
        return True
    near_max = np.abs(waveform) >= 0.999
    if np.count_nonzero(near_max) >= 3 and np.any(np.convolve(near_max.astype(np.int32), np.ones(3, dtype=np.int32), mode="valid") >= 3):
        return True
    if true_peak_dbfs is not None and true_peak_dbfs >= -0.1:
        return True
    return False


def _frame_rms(waveform: np.ndarray, frame_size: int, hop_size: int) -> np.ndarray:
    if waveform.size == 0 or frame_size <= 0 or hop_size <= 0:
        return np.zeros(0, dtype=np.float32)
    if waveform.size < frame_size:
        padded = np.pad(waveform, (0, frame_size - waveform.size))
        return np.array([float(np.sqrt(np.mean(np.square(padded)) + 1e-12))], dtype=np.float32)

    rms_vals: list[float] = []
    for start in range(0, waveform.size - frame_size + 1, hop_size):
        frame = waveform[start : start + frame_size]
        rms_vals.append(float(np.sqrt(np.mean(np.square(frame)) + 1e-12)))
    return np.array(rms_vals, dtype=np.float32)


def _detect_silence_gap(waveform: np.ndarray, sample_rate: int) -> tuple[bool, float]:
    frame_size = max(1, int(0.05 * sample_rate))
    hop_size = frame_size
    rms = _frame_rms(waveform, frame_size, hop_size)
    if rms.size == 0:
        return False, 0.0
    silence_threshold = float(SILENCE_RMS_THRESHOLD)
    silent = rms < silence_threshold

    edge_frames = int(SILENCE_IGNORE_EDGE_MS / 1000.0 / (hop_size / sample_rate))
    if edge_frames > 0:
        active = np.ones(silent.shape, dtype=bool)
        active[: min(edge_frames, active.size)] = False
        active[max(0, active.size - edge_frames) :] = False
        silent = silent & active
    max_run = 0
    run = 0
    for flag in silent:
        if bool(flag):
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0
    max_gap_ms = float(max_run * (hop_size / sample_rate) * 1000.0)
    return max_gap_ms >= float(SILENCE_GAP_MIN_MS), max_gap_ms


def _silence_component_from_gap(silence_max_gap_ms: float) -> float:
    gap = max(float(silence_max_gap_ms), 0.0)
    min_gap = float(SILENCE_GAP_MIN_MS)
    if gap < min_gap:
        return 1.0
    if gap <= 2000.0:
        span = max(2000.0 - min_gap, 1e-9)
        fraction = (gap - min_gap) / span
        return float(np.clip(1.0 - (0.5 * fraction), 0.0, 1.0))
    if gap >= 4000.0:
        return 0.0
    fraction = (gap - 2000.0) / 2000.0
    return float(np.clip(0.5 * (1.0 - fraction), 0.0, 1.0))


def _detect_abrupt_gain_jumps(waveform: np.ndarray, sample_rate: int) -> tuple[bool, float, float]:
    frame_size = max(1, int(0.05 * sample_rate))
    hop_size = frame_size
    rms = _frame_rms(waveform, frame_size, hop_size)
    if rms.size <= 1:
        return False, 0.0, 0.0
    deltas = np.abs(np.diff(rms))
    max_delta = float(np.max(deltas))
    rms_std = float(np.std(rms))
    return max_delta > 0.25, rms_std, max_delta


def _analyze_texture(path: Path) -> dict[str, str | float | None]:
    waveform, sample_rate, _channels_count, _stereo_waveform = _load_wav_pcm(path)
    return _analyze_texture_from_waveform(waveform, sample_rate)


def _analyze_texture_from_waveform(waveform: np.ndarray, sample_rate: int) -> dict[str, str | float | None]:
    """Classify dominant texture using lightweight waveform heuristics."""
    label, confidence, _debug = classify_texture(waveform, sample_rate)
    reason = "ok"
    if confidence < 0.35:
        label = "mixed"
        reason = "low_confidence"

    return {
        "dominant_texture": label,
        "texture_backend": "heuristic",
        "texture_confidence": confidence,
        "texture_reason": reason,
    }


def _compute_timbre_metrics(
    waveform: np.ndarray,
    sample_rate: int,
    *,
    dominant_texture: str,
) -> dict[str, float]:
    if waveform.size == 0 or sample_rate <= 0:
        return {
            "brightness": 0.0,
            "warmth": 0.0,
            "darkness": 1.0,
            "spectral_centroid_mean": 0.0,
            "spectral_rolloff_mean": 0.0,
            "low_end_weight": 0.0,
            "high_end_sharpness": 0.0,
            "harmonic_density": 0.0,
            "tonal_stability": 0.0,
            "drone_presence": 0.0,
            "pad_presence": 0.0,
            "percussion_presence": 0.0,
            "melodic_prominence": 0.0,
            "texture_smoothness": 0.0,
        }

    n_fft = min(4096, max(512, int(2 ** np.floor(np.log2(max(512, waveform.size // 4))))))
    window = np.hanning(n_fft).astype(np.float32)
    hop = max(1, n_fft // 4)
    if waveform.size < n_fft:
        waveform = np.pad(waveform, (0, n_fft - waveform.size))
    frames = np.lib.stride_tricks.sliding_window_view(waveform, n_fft)[::hop]
    if frames.size == 0:
        frames = waveform[:n_fft][None, :]
    spectrum = np.abs(np.fft.rfft(frames * window[None, :], axis=1)) + 1e-12
    freqs = np.fft.rfftfreq(n_fft, d=1.0 / sample_rate)

    frame_energy = np.sum(spectrum, axis=1, keepdims=True)
    centroid = np.sum(spectrum * freqs[None, :], axis=1) / np.maximum(frame_energy[:, 0], 1e-12)
    cdf = np.cumsum(spectrum, axis=1) / np.maximum(frame_energy, 1e-12)
    roll_idx = np.argmax(cdf >= 0.85, axis=1)
    rolloff = freqs[np.clip(roll_idx, 0, freqs.size - 1)]

    nyquist = max(sample_rate / 2.0, 1.0)
    centroid_norm = float(np.clip(np.mean(centroid) / nyquist, 0.0, 1.0))
    rolloff_norm = float(np.clip(np.mean(rolloff) / nyquist, 0.0, 1.0))

    low_mask = freqs <= 250.0
    high_mask = freqs >= 4000.0
    mid_mask = (freqs > 250.0) & (freqs < 4000.0)
    low_energy = float(np.mean(np.sum(spectrum[:, low_mask], axis=1) / np.maximum(frame_energy[:, 0], 1e-12)))
    high_energy = float(np.mean(np.sum(spectrum[:, high_mask], axis=1) / np.maximum(frame_energy[:, 0], 1e-12)))
    mid_energy = float(np.mean(np.sum(spectrum[:, mid_mask], axis=1) / np.maximum(frame_energy[:, 0], 1e-12)))

    diff_energy = np.abs(np.diff(np.sum(spectrum, axis=1)))
    transient = float(np.clip(np.mean(diff_energy) / (np.mean(np.sum(spectrum, axis=1)) + 1e-12), 0.0, 1.0))
    tonal_stability = float(np.clip(1.0 - np.std(centroid / nyquist), 0.0, 1.0))

    texture_smoothness = float(np.clip(1.0 - (transient * 2.0), 0.0, 1.0))
    if dominant_texture in {"pad", "drone"}:
        texture_smoothness = float(np.clip(max(texture_smoothness, 0.7), 0.0, 1.0))

    harmonic_density = float(np.clip(mid_energy + (1.0 - transient) * 0.25, 0.0, 1.0))
    brightness = float(np.clip((centroid_norm * 0.6) + (high_energy * 0.4), 0.0, 1.0))
    warmth = float(np.clip((low_energy * 0.7) + ((1.0 - centroid_norm) * 0.3), 0.0, 1.0))
    darkness = float(np.clip(1.0 - brightness, 0.0, 1.0))

    return {
        "brightness": brightness,
        "warmth": warmth,
        "darkness": darkness,
        "spectral_centroid_mean": centroid_norm,
        "spectral_rolloff_mean": rolloff_norm,
        "low_end_weight": float(np.clip(low_energy, 0.0, 1.0)),
        "high_end_sharpness": float(np.clip(high_energy + transient * 0.3, 0.0, 1.0)),
        "harmonic_density": harmonic_density,
        "tonal_stability": tonal_stability,
        "drone_presence": float(np.clip((1.0 - transient) * low_energy, 0.0, 1.0)),
        "pad_presence": float(np.clip(texture_smoothness * (0.5 + mid_energy * 0.5), 0.0, 1.0)),
        "percussion_presence": float(np.clip(transient * (0.5 + high_energy * 0.5), 0.0, 1.0)),
        "melodic_prominence": float(np.clip(harmonic_density * tonal_stability, 0.0, 1.0)),
        "texture_smoothness": texture_smoothness,
    }


def _compute_structure_metrics(waveform: np.ndarray, sample_rate: int) -> dict[str, Any]:
    frame_size = max(1, int(0.2 * sample_rate))
    hop_size = frame_size
    rms = _frame_rms(waveform, frame_size, hop_size)
    if rms.size == 0:
        rms = np.array([0.0], dtype=np.float32)
    rms_norm = rms / max(float(np.max(rms)), 1e-12)

    sections = np.array_split(rms_norm, 5)
    energy_values = [float(np.mean(section)) if section.size else 0.0 for section in sections]
    intro, early, middle, late, outro = energy_values

    first_deltas = np.abs(np.diff(sections[0])) if sections[0].size > 1 else np.array([0.0])
    last_deltas = np.abs(np.diff(sections[-1])) if sections[-1].size > 1 else np.array([0.0])
    global_deltas = np.abs(np.diff(rms_norm)) if rms_norm.size > 1 else np.array([0.0])
    abruptness = float(np.clip(np.mean(global_deltas) * 3.0, 0.0, 1.0))

    summary = {
        "parts": [
            {"name": "intro", "energy": intro},
            {"name": "early", "energy": early},
            {"name": "middle", "energy": middle},
            {"name": "late", "energy": late},
            {"name": "outro", "energy": outro},
        ],
        "peak_section": ["intro", "early", "middle", "late", "outro"][int(np.argmax(energy_values))],
        "mean_energy": float(np.mean(energy_values)),
    }

    return {
        "intro_energy": intro,
        "early_section_energy": early,
        "middle_section_energy": middle,
        "late_section_energy": late,
        "outro_energy": outro,
        "intro_smoothness": float(np.clip(1.0 - np.mean(first_deltas) * 4.0, 0.0, 1.0)),
        "outro_smoothness": float(np.clip(1.0 - np.mean(last_deltas) * 4.0, 0.0, 1.0)),
        "structural_stability": float(np.clip(1.0 - np.std(energy_values) * 2.0, 0.0, 1.0)),
        "climax_presence": float(np.clip(max(energy_values) - np.mean(energy_values), 0.0, 1.0)),
        "abruptness_score": abruptness,
        "loop_friendliness": float(np.clip(1.0 - abs(intro - outro), 0.0, 1.0)),
        "fade_friendliness": float(np.clip(max(0.0, late - outro), 0.0, 1.0)),
        "section_summary": summary,
    }


def _compute_voice_metrics(yamnet_agg: dict[str, Any]) -> dict[str, float]:
    speech_probability = float(np.clip(yamnet_agg.get("speech_prob") or 0.0, 0.0, 1.0))
    voice_probability = float(np.clip(yamnet_agg.get("voice_prob") or 0.0, 0.0, 1.0))
    singing_probability = float(np.clip(yamnet_agg.get("singing_prob") or 0.0, 0.0, 1.0))
    spoken_word_density = float(np.clip((speech_probability * 0.8) + (voice_probability * 0.2), 0.0, 1.0))
    human_presence = float(np.clip(max(voice_probability, singing_probability, speech_probability), 0.0, 1.0))
    return {
        "speech_probability": speech_probability,
        "vocal_probability": voice_probability,
        "spoken_word_density": spoken_word_density,
        "human_presence_score": human_presence,
    }


def _compute_similarity_metrics(
    *,
    timbre: dict[str, Any],
    structure: dict[str, Any],
    voice: dict[str, Any],
) -> dict[str, Any]:
    flattened = {
        **{f"timbre.{k}": float(v) for k, v in timbre.items()},
        **{f"structure.{k}": float(v) for k, v in structure.items() if k != "section_summary"},
        **{f"voice.{k}": float(v) for k, v in voice.items()},
    }
    vector = [float(np.clip(flattened.get(key, 0.0), 0.0, 1.0)) for key in SIMILARITY_VECTOR_ORDER]
    return {
        "normalized_feature_vector": vector,
        "diversity_penalty_base": float(np.clip(np.mean(vector), 0.0, 1.0)),
    }


def _clip01(value: float) -> float:
    return float(np.clip(value, 0.0, 1.0))


def _compute_advanced_derived_outputs(
    *,
    quality: dict[str, Any],
    dynamics: dict[str, Any],
    timbre: dict[str, Any],
    structure: dict[str, Any],
    voice: dict[str, Any],
    similarity: dict[str, Any],
    channel_slug: str,
) -> dict[str, Any]:
    proxies = {
        "speech": _clip01(float(voice.get("speech_probability") or 0.0)),
        "vocal": _clip01(float(voice.get("vocal_probability") or 0.0)),
        "human": _clip01(float(voice.get("human_presence_score") or 0.0)),
        "ambient": _clip01(float(timbre.get("pad_presence") or 0.0) * 0.6 + float(timbre.get("drone_presence") or 0.0) * 0.4),
        "rhythmic": _clip01(float(timbre.get("percussion_presence") or 0.0) * 0.7 + float(dynamics.get("pulse_strength") or 0.0) * 0.3),
        "smooth": _clip01(float(timbre.get("texture_smoothness") or 0.0) * 0.5 + float(structure.get("intro_smoothness") or 0.0) * 0.25 + float(structure.get("outro_smoothness") or 0.0) * 0.25),
    }
    functional_scores = {
        "focus": _clip01(0.4 * proxies["smooth"] + 0.35 * proxies["ambient"] + 0.25 * (1.0 - proxies["speech"])),
        "energy": _clip01(0.5 * proxies["rhythmic"] + 0.3 * float(dynamics.get("energy_mean") or 0.0) + 0.2 * float(structure.get("climax_presence") or 0.0)),
        "narrative": _clip01(0.5 * proxies["speech"] + 0.3 * proxies["vocal"] + 0.2 * float(voice.get("spoken_word_density") or 0.0)),
        "background_compatibility": _clip01(0.45 * proxies["ambient"] + 0.35 * proxies["smooth"] + 0.2 * (1.0 - proxies["human"])),
    }
    mood_tags: list[str] = []
    if functional_scores["focus"] >= 0.62:
        mood_tags.append("calm")
    if functional_scores["energy"] >= 0.58:
        mood_tags.append("driving")
    if functional_scores["narrative"] >= 0.55:
        mood_tags.append("lyrical")
    if proxies["ambient"] >= 0.5:
        mood_tags.append("ambient")

    theme_tags: list[str] = []
    if float(structure.get("fade_friendliness") or 0.0) >= 0.5:
        theme_tags.append("fade-friendly")
    if float(structure.get("loop_friendliness") or 0.0) >= 0.7:
        theme_tags.append("loopable")
    if float(similarity.get("diversity_penalty_base") or 0.0) <= 0.42:
        theme_tags.append("minimal")

    playlist_fit = {
        "continuity_score": _clip01(0.55 * float(structure.get("loop_friendliness") or 0.0) + 0.45 * proxies["smooth"]),
        "mixability_score": _clip01(0.5 * float(structure.get("fade_friendliness") or 0.0) + 0.3 * (1.0 - float(structure.get("abruptness_score") or 0.0)) + 0.2 * proxies["ambient"]),
        "variety_support_score": _clip01(1.0 - float(similarity.get("diversity_penalty_base") or 0.0)),
    }
    transition = {
        "intro_profile": "soft" if proxies["smooth"] >= 0.55 else "hard",
        "outro_profile": "tail" if float(structure.get("fade_friendliness") or 0.0) >= 0.45 else "cut",
        "transition_risk_score": _clip01(0.5 * float(structure.get("abruptness_score") or 0.0) + 0.5 * (1.0 - playlist_fit["mixability_score"])),
    }

    context_scores = {
        "LONG_INSTRUMENTAL_AMBIENT": _clip01(0.45 * functional_scores["background_compatibility"] + 0.3 * (1.0 - proxies["speech"]) + 0.25 * proxies["ambient"]),
        "LONG_LYRICAL": _clip01(0.5 * functional_scores["narrative"] + 0.25 * proxies["vocal"] + 0.25 * float(dynamics.get("tempo_confidence") or 0.0)),
    }
    selected_context = max(P0_CONTEXTS, key=lambda key: context_scores[key])
    suitability = {
        "content_type_fit_score": context_scores[selected_context],
        "content_type_fit_by_context": context_scores,
        "selected_content_context": selected_context,
    }
    channel_archetype = P0_CHANNEL_ARCHETYPE_BY_SLUG.get(channel_slug)
    if channel_archetype in context_scores:
        suitability["channel_fit_score"] = context_scores[channel_archetype]

    warning_codes: list[str] = []
    soft_penalty_total = 0.0
    hard_veto = False
    if proxies["speech"] >= 0.85 and proxies["ambient"] >= 0.55:
        hard_veto = True
        warning_codes.append("VETO_SPEECH_IN_INSTRUMENTAL")
    if float(quality.get("clipping_ratio") or 0.0) > 0.03:
        soft_penalty_total += 0.2
        warning_codes.append("PENALTY_CLIPPING")
    if transition["transition_risk_score"] > 0.65:
        soft_penalty_total += 0.15
        warning_codes.append("PENALTY_TRANSITION_RISK")
    if playlist_fit["continuity_score"] < 0.35:
        soft_penalty_total += 0.1
        warning_codes.append("PENALTY_LOW_CONTINUITY")

    rule_trace = [
        {
            "rule_id": "semantic.focus.v1",
            "inputs": {"smooth": proxies["smooth"], "ambient": proxies["ambient"], "speech": proxies["speech"]},
            "weights": {"smooth": 0.4, "ambient": 0.35, "speech_inverse": 0.25},
            "output": functional_scores["focus"],
        },
        {
            "rule_id": "semantic.narrative.v1",
            "inputs": {"speech": proxies["speech"], "vocal": proxies["vocal"], "spoken_word_density": float(voice.get("spoken_word_density") or 0.0)},
            "weights": {"speech": 0.5, "vocal": 0.3, "spoken_word_density": 0.2},
            "output": functional_scores["narrative"],
        },
        {
            "rule_id": "decision.penalty.transition_risk.v1",
            "inputs": {"transition_risk_score": transition["transition_risk_score"]},
            "thresholds": {"gt": 0.65},
            "weight": 0.15,
            "matched": transition["transition_risk_score"] > 0.65,
        },
    ]
    if hard_veto:
        rule_trace.append(
            {
                "rule_id": "decision.veto.instrumental_speech.v1",
                "inputs": {
                    "speech": proxies["speech"],
                    "ambient": proxies["ambient"],
                },
                "thresholds": {"speech_gte": 0.85, "ambient_gte": 0.55},
                "matched": True,
            }
        )

    voice_tags = ["spoken_word"] if proxies["speech"] >= 0.55 else []
    return {
        "semantic": {
            "functional_scores": functional_scores,
            "mood_tags": sorted(set(mood_tags)),
            "theme_tags": sorted(set(theme_tags)),
        },
        "playlist_fit": playlist_fit,
        "transition": transition,
        "suitability": suitability,
        "rule_trace": rule_trace,
        "voice_tags": voice_tags,
        "final_decisions": {
            "hard_veto": hard_veto,
            "soft_penalty_total": _clip01(soft_penalty_total),
            "warning_codes": warning_codes,
        },
    }


def _load_wav_mono(path: Path) -> tuple[np.ndarray, int]:
    waveform, sample_rate, _channels_count, _stereo_waveform = _load_wav_pcm(path)
    return waveform, sample_rate


def _load_wav_pcm(path: Path) -> tuple[np.ndarray, int, int, np.ndarray | None]:
    with wave.open(str(path), "rb") as wav_file:
        sample_rate = int(wav_file.getframerate())
        sample_width = int(wav_file.getsampwidth())
        channels = int(wav_file.getnchannels())
        frames = wav_file.readframes(wav_file.getnframes())

    if channels <= 0:
        raise ValueError("invalid channel count")

    if sample_width == 1:
        data = np.frombuffer(frames, dtype=np.uint8).astype(np.float32)
        data = (data - 128.0) / 128.0
    elif sample_width == 2:
        data = np.frombuffer(frames, dtype=np.int16).astype(np.float32) / 32768.0
    elif sample_width == 4:
        data = np.frombuffer(frames, dtype=np.int32).astype(np.float32) / 2147483648.0
    else:
        raise ValueError(f"unsupported sample width: {sample_width}")

    stereo_waveform: np.ndarray | None = None
    if channels > 1:
        reshaped = data.reshape(-1, channels)
        if channels >= 2:
            stereo_waveform = reshaped[:, :2].astype(np.float32)
        mono = reshaped.mean(axis=1)
    else:
        mono = data

    return mono.astype(np.float32), sample_rate, channels, stereo_waveform
