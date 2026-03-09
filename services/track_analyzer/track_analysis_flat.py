from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from services.common import db as dbm


def build_track_analysis_flat_row(
    *,
    track_row: dict[str, Any],
    features_payload: dict[str, Any],
    tags_payload: dict[str, Any],
    scores_payload: dict[str, Any],
    analysis_computed_at: float,
) -> dict[str, Any]:
    top_labels = _derive_yamnet_top_labels(features_payload, tags_payload)
    prohibited_summary = _derive_prohibited_cues_summary(tags_payload)

    meta = ((features_payload.get("advanced_v1") or {}).get("meta") or {}) if isinstance(features_payload, dict) else {}

    row = {
        "track_pk": int(track_row["id"]),
        "channel_slug": str(track_row.get("channel_slug") or ""),
        "track_id": str(track_row.get("track_id") or ""),
        "gdrive_file_id": track_row.get("gdrive_file_id"),
        "analysis_computed_at": float(analysis_computed_at),
        "analysis_status": _first_text(
            features_payload.get("analysis_status"),
            tags_payload.get("analysis_status"),
            scores_payload.get("analysis_status"),
            default="UNKNOWN",
        ),
        "analyzer_version": _nullable_text(meta.get("analyzer_version")),
        "schema_version": _nullable_text(meta.get("schema_version")),
        "duration_sec": _nullable_float(features_payload.get("duration_sec")),
        "true_peak_dbfs": _nullable_float(features_payload.get("true_peak_dbfs")),
        "spikes_found": _as_int_bool(features_payload.get("spikes_found")),
        "yamnet_top_tags_text": ", ".join(top_labels) if top_labels else None,
        "yamnet_top_classes_json": dbm.json_dumps(features_payload.get("yamnet_top_classes") or []),
        "voice_flag": _as_int_bool(features_payload.get("voice_flag")),
        "voice_flag_reason": _nullable_text(features_payload.get("voice_flag_reason")),
        "speech_flag": _as_int_bool(features_payload.get("speech_flag")),
        "speech_flag_reason": _nullable_text(features_payload.get("speech_flag_reason")),
        "dominant_texture": _nullable_text(features_payload.get("dominant_texture")),
        "texture_confidence": _nullable_float(features_payload.get("texture_confidence")),
        "texture_reason": _nullable_text(features_payload.get("texture_reason")),
        "prohibited_cues_summary": prohibited_summary,
        "prohibited_cues_flags_json": dbm.json_dumps((tags_payload.get("prohibited_cues") or {}).get("flags") or {}),
        "dsp_score": _nullable_float(scores_payload.get("dsp_score")),
        "dsp_score_version": _nullable_text(scores_payload.get("dsp_score_version")),
        "dsp_notes": _nullable_text(scores_payload.get("dsp_notes")),
        "legacy_scene": _nullable_text(features_payload.get("scene")),
        "legacy_mood": _nullable_text(tags_payload.get("mood")),
        "legacy_safety": _nullable_float(scores_payload.get("safety")),
        "legacy_scene_match": _nullable_float(scores_payload.get("scene_match")),
        "human_readable_notes": _human_notes(tags_payload, scores_payload, features_payload),
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    return row


def upsert_track_analysis_flat(conn: Any, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO track_analysis_flat(
            track_pk, channel_slug, track_id, gdrive_file_id, analysis_computed_at,
            analysis_status, analyzer_version, schema_version, duration_sec,
            true_peak_dbfs, spikes_found, yamnet_top_tags_text, yamnet_top_classes_json,
            voice_flag, voice_flag_reason, speech_flag, speech_flag_reason,
            dominant_texture, texture_confidence, texture_reason, prohibited_cues_summary,
            prohibited_cues_flags_json, dsp_score, dsp_score_version, dsp_notes,
            legacy_scene, legacy_mood, legacy_safety, legacy_scene_match,
            human_readable_notes, updated_at
        ) VALUES(
            :track_pk, :channel_slug, :track_id, :gdrive_file_id, :analysis_computed_at,
            :analysis_status, :analyzer_version, :schema_version, :duration_sec,
            :true_peak_dbfs, :spikes_found, :yamnet_top_tags_text, :yamnet_top_classes_json,
            :voice_flag, :voice_flag_reason, :speech_flag, :speech_flag_reason,
            :dominant_texture, :texture_confidence, :texture_reason, :prohibited_cues_summary,
            :prohibited_cues_flags_json, :dsp_score, :dsp_score_version, :dsp_notes,
            :legacy_scene, :legacy_mood, :legacy_safety, :legacy_scene_match,
            :human_readable_notes, :updated_at
        )
        ON CONFLICT(track_pk) DO UPDATE SET
            channel_slug=excluded.channel_slug,
            track_id=excluded.track_id,
            gdrive_file_id=excluded.gdrive_file_id,
            analysis_computed_at=excluded.analysis_computed_at,
            analysis_status=excluded.analysis_status,
            analyzer_version=excluded.analyzer_version,
            schema_version=excluded.schema_version,
            duration_sec=excluded.duration_sec,
            true_peak_dbfs=excluded.true_peak_dbfs,
            spikes_found=excluded.spikes_found,
            yamnet_top_tags_text=excluded.yamnet_top_tags_text,
            yamnet_top_classes_json=excluded.yamnet_top_classes_json,
            voice_flag=excluded.voice_flag,
            voice_flag_reason=excluded.voice_flag_reason,
            speech_flag=excluded.speech_flag,
            speech_flag_reason=excluded.speech_flag_reason,
            dominant_texture=excluded.dominant_texture,
            texture_confidence=excluded.texture_confidence,
            texture_reason=excluded.texture_reason,
            prohibited_cues_summary=excluded.prohibited_cues_summary,
            prohibited_cues_flags_json=excluded.prohibited_cues_flags_json,
            dsp_score=excluded.dsp_score,
            dsp_score_version=excluded.dsp_score_version,
            dsp_notes=excluded.dsp_notes,
            legacy_scene=excluded.legacy_scene,
            legacy_mood=excluded.legacy_mood,
            legacy_safety=excluded.legacy_safety,
            legacy_scene_match=excluded.legacy_scene_match,
            human_readable_notes=excluded.human_readable_notes,
            updated_at=excluded.updated_at
        """,
        row,
    )


def sync_track_analysis_flat(
    conn: Any,
    *,
    track_row: dict[str, Any],
    features_payload: dict[str, Any],
    tags_payload: dict[str, Any],
    scores_payload: dict[str, Any],
    analysis_computed_at: float,
) -> None:
    full_track_row = track_row
    if not track_row.get("channel_slug") or not track_row.get("track_id"):
        fetched = conn.execute("SELECT * FROM tracks WHERE id = ?", (int(track_row["id"]),)).fetchone()
        if fetched is not None:
            full_track_row = fetched

    upsert_track_analysis_flat(
        conn,
        build_track_analysis_flat_row(
            track_row=full_track_row,
            features_payload=features_payload,
            tags_payload=tags_payload,
            scores_payload=scores_payload,
            analysis_computed_at=analysis_computed_at,
        ),
    )


def _derive_yamnet_top_labels(features_payload: dict[str, Any], tags_payload: dict[str, Any]) -> list[str]:
    labels: set[str] = set()
    for raw_label in tags_payload.get("yamnet_tags") or []:
        if isinstance(raw_label, str) and raw_label.strip():
            labels.add(raw_label.strip())

    for entry in features_payload.get("yamnet_top_classes") or []:
        if not isinstance(entry, dict):
            continue
        raw_label = entry.get("label")
        if isinstance(raw_label, str) and raw_label.strip():
            labels.add(raw_label.strip())

    return sorted(labels)


def _derive_prohibited_cues_summary(tags_payload: dict[str, Any]) -> str | None:
    prohibited_cues = tags_payload.get("prohibited_cues") or {}
    flags = prohibited_cues.get("flags") or {}
    active_flags = sorted(name for name, enabled in flags.items() if bool(enabled))
    notes = _nullable_text(tags_payload.get("prohibited_cues_notes"))

    parts: list[str] = []
    if notes:
        parts.append(notes)
    if active_flags:
        parts.append("active_flags=" + ", ".join(active_flags))
    if not parts:
        return None
    return " | ".join(parts)


def _human_notes(tags_payload: dict[str, Any], scores_payload: dict[str, Any], features_payload: dict[str, Any]) -> str | None:
    parts = [
        _nullable_text(tags_payload.get("prohibited_cues_notes")),
        _nullable_text(scores_payload.get("dsp_notes")),
        _nullable_text(features_payload.get("texture_reason")),
        _nullable_text(features_payload.get("voice_flag_reason")),
        _nullable_text(features_payload.get("speech_flag_reason")),
    ]
    clean_parts = [part for part in parts if part]
    if not clean_parts:
        return None
    return " | ".join(clean_parts)


def _nullable_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_int_bool(value: Any) -> int:
    return 1 if bool(value) else 0


def _first_text(*values: Any, default: str) -> str:
    for value in values:
        normalized = _nullable_text(value)
        if normalized is not None:
            return normalized
    return default
