from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from services.track_analysis_report.flatten import flatten_value, resolve_source_path
from services.track_analysis_report.registry import COLUMN_GROUPS, COLUMN_REGISTRY


class TrackAnalysisReportError(Exception):
    """Base error for track analysis report building."""


class InvalidChannelSlugError(TrackAnalysisReportError):
    """Raised when channel slug input is missing/blank."""


class ChannelNotFoundError(TrackAnalysisReportError):
    """Raised when channel slug is not present in channels table."""


_FLAT_PRIMARY_COLUMN_BY_KEY: dict[str, str] = {
    "analysis_status": "taf_analysis_status",
    "true_peak_dbfs": "taf_true_peak_dbfs",
    "spikes_found": "taf_spikes_found",
    "voice_flag": "taf_voice_flag",
    "voice_flag_reason": "taf_voice_flag_reason",
    "speech_flag": "taf_speech_flag",
    "speech_flag_reason": "taf_speech_flag_reason",
    "yamnet_tags": "taf_yamnet_top_tags_text",
    "dominant_texture": "taf_dominant_texture",
    "texture_confidence": "taf_texture_confidence",
    "texture_reason": "taf_texture_reason",
    "dsp_score": "taf_dsp_score",
    "analyzer_version": "taf_analyzer_version",
    "schema_version": "taf_schema_version",
}


def _coerce_flat_value(column_key: str, value: Any) -> Any:
    if value is None:
        return None
    if column_key in {"voice_flag", "speech_flag"}:
        return bool(value)
    return value


def _parse_payload_json(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _row_to_mapping(row: Any, column_names: Sequence[str]) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    if isinstance(row, Sequence):
        return {column_names[idx]: row[idx] for idx in range(min(len(column_names), len(row)))}
    return {}


def _resolve_effective_custom_tags_by_track(
    conn: sqlite3.Connection, track_pks: Sequence[int]
) -> dict[int, dict[str, list[str]]]:
    if not track_pks:
        return {}

    placeholders = ", ".join("?" for _ in track_pks)
    rows = conn.execute(
        f"""
        SELECT a.track_pk AS track_pk, t.category AS category, t.label AS label
        FROM track_custom_tag_assignments a
        JOIN custom_tags t ON t.id = a.tag_id
        WHERE a.track_pk IN ({placeholders})
          AND a.state IN ('AUTO', 'MANUAL')
        ORDER BY a.track_pk ASC, t.category ASC, t.label ASC, t.id ASC
        """,
        tuple(track_pks),
    ).fetchall()

    output: dict[int, dict[str, list[str]]] = {}
    category_map = {"VISUAL": "visual", "MOOD": "mood", "THEME": "theme"}
    for row in rows:
        track_pk = int(row["track_pk"])
        category = category_map.get(str(row["category"]))
        if category is None:
            continue
        bucket = output.setdefault(track_pk, {})
        bucket.setdefault(category, []).append(str(row["label"]))
    return output


def build_channel_report(conn: sqlite3.Connection, channel_slug: str) -> dict[str, Any]:
    normalized_slug = str(channel_slug or "").strip()
    if not normalized_slug:
        raise InvalidChannelSlugError("channel_slug is required")

    channel_exists = conn.execute(
        "SELECT 1 FROM channels WHERE slug = ? LIMIT 1",
        (normalized_slug,),
    ).fetchone()
    if channel_exists is None:
        raise ChannelNotFoundError(f"channel not found: {normalized_slug}")

    cursor = conn.execute(
        """
        SELECT
            t.id AS t_id,
            t.channel_slug AS t_channel_slug,
            t.track_id AS t_track_id,
            t.gdrive_file_id AS t_gdrive_file_id,
            t.source AS t_source,
            t.filename AS t_filename,
            t.title AS t_title,
            t.artist AS t_artist,
            t.duration_sec AS t_duration_sec,
            t.discovered_at AS t_discovered_at,
            t.analyzed_at AS t_analyzed_at,
            tf.payload_json AS tf_payload_json,
            tf.computed_at AS tf_computed_at,
            tt.payload_json AS tt_payload_json,
            tt.computed_at AS tt_computed_at,
            ts.payload_json AS ts_payload_json,
            ts.computed_at AS ts_computed_at,
            taf.analysis_status AS taf_analysis_status,
            taf.true_peak_dbfs AS taf_true_peak_dbfs,
            taf.spikes_found AS taf_spikes_found,
            taf.voice_flag AS taf_voice_flag,
            taf.voice_flag_reason AS taf_voice_flag_reason,
            taf.speech_flag AS taf_speech_flag,
            taf.speech_flag_reason AS taf_speech_flag_reason,
            taf.yamnet_top_tags_text AS taf_yamnet_top_tags_text,
            taf.dominant_texture AS taf_dominant_texture,
            taf.texture_confidence AS taf_texture_confidence,
            taf.texture_reason AS taf_texture_reason,
            taf.dsp_score AS taf_dsp_score,
            taf.analyzer_version AS taf_analyzer_version,
            taf.schema_version AS taf_schema_version
        FROM tracks t
        LEFT JOIN track_features tf ON tf.track_pk = t.id
        LEFT JOIN track_tags tt ON tt.track_pk = t.id
        LEFT JOIN track_scores ts ON ts.track_pk = t.id
        LEFT JOIN track_analysis_flat taf ON taf.track_pk = t.id
        WHERE t.channel_slug = ?
        ORDER BY t.id ASC
        """,
        (normalized_slug,),
    )
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in (cursor.description or ())]
    track_pks = [int(_row_to_mapping(raw_row, column_names).get("t_id") or 0) for raw_row in rows]
    effective_tags_by_track = _resolve_effective_custom_tags_by_track(conn, [pk for pk in track_pks if pk > 0])

    report_rows: list[dict[str, Any]] = []
    for raw_row in rows:
        db_row = _row_to_mapping(raw_row, column_names)

        sources = {
            "tracks": {
                "id": db_row.get("t_id"),
                "channel_slug": db_row.get("t_channel_slug"),
                "track_id": db_row.get("t_track_id"),
                "gdrive_file_id": db_row.get("t_gdrive_file_id"),
                "source": db_row.get("t_source"),
                "filename": db_row.get("t_filename"),
                "title": db_row.get("t_title"),
                "artist": db_row.get("t_artist"),
                "duration_sec": db_row.get("t_duration_sec"),
                "discovered_at": db_row.get("t_discovered_at"),
                "analyzed_at": db_row.get("t_analyzed_at"),
            },
            "features": {
                "computed_at": db_row.get("tf_computed_at"),
                "payload_json": _parse_payload_json(db_row.get("tf_payload_json")),
            },
            "tags": {
                "computed_at": db_row.get("tt_computed_at"),
                "payload_json": _parse_payload_json(db_row.get("tt_payload_json")),
            },
            "scores": {
                "computed_at": db_row.get("ts_computed_at"),
                "payload_json": _parse_payload_json(db_row.get("ts_payload_json")),
            },
            "effective_custom_tags": effective_tags_by_track.get(int(db_row.get("t_id") or 0), {}),
        }

        flattened_row: dict[str, Any] = {}
        for entry in COLUMN_REGISTRY:
            flat_alias = _FLAT_PRIMARY_COLUMN_BY_KEY.get(entry["key"])
            raw_value = _coerce_flat_value(entry["key"], db_row.get(flat_alias)) if flat_alias else None
            if raw_value is None:
                raw_value = resolve_source_path(sources, entry["source"], entry["path"])
            flattened_row[entry["key"]] = flatten_value(raw_value, entry["flatten"])
        report_rows.append(flattened_row)

    columns = []
    for entry in COLUMN_REGISTRY:
        column = dict(entry)
        column["source_path"] = str(entry["path"])
        columns.append(column)

    return {
        "channel_slug": normalized_slug,
        "column_groups": list(COLUMN_GROUPS),
        "columns": columns,
        "rows": report_rows,
        "summary": {"tracks_count": len(report_rows)},
    }
