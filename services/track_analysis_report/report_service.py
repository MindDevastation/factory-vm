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
            ts.computed_at AS ts_computed_at
        FROM tracks t
        LEFT JOIN track_features tf ON tf.track_pk = t.id
        LEFT JOIN track_tags tt ON tt.track_pk = t.id
        LEFT JOIN track_scores ts ON ts.track_pk = t.id
        WHERE t.channel_slug = ?
        ORDER BY t.id ASC
        """,
        (normalized_slug,),
    )
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in (cursor.description or ())]

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
        }

        flattened_row: dict[str, Any] = {}
        for entry in COLUMN_REGISTRY:
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
