from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from services.playlist_builder.models import PlaylistBrief, PlaylistChannelSettingsPatch


class PlaylistBuilderValidationError(Exception):
    pass


SYSTEM_DEFAULTS: dict[str, Any] = {
    "generation_mode": "smart",
    "strictness_mode": "balanced",
    "min_duration_min": 30,
    "max_duration_min": 60,
    "tolerance_min": 5,
    "allow_cross_channel": False,
    "preferred_month_batch": None,
    "preferred_batch_ratio": 70,
    "novelty_target_min": 0.50,
    "novelty_target_max": 0.80,
    "position_memory_window": 20,
    "vocal_policy": "allow_any",
    "reuse_policy": "avoid_recent",
    "required_tags": [],
    "excluded_tags": [],
    "notes": None,
    "random_seed": None,
    "candidate_limit": None,
    "preferred_track_count_min": None,
    "preferred_track_count_max": None,
    "content_type": None,
}


def channel_settings_row_to_patch(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "generation_mode": row.get("default_generation_mode"),
        "strictness_mode": row.get("strictness_mode"),
        "min_duration_min": row.get("min_duration_min"),
        "max_duration_min": row.get("max_duration_min"),
        "tolerance_min": row.get("tolerance_min"),
        "preferred_month_batch": row.get("preferred_month_batch"),
        "preferred_batch_ratio": row.get("preferred_batch_ratio"),
        "allow_cross_channel": bool(row.get("allow_cross_channel")),
        "novelty_target_min": row.get("novelty_target_min"),
        "novelty_target_max": row.get("novelty_target_max"),
        "position_memory_window": row.get("position_memory_window"),
        "vocal_policy": row.get("vocal_policy"),
        "reuse_policy": row.get("reuse_policy"),
    }


def build_channel_settings_payload(*, channel_slug: str, row: dict[str, Any] | None) -> dict[str, Any]:
    default_settings = PlaylistChannelSettingsPatch().model_dump()
    merged = {**default_settings, **channel_settings_row_to_patch(row)}
    return {
        "channel_slug": channel_slug,
        "settings": PlaylistChannelSettingsPatch.model_validate(merged).model_dump(),
    }


def resolve_playlist_brief(
    *,
    channel_slug: str,
    job_id: int | None,
    channel_settings: dict[str, Any] | None,
    job_override: dict[str, Any] | None,
    request_override: dict[str, Any] | None = None,
) -> PlaylistBrief:
    merged: dict[str, Any] = {
        "channel_slug": channel_slug,
        "job_id": job_id,
        **SYSTEM_DEFAULTS,
        **(channel_settings or {}),
        **(job_override or {}),
        **(request_override or {}),
    }
    try:
        return PlaylistBrief.model_validate(merged)
    except ValidationError as exc:
        raise PlaylistBuilderValidationError(str(exc)) from exc


def parse_override_json(raw_value: str | None) -> dict[str, Any]:
    if raw_value is None:
        return {}
    raw = str(raw_value).strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PlaylistBuilderValidationError("override_json must be valid JSON object") from exc
    if not isinstance(parsed, dict):
        raise PlaylistBuilderValidationError("override_json must be a JSON object")
    return parsed
