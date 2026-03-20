from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import sqlite3
from typing import Any, Dict, List

from services.common import db as dbm
from services.metadata import video_tag_preset_service

_DATE_VARIABLES = {"release_year", "release_month_number", "release_day_number"}


class VideoTagsGenError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ContextResult:
    release_id: int
    channel_slug: str
    current_tags_json: List[str]
    has_existing_tags: bool
    default_preset: Dict[str, Any] | None
    active_presets: List[Dict[str, Any]]
    can_generate_with_default: bool
    release_row: Dict[str, Any]
    channel_row: Dict[str, Any]


@dataclass(frozen=True)
class GenerateResult:
    release_id: int
    channel_slug: str
    current_tags_json: List[str]
    has_existing_tags: bool
    used_preset: Dict[str, Any]
    overwrite_required: bool
    rendered_items_before_normalization: List[str]
    dropped_empty_items: List[str]
    removed_duplicates: List[str]
    proposed_tags_json: List[str]
    normalized_count: int
    generation_fingerprint: str
    warnings: List[str]


@dataclass(frozen=True)
class ApplyResult:
    release_id: int
    channel_slug: str
    used_preset_id: int
    tags_updated: bool
    tags_before: List[str]
    tags_after: List[str]
    overwrite_required: bool
    message: str | None = None


def load_video_tags_context(conn: sqlite3.Connection, *, release_id: int) -> ContextResult:
    release = conn.execute(
        """
        SELECT r.id, r.title, r.tags_json, r.planned_at, r.created_at, c.slug AS channel_slug, c.display_name, c.kind
        FROM releases r
        JOIN channels c ON c.id = r.channel_id
        WHERE r.id = ?
        """,
        (release_id,),
    ).fetchone()
    if not release:
        raise VideoTagsGenError(code="MTV_RELEASE_NOT_FOUND", message="Release not found")

    channel_slug = str(release["channel_slug"])
    normalized_current = _normalize_current_tags_json(release.get("tags_json"))

    default_preset = conn.execute(
        """
        SELECT *
        FROM video_tag_presets
        WHERE channel_slug = ? AND status = 'ACTIVE' AND validation_status = 'VALID' AND is_default = 1
        ORDER BY id DESC
        LIMIT 1
        """,
        (channel_slug,),
    ).fetchone()

    active_rows = conn.execute(
        """
        SELECT id, preset_name, status, is_default
        FROM video_tag_presets
        WHERE channel_slug = ? AND status = 'ACTIVE' AND validation_status = 'VALID'
        ORDER BY id DESC
        """,
        (channel_slug,),
    ).fetchall()
    active_presets = [
        {
            "id": int(row["id"]),
            "preset_name": str(row["preset_name"]),
            "status": str(row["status"] or ""),
            "is_default": bool(int(row["is_default"] or 0)),
        }
        for row in active_rows
    ]

    return ContextResult(
        release_id=int(release["id"]),
        channel_slug=channel_slug,
        current_tags_json=normalized_current,
        has_existing_tags=bool(normalized_current),
        default_preset=dict(default_preset) if default_preset else None,
        active_presets=active_presets,
        can_generate_with_default=default_preset is not None,
        release_row=dict(release),
        channel_row={
            "slug": channel_slug,
            "display_name": str(release.get("display_name") or ""),
            "kind": str(release.get("kind") or ""),
        },
    )


def generate_video_tags_preview(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    preset_id: int | None,
) -> GenerateResult:
    context = load_video_tags_context(conn, release_id=release_id)
    preset = _resolve_preset(conn, channel_slug=context.channel_slug, default_preset=context.default_preset, preset_id=preset_id)

    preset_body = dbm.json_loads(str(preset.get("preset_body_json") or "[]"))
    used_variables = _collect_used_variables(preset_body)

    if "release_title" in used_variables:
        title = str(context.release_row.get("title") or "")
        if not title.strip():
            raise VideoTagsGenError(
                code="MTV_RELEASE_TITLE_NOT_USABLE",
                message="release.title is required and must be non-empty when preset uses release_title",
            )

    release_date = _parse_release_date(context.release_row.get("planned_at"))
    if used_variables.intersection(_DATE_VARIABLES) and release_date is None:
        raise VideoTagsGenError(
            code="MTV_RELEASE_DATE_CONTEXT_MISSING",
            message="Release scheduling datetime is required for date-derived variables",
        )

    preview = video_tag_preset_service.preview_video_tag_preset(
        channel=context.channel_row,
        preset_body=preset_body,
        release_row=context.release_row,
    )
    if preview.missing_variables:
        raise VideoTagsGenError(
            code="MTV_REQUIRED_CONTEXT_MISSING",
            message=f"Missing required context for variables: {', '.join(preview.missing_variables)}",
        )
    if preview.validation_errors:
        first_code = str(preview.validation_errors[0].get("code") or "")
        if first_code in {"MTV_TAG_ITEM_TOO_LONG", "MTV_TAG_COUNT_EXCEEDED", "MTV_TAG_TOTAL_CHARS_EXCEEDED", "MTV_PRESET_EMPTY_AFTER_NORMALIZATION"}:
            raise VideoTagsGenError(code="MTV_PRESET_INVALID", message="Rendered tags failed validation")
        raise VideoTagsGenError(code="MTV_REQUIRED_CONTEXT_MISSING", message="Required render context is missing")
    if preview.render_status != "FULL":
        raise VideoTagsGenError(code="MTV_RENDER_FAILED", message="Failed to render proposed tags")
    if not preview.final_normalized_tags:
        raise VideoTagsGenError(code="MTV_RENDER_FAILED", message="Rendered tags cannot be empty")

    overwrite_required = bool(context.current_tags_json)
    warnings: List[str] = []
    if overwrite_required:
        warnings.append("Applying this result will overwrite the existing release tags.")

    fingerprint = _build_generation_fingerprint(
        release_id=context.release_id,
        channel_slug=context.channel_slug,
        preset_id=int(preset["id"]),
        preset_updated_at=preset.get("updated_at"),
        release_context_version=_build_release_context_version(context.release_row),
        proposed_tags_json=list(preview.final_normalized_tags),
    )

    return GenerateResult(
        release_id=context.release_id,
        channel_slug=context.channel_slug,
        current_tags_json=context.current_tags_json,
        has_existing_tags=context.has_existing_tags,
        used_preset={
            "id": int(preset["id"]),
            "preset_name": str(preset["preset_name"]),
            "is_default_channel_preset": bool(int(preset.get("is_default") or 0)),
        },
        overwrite_required=overwrite_required,
        rendered_items_before_normalization=list(preview.rendered_items_before_normalization),
        dropped_empty_items=list(preview.dropped_empty_items),
        removed_duplicates=list(preview.removed_duplicates),
        proposed_tags_json=list(preview.final_normalized_tags),
        normalized_count=preview.normalized_count,
        generation_fingerprint=fingerprint,
        warnings=warnings,
    )


def apply_generated_video_tags(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    preset_id: int | None,
    generation_fingerprint: str,
    overwrite_confirmed: bool,
) -> ApplyResult:
    regenerated = generate_video_tags_preview(conn, release_id=release_id, preset_id=preset_id)
    if generation_fingerprint != regenerated.generation_fingerprint:
        raise VideoTagsGenError(code="MTV_PREVIEW_STALE", message="Generated preview is stale; regenerate before apply")

    tags_before = list(regenerated.current_tags_json)
    proposed_tags = list(regenerated.proposed_tags_json)
    overwrite_required = bool(tags_before) and tags_before != proposed_tags

    if tags_before == proposed_tags:
        return ApplyResult(
            release_id=regenerated.release_id,
            channel_slug=regenerated.channel_slug,
            used_preset_id=int(regenerated.used_preset["id"]),
            tags_updated=False,
            tags_before=tags_before,
            tags_after=tags_before,
            overwrite_required=False,
            message="Release tags already match generated result.",
        )

    if overwrite_required and not overwrite_confirmed:
        raise VideoTagsGenError(
            code="MTV_OVERWRITE_CONFIRMATION_REQUIRED",
            message="overwrite_confirmed=true is required to replace existing release tags",
        )

    conn.execute(
        "UPDATE releases SET tags_json = ? WHERE id = ?",
        (dbm.json_dumps(proposed_tags), release_id),
    )
    conn.commit()

    return ApplyResult(
        release_id=regenerated.release_id,
        channel_slug=regenerated.channel_slug,
        used_preset_id=int(regenerated.used_preset["id"]),
        tags_updated=True,
        tags_before=tags_before,
        tags_after=proposed_tags,
        overwrite_required=overwrite_required,
    )


def _resolve_preset(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    default_preset: Dict[str, Any] | None,
    preset_id: int | None,
) -> Dict[str, Any]:
    if preset_id is None:
        if default_preset is None:
            raise VideoTagsGenError(
                code="MTV_DEFAULT_PRESET_NOT_CONFIGURED",
                message="No active default video tag preset configured for release channel",
            )
        preset = default_preset
    else:
        row = dbm.get_video_tag_preset_by_id(conn, preset_id)
        if not row:
            raise VideoTagsGenError(code="MTV_PRESET_NOT_FOUND", message="Preset not found")
        preset = dict(row)

    if str(preset.get("channel_slug") or "") != channel_slug:
        raise VideoTagsGenError(code="MTV_PRESET_CHANNEL_MISMATCH", message="Preset does not belong to release channel")
    if str(preset.get("status") or "") != "ACTIVE":
        raise VideoTagsGenError(code="MTV_PRESET_NOT_ACTIVE", message="Preset must be ACTIVE")
    if str(preset.get("validation_status") or "") != "VALID":
        raise VideoTagsGenError(code="MTV_PRESET_INVALID", message="Preset must be VALID")
    return preset


def _collect_used_variables(preset_body: List[str]) -> set[str]:
    used: set[str] = set()
    for item in preset_body:
        parsed = video_tag_preset_service.parse_tag_item(item)
        if parsed.errors:
            raise VideoTagsGenError(code="MTV_RENDER_FAILED", message="Preset template syntax is invalid")
        for token in parsed.tokens:
            if token.kind == "var":
                used.add(token.value)
    return used


def _normalize_current_tags_json(tags_json_value: Any) -> List[str]:
    try:
        raw = dbm.json_loads(str(tags_json_value or "[]"))
    except Exception:
        raw = []
    if not isinstance(raw, list):
        raw = []
    string_items = [str(item) for item in raw if isinstance(item, str)]
    return video_tag_preset_service._normalize_items(string_items)["final_normalized_tags"]


def _parse_release_date(value: Any):
    return video_tag_preset_service._release_date_from_row({"planned_at": value})


def _build_release_context_version(release_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": release_row.get("title"),
        "planned_at": release_row.get("planned_at"),
        "created_at": release_row.get("created_at"),
    }


def _build_generation_fingerprint(
    *,
    release_id: int,
    channel_slug: str,
    preset_id: int,
    preset_updated_at: Any,
    release_context_version: Dict[str, Any],
    proposed_tags_json: List[str],
) -> str:
    payload = {
        "release_id": release_id,
        "channel_slug": channel_slug,
        "preset_id": preset_id,
        "preset_updated_at": preset_updated_at,
        "release_context_version": release_context_version,
        "proposed_tags_json": proposed_tags_json,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
