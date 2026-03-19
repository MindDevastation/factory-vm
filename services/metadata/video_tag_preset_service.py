from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import re
import sqlite3
from typing import Any, Dict, List, Sequence

from services.common import db as dbm

_VARIABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_RELEASE_TITLE_ESTIMATED_MAX_LEN = 1000
_MAX_TAG_ITEM_LEN = 500
_MAX_TAG_COUNT = 500
_MAX_COMBINED_CHARS = 5000

_CHANNEL_VARIABLES = {"channel_display_name", "channel_slug", "channel_kind"}
_RELEASE_VARIABLES = {"release_title"}
_RELEASE_DATE_VARIABLES = {"release_year", "release_month_number", "release_day_number"}

_ALLOWED_VARIABLES: List[Dict[str, str]] = [
    {
        "name": "channel_display_name",
        "group": "channel",
        "requires_context": "channel",
        "description": "Human-readable channel name",
    },
    {
        "name": "channel_slug",
        "group": "channel",
        "requires_context": "channel",
        "description": "Stable channel slug identifier",
    },
    {
        "name": "channel_kind",
        "group": "channel",
        "requires_context": "channel",
        "description": "Channel kind value",
    },
    {
        "name": "release_title",
        "group": "release",
        "requires_context": "release",
        "description": "Current release.title value, if usable",
    },
    {
        "name": "release_year",
        "group": "release_date",
        "requires_context": "release_date",
        "description": "4-digit year from canonical release scheduling datetime",
    },
    {
        "name": "release_month_number",
        "group": "release_date",
        "requires_context": "release_date",
        "description": "2-digit month from canonical release scheduling datetime",
    },
    {
        "name": "release_day_number",
        "group": "release_date",
        "requires_context": "release_date",
        "description": "2-digit day from canonical release scheduling datetime",
    },
]
_ALLOWED_BY_NAME = {item["name"]: item for item in _ALLOWED_VARIABLES}


class VideoTagPresetError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ParsedToken:
    kind: str
    value: str


@dataclass(frozen=True)
class ParseResult:
    tokens: Sequence[ParsedToken]
    errors: Sequence[str]


@dataclass(frozen=True)
class SaveValidationResult:
    syntax_valid: bool
    structurally_valid: bool
    used_variables: Sequence[str]
    validation_errors: Sequence[Dict[str, str]]


@dataclass(frozen=True)
class PreviewResult:
    syntax_valid: bool
    structurally_valid: bool
    render_status: str
    used_variables: Sequence[str]
    resolved_values: Dict[str, str]
    missing_variables: Sequence[str]
    rendered_items_before_normalization: Sequence[str]
    dropped_empty_items: Sequence[str]
    removed_duplicates: Sequence[str]
    final_normalized_tags: Sequence[str]
    normalized_count: int
    validation_errors: Sequence[Dict[str, str]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "syntax_valid": self.syntax_valid,
            "structurally_valid": self.structurally_valid,
            "render_status": self.render_status,
            "used_variables": list(self.used_variables),
            "resolved_values": dict(self.resolved_values),
            "missing_variables": list(self.missing_variables),
            "rendered_items_before_normalization": list(self.rendered_items_before_normalization),
            "dropped_empty_items": list(self.dropped_empty_items),
            "removed_duplicates": list(self.removed_duplicates),
            "final_normalized_tags": list(self.final_normalized_tags),
            "normalized_count": self.normalized_count,
            "validation_errors": list(self.validation_errors),
        }


def allowed_variables_catalog() -> List[Dict[str, str]]:
    return list(_ALLOWED_VARIABLES)


def parse_tag_item(item_template: str) -> ParseResult:
    if not isinstance(item_template, str):
        return ParseResult(tokens=(), errors=["Tag item template must be a string"])

    tokens: List[ParsedToken] = []
    errors: List[str] = []
    idx = 0
    while idx < len(item_template):
        if item_template.startswith("{{", idx):
            end = item_template.find("}}", idx + 2)
            if end == -1:
                errors.append("Unmatched '{{' in template")
                break
            inner = item_template[idx + 2 : end]
            if "{" in inner or "}" in inner:
                errors.append("Nested braces are not allowed")
                idx = end + 2
                continue
            name = inner.strip()
            if not name:
                errors.append("Empty placeholder is not allowed")
                idx = end + 2
                continue
            if not _VARIABLE_NAME_RE.fullmatch(name):
                errors.append(f"Invalid placeholder name: {name}")
                idx = end + 2
                continue
            tokens.append(ParsedToken(kind="var", value=name))
            idx = end + 2
            continue
        if item_template.startswith("}}", idx):
            errors.append("Unmatched '}}' in template")
            idx += 2
            continue
        if item_template[idx] in "{}":
            errors.append("Single braces are not allowed")
            idx += 1
            continue
        next_brace = min([pos for pos in (item_template.find("{", idx), item_template.find("}", idx)) if pos != -1] or [len(item_template)])
        tokens.append(ParsedToken(kind="text", value=item_template[idx:next_brace]))
        idx = next_brace

    return ParseResult(tokens=tokens, errors=errors)


def validate_preset_for_save(*, channel: Dict[str, Any], preset_name: str, preset_body_json: str) -> SaveValidationResult:
    errors: List[Dict[str, str]] = []
    if not str(preset_name or "").strip():
        errors.append({"code": "MTV_PRESET_NAME_REQUIRED", "message": "preset_name is required"})

    body_items = _decode_preset_body_json(preset_body_json)
    if isinstance(body_items, list):
        if not body_items:
            errors.append({"code": "MTV_PRESET_BODY_EMPTY", "message": "preset_body must be a non-empty array"})
        if any(not isinstance(item, str) for item in body_items):
            errors.append({"code": "MTV_PRESET_BODY_ITEM_TYPE", "message": "preset_body items must all be strings"})
    else:
        errors.extend(body_items)
        body_items = []

    syntax_errors: List[str] = []
    used_variables: set[str] = set()
    contains_non_whitespace_static = False
    parsed_items: List[ParseResult] = []

    for item in body_items:
        if not isinstance(item, str):
            continue
        parsed = parse_tag_item(item)
        parsed_items.append(parsed)
        syntax_errors.extend(parsed.errors)
        for token in parsed.tokens:
            if token.kind == "var":
                used_variables.add(token.value)
            elif token.value.strip():
                contains_non_whitespace_static = True

    for err in syntax_errors:
        errors.append({"code": "MTV_TEMPLATE_SYNTAX", "message": err})

    for var_name in sorted(used_variables):
        if var_name not in _ALLOWED_BY_NAME:
            errors.append({"code": "MTV_TEMPLATE_VARIABLE_NOT_ALLOWED", "message": f"Variable not allowed: {var_name}"})

    if not contains_non_whitespace_static and not used_variables and body_items:
        errors.append({"code": "MTV_PRESET_MEANINGLESS", "message": "preset_body cannot be only whitespace items"})

    for var_name in sorted(used_variables):
        if var_name in _CHANNEL_VARIABLES and _resolve_channel_variable(var_name, channel=channel) is None:
            errors.append({"code": "MTV_CHANNEL_CONTEXT_MISSING", "message": f"Channel context missing for variable: {var_name}"})

    estimated = _render_for_estimation(channel=channel, parsed_items=parsed_items)
    normalized = _normalize_items(estimated)
    structural_errors = _structural_errors(normalized)
    if not normalized["final_normalized_tags"]:
        structural_errors.append(
            {"code": "MTV_PRESET_EMPTY_AFTER_NORMALIZATION", "message": "Normalized tags cannot be empty"}
        )
    errors.extend(structural_errors)

    return SaveValidationResult(
        syntax_valid=not bool(syntax_errors),
        structurally_valid=not bool(structural_errors),
        used_variables=sorted(used_variables),
        validation_errors=errors,
    )


def preview_video_tag_preset(*, channel: Dict[str, Any], preset_body: Sequence[str], release_row: Dict[str, Any] | None) -> PreviewResult:
    if any(not isinstance(item, str) for item in preset_body):
        return PreviewResult(
            syntax_valid=False,
            structurally_valid=False,
            render_status="ERROR",
            used_variables=[],
            resolved_values={},
            missing_variables=[],
            rendered_items_before_normalization=[],
            dropped_empty_items=[],
            removed_duplicates=[],
            final_normalized_tags=[],
            normalized_count=0,
            validation_errors=[{"code": "MTV_PRESET_BODY_ITEM_TYPE", "message": "preset_body items must all be strings"}],
        )

    errors: List[Dict[str, str]] = []
    all_parsed: List[ParseResult] = []
    used_variables: set[str] = set()

    for item in preset_body:
        parsed = parse_tag_item(item)
        all_parsed.append(parsed)
        for err in parsed.errors:
            errors.append({"code": "MTV_TEMPLATE_SYNTAX", "message": err})
        for token in parsed.tokens:
            if token.kind == "var":
                used_variables.add(token.value)

    for var_name in sorted(used_variables):
        if var_name not in _ALLOWED_BY_NAME:
            errors.append({"code": "MTV_TEMPLATE_VARIABLE_NOT_ALLOWED", "message": f"Variable not allowed: {var_name}"})

    if errors:
        return PreviewResult(
            syntax_valid=all(not parsed.errors for parsed in all_parsed),
            structurally_valid=False,
            render_status="ERROR",
            used_variables=sorted(used_variables),
            resolved_values={},
            missing_variables=[],
            rendered_items_before_normalization=[],
            dropped_empty_items=[],
            removed_duplicates=[],
            final_normalized_tags=[],
            normalized_count=0,
            validation_errors=errors,
        )

    release_date = _release_date_from_row(release_row)
    resolved_values: Dict[str, str] = {}
    missing_variables: List[str] = []
    rendered_items: List[str] = []

    for parsed in all_parsed:
        parts: List[str] = []
        for token in parsed.tokens:
            if token.kind == "text":
                parts.append(token.value)
                continue
            resolved = _resolve_variable(token.value, channel=channel, release_row=release_row, release_date=release_date)
            if resolved is None:
                missing_variables.append(token.value)
                parts.append(f"<<missing:{token.value}>>")
            else:
                parts.append(resolved)
                resolved_values[token.value] = resolved
        rendered_items.append("".join(parts))

    normalized = _normalize_items(rendered_items)
    errors.extend(_structural_errors(normalized))
    if not normalized["final_normalized_tags"]:
        errors.append({"code": "MTV_PRESET_EMPTY_AFTER_NORMALIZATION", "message": "Normalized tags cannot be empty"})

    if errors:
        status = "ERROR"
        structurally_valid = False
    else:
        status = "PARTIAL" if missing_variables else "FULL"
        structurally_valid = True

    return PreviewResult(
        syntax_valid=True,
        structurally_valid=structurally_valid,
        render_status=status,
        used_variables=sorted(used_variables),
        resolved_values=resolved_values,
        missing_variables=sorted(set(missing_variables)),
        rendered_items_before_normalization=rendered_items,
        dropped_empty_items=normalized["dropped_empty_items"],
        removed_duplicates=normalized["removed_duplicates"],
        final_normalized_tags=normalized["final_normalized_tags"],
        normalized_count=len(normalized["final_normalized_tags"]),
        validation_errors=errors,
    )


def load_preview_release_context(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    release_id: int | None,
) -> Dict[str, Any] | None:
    if release_id is None:
        return None
    row = conn.execute(
        """
        SELECT r.id, r.title, r.planned_at, c.slug AS channel_slug
        FROM releases r
        JOIN channels c ON c.id = r.channel_id
        WHERE r.id = ?
        """,
        (release_id,),
    ).fetchone()
    if not row:
        raise VideoTagPresetError(code="MTV_RELEASE_NOT_FOUND", message="Release not found")
    if str(row.get("channel_slug") or "") != channel_slug:
        raise VideoTagPresetError(code="MTV_RELEASE_CHANNEL_MISMATCH", message="Release does not belong to requested channel")
    return row


def _decode_preset_body_json(preset_body_json: str) -> List[str] | List[Dict[str, str]]:
    try:
        decoded = json.loads(preset_body_json)
    except Exception:
        return [{"code": "MTV_PRESET_BODY_JSON_INVALID", "message": "preset_body_json must be valid JSON"}]
    if not isinstance(decoded, list):
        return [{"code": "MTV_PRESET_BODY_NOT_ARRAY", "message": "preset_body_json must decode to an array"}]
    return decoded


def _render_for_estimation(*, channel: Dict[str, Any], parsed_items: Sequence[ParseResult]) -> List[str]:
    values = {
        "channel_display_name": _resolve_channel_variable("channel_display_name", channel=channel) or "",
        "channel_slug": _resolve_channel_variable("channel_slug", channel=channel) or "",
        "channel_kind": _resolve_channel_variable("channel_kind", channel=channel) or "",
        "release_title": "x" * _RELEASE_TITLE_ESTIMATED_MAX_LEN,
        "release_year": "9999",
        "release_month_number": "12",
        "release_day_number": "31",
    }
    items: List[str] = []
    for parsed in parsed_items:
        parts: List[str] = []
        for token in parsed.tokens:
            if token.kind == "text":
                parts.append(token.value)
            else:
                parts.append(values.get(token.value, ""))
        items.append("".join(parts))
    return items


def _normalize_items(items: Sequence[str]) -> Dict[str, List[str]]:
    dropped_empty: List[str] = []
    removed_duplicates: List[str] = []
    final_items: List[str] = []
    seen: set[str] = set()

    for item in items:
        trimmed = item.strip()
        if not trimmed:
            dropped_empty.append(item)
            continue
        if trimmed in seen:
            removed_duplicates.append(trimmed)
            continue
        seen.add(trimmed)
        final_items.append(trimmed)

    return {
        "dropped_empty_items": dropped_empty,
        "removed_duplicates": removed_duplicates,
        "final_normalized_tags": final_items,
    }


def _structural_errors(normalized: Dict[str, List[str]]) -> List[Dict[str, str]]:
    errors: List[Dict[str, str]] = []
    final_items = normalized["final_normalized_tags"]
    for value in final_items:
        if len(value) > _MAX_TAG_ITEM_LEN:
            errors.append({"code": "MTV_TAG_ITEM_TOO_LONG", "message": "Normalized tag item exceeds 500 characters"})
    if len(final_items) > _MAX_TAG_COUNT:
        errors.append({"code": "MTV_TAG_COUNT_EXCEEDED", "message": "Normalized tag count exceeds 500"})
    if sum(len(value) for value in final_items) > _MAX_COMBINED_CHARS:
        errors.append({"code": "MTV_TAG_TOTAL_CHARS_EXCEEDED", "message": "Normalized combined tag length exceeds 5000"})
    return errors


def _resolve_channel_variable(name: str, *, channel: Dict[str, Any]) -> str | None:
    if name == "channel_display_name":
        value = str(channel.get("display_name") or "").strip()
        return value or None
    if name == "channel_slug":
        value = str(channel.get("slug") or "").strip()
        return value or None
    if name == "channel_kind":
        value = str(channel.get("kind") or "").strip()
        return value or None
    return None


def _resolve_variable(name: str, *, channel: Dict[str, Any], release_row: Dict[str, Any] | None, release_date: date | None) -> str | None:
    if name in _CHANNEL_VARIABLES:
        return _resolve_channel_variable(name, channel=channel)
    if name == "release_title":
        if not release_row:
            return None
        value = str(release_row.get("title") or "")
        return value if value.strip() else None
    if name == "release_year":
        return str(release_date.year) if release_date else None
    if name == "release_month_number":
        return f"{release_date.month:02d}" if release_date else None
    if name == "release_day_number":
        return f"{release_date.day:02d}" if release_date else None
    return None


def _release_date_from_row(release_row: Dict[str, Any] | None) -> date | None:
    if not release_row:
        return None
    planned_at = str(release_row.get("planned_at") or "").strip()
    if not planned_at:
        return None
    try:
        if len(planned_at) >= 10:
            return date.fromisoformat(planned_at[:10])
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(planned_at.replace("Z", "+00:00")).astimezone(timezone.utc).date()
    except ValueError:
        return None
