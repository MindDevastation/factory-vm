from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import re
import sqlite3
from typing import Any, Dict, List, Sequence

from services.common import db as dbm

_VARIABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_RELEASE_TITLE_ESTIMATED_MAX_LEN = 1000  # repo-aligned with planner/import_service.py TITLE_TOO_LONG policy
_MAX_DESCRIPTION_LEN = 5000
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


class DescriptionTemplateError(Exception):
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
    rendered_description_preview: str | None
    normalized_length: int
    line_count: int
    validation_errors: Sequence[Dict[str, str]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "syntax_valid": self.syntax_valid,
            "structurally_valid": self.structurally_valid,
            "render_status": self.render_status,
            "used_variables": list(self.used_variables),
            "resolved_values": dict(self.resolved_values),
            "missing_variables": list(self.missing_variables),
            "rendered_description_preview": self.rendered_description_preview,
            "normalized_length": self.normalized_length,
            "line_count": self.line_count,
            "validation_errors": list(self.validation_errors),
        }


def allowed_variables_catalog() -> List[Dict[str, str]]:
    return list(_ALLOWED_VARIABLES)


def parse_template(template_body: str) -> ParseResult:
    if not isinstance(template_body, str):
        return ParseResult(tokens=(), errors=["Template body must be a string"])

    tokens: List[ParsedToken] = []
    errors: List[str] = []
    idx = 0
    while idx < len(template_body):
        if template_body.startswith("{{", idx):
            end = template_body.find("}}", idx + 2)
            if end == -1:
                errors.append("Unmatched '{{' in template")
                break
            inner = template_body[idx + 2 : end]
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
        if template_body.startswith("}}", idx):
            errors.append("Unmatched '}}' in template")
            idx += 2
            continue
        if template_body[idx] in "{}":
            errors.append("Single braces are not allowed")
            idx += 1
            continue
        next_brace = min(
            [pos for pos in (template_body.find("{", idx), template_body.find("}", idx)) if pos != -1] or [len(template_body)]
        )
        tokens.append(ParsedToken(kind="text", value=template_body[idx:next_brace]))
        idx = next_brace

    return ParseResult(tokens=tokens, errors=errors)


def validate_template_for_save(*, channel: Dict[str, Any], template_name: str, template_body: str) -> SaveValidationResult:
    errors: List[Dict[str, str]] = []
    if not template_name.strip():
        errors.append({"code": "MTD_TEMPLATE_NAME_REQUIRED", "message": "template_name is required"})
    if not template_body.strip():
        errors.append({"code": "MTD_TEMPLATE_BODY_REQUIRED", "message": "template_body is required"})

    parsed = parse_template(template_body)
    for err in parsed.errors:
        errors.append({"code": "MTD_TEMPLATE_SYNTAX", "message": err})

    used_variables = sorted({token.value for token in parsed.tokens if token.kind == "var"})
    for var_name in used_variables:
        if var_name not in _ALLOWED_BY_NAME:
            errors.append({"code": "MTD_TEMPLATE_VARIABLE_NOT_ALLOWED", "message": f"Variable not allowed: {var_name}"})

    if "\t" in template_body:
        errors.append({"code": "MTD_TEMPLATE_TAB_NOT_ALLOWED", "message": "Tabs are not allowed"})
    if _CONTROL_CHAR_RE.search(template_body.replace("\n", "")):
        errors.append({"code": "MTD_TEMPLATE_CONTROL_CHAR", "message": "Template contains control characters"})

    for var_name in used_variables:
        if var_name in _CHANNEL_VARIABLES and not _resolve_channel_variable(var_name, channel=channel):
            errors.append({
                "code": "MTD_CHANNEL_CONTEXT_MISSING",
                "message": f"Channel context missing for variable: {var_name}",
            })

    estimated = _estimate_rendered_description(channel=channel, parsed=parsed)
    normalized = normalize_multiline(estimated)
    structural_errors = _structural_errors(normalized)
    errors.extend(structural_errors)

    return SaveValidationResult(
        syntax_valid=not parsed.errors,
        structurally_valid=not structural_errors,
        used_variables=used_variables,
        validation_errors=errors,
    )


def preview_description_template(
    *,
    channel: Dict[str, Any],
    template_body: str,
    release_row: Dict[str, Any] | None,
) -> PreviewResult:
    parsed = parse_template(template_body)
    errors: List[Dict[str, str]] = []
    for err in parsed.errors:
        errors.append({"code": "MTD_TEMPLATE_SYNTAX", "message": err})

    used_variables = sorted({token.value for token in parsed.tokens if token.kind == "var"})
    unknown_variables = sorted(name for name in used_variables if name not in _ALLOWED_BY_NAME)
    for var_name in unknown_variables:
        errors.append({"code": "MTD_TEMPLATE_VARIABLE_NOT_ALLOWED", "message": f"Variable not allowed: {var_name}"})

    if "\t" in template_body:
        errors.append({"code": "MTD_TEMPLATE_TAB_NOT_ALLOWED", "message": "Tabs are not allowed"})
    if _CONTROL_CHAR_RE.search(template_body.replace("\n", "")):
        errors.append({"code": "MTD_TEMPLATE_CONTROL_CHAR", "message": "Template contains control characters"})

    if errors:
        return PreviewResult(
            syntax_valid=not parsed.errors,
            structurally_valid=False,
            render_status="ERROR",
            used_variables=used_variables,
            resolved_values={},
            missing_variables=[],
            rendered_description_preview=None,
            normalized_length=0,
            line_count=0,
            validation_errors=errors,
        )

    resolved_values: Dict[str, str] = {}
    missing_variables: List[str] = []
    parts: List[str] = []
    release_date = _release_date_from_row(release_row)

    for token in parsed.tokens:
        if token.kind == "text":
            parts.append(token.value)
            continue
        resolved = _resolve_variable(token.value, channel=channel, release_row=release_row, release_date=release_date)
        if resolved is None:
            missing_variables.append(token.value)
            parts.append(f"<<missing:{token.value}>>")
            continue
        parts.append(resolved)
        resolved_values[token.value] = resolved

    rendered = "".join(parts)
    normalized = normalize_multiline(rendered)
    structural_errors = _structural_errors(normalized)
    errors.extend(structural_errors)

    if errors:
        return PreviewResult(
            syntax_valid=True,
            structurally_valid=False,
            render_status="ERROR",
            used_variables=used_variables,
            resolved_values=resolved_values,
            missing_variables=sorted(set(missing_variables)),
            rendered_description_preview=normalized or None,
            normalized_length=len(normalized),
            line_count=_line_count(normalized),
            validation_errors=errors,
        )

    return PreviewResult(
        syntax_valid=True,
        structurally_valid=True,
        render_status="PARTIAL" if missing_variables else "FULL",
        used_variables=used_variables,
        resolved_values=resolved_values,
        missing_variables=sorted(set(missing_variables)),
        rendered_description_preview=normalized,
        normalized_length=len(normalized),
        line_count=_line_count(normalized),
        validation_errors=[],
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
        raise DescriptionTemplateError(code="MTD_RELEASE_NOT_FOUND", message="Release not found")
    if str(row.get("channel_slug") or "") != channel_slug:
        raise DescriptionTemplateError(
            code="MTD_RELEASE_CHANNEL_MISMATCH",
            message="Release does not belong to requested channel",
        )
    return row


def normalize_multiline(value: str) -> str:
    normalized_newlines = value.replace("\r\n", "\n").replace("\r", "\n")
    trimmed_lines = [line.rstrip(" ") for line in normalized_newlines.split("\n")]
    return "\n".join(trimmed_lines).strip()


def _structural_errors(normalized: str) -> List[Dict[str, str]]:
    errors: List[Dict[str, str]] = []
    if "\t" in normalized:
        errors.append({"code": "MTD_RENDER_TAB_NOT_ALLOWED", "message": "Rendered description contains tab characters"})
    if _CONTROL_CHAR_RE.search(normalized.replace("\n", "")):
        errors.append({"code": "MTD_RENDER_CONTROL_CHAR", "message": "Rendered description contains control characters"})
    if not normalized:
        errors.append({"code": "MTD_RENDER_EMPTY", "message": "Rendered description is empty after normalization"})
    if len(normalized) > _MAX_DESCRIPTION_LEN:
        errors.append({"code": "MTD_RENDER_TOO_LONG", "message": "Rendered description exceeds 5000 characters"})
    return errors


def _line_count(normalized: str) -> int:
    if not normalized:
        return 0
    return normalized.count("\n") + 1


def _resolve_variable(name: str, *, channel: Dict[str, Any], release_row: Dict[str, Any] | None, release_date: date | None) -> str | None:
    if name in _CHANNEL_VARIABLES:
        return _resolve_channel_variable(name, channel=channel)
    if name == "release_title":
        if not release_row:
            return None
        title = str(release_row.get("title") or "").strip()
        return title or None
    if name in _RELEASE_DATE_VARIABLES:
        if not release_date:
            return None
        if name == "release_year":
            return f"{release_date.year:04d}"
        if name == "release_month_number":
            return f"{release_date.month:02d}"
        return f"{release_date.day:02d}"
    return None


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


def _estimate_rendered_description(*, channel: Dict[str, Any], parsed: ParseResult) -> str:
    pieces: List[str] = []
    for token in parsed.tokens:
        if token.kind == "text":
            pieces.append(token.value)
            continue
        if token.value == "release_title":
            pieces.append("X" * _RELEASE_TITLE_ESTIMATED_MAX_LEN)
            continue
        if token.value == "release_year":
            pieces.append("9999")
            continue
        if token.value == "release_month_number":
            pieces.append("12")
            continue
        if token.value == "release_day_number":
            pieces.append("31")
            continue
        pieces.append(_resolve_channel_variable(token.value, channel=channel) or "")
    return "".join(pieces)


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
        return datetime.fromisoformat(planned_at.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
