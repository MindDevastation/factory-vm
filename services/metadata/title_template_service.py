from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
from typing import Any, Dict, List, Sequence

_VARIABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

_ALLOWED_VARIABLES: List[Dict[str, str]] = [
    {
        "name": "channel_display_name",
        "group": "channel",
        "requires_context": "channel",
        "description": "Display name for the channel.",
    },
    {
        "name": "channel_slug",
        "group": "channel",
        "requires_context": "channel",
        "description": "Channel slug identifier.",
    },
    {
        "name": "channel_kind",
        "group": "channel",
        "requires_context": "channel",
        "description": "Channel kind value from channel settings.",
    },
    {
        "name": "release_year",
        "group": "release_date",
        "requires_context": "release_date",
        "description": "Release year from provided release date.",
    },
    {
        "name": "release_month_number",
        "group": "release_date",
        "requires_context": "release_date",
        "description": "Release month number from provided release date.",
    },
    {
        "name": "release_day_number",
        "group": "release_date",
        "requires_context": "release_date",
        "description": "Release day number from provided release date.",
    },
]
_ALLOWED_BY_NAME = {item["name"]: item for item in _ALLOWED_VARIABLES}


@dataclass(frozen=True)
class ParsedToken:
    kind: str
    value: str


@dataclass(frozen=True)
class ParseResult:
    tokens: Sequence[ParsedToken]
    errors: Sequence[str]


@dataclass(frozen=True)
class PreviewResult:
    render_status: str
    rendered_title: str | None
    missing_variables: Sequence[str]
    validation_errors: Sequence[Dict[str, str]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "render_status": self.render_status,
            "rendered_title": self.rendered_title,
            "missing_variables": list(self.missing_variables),
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


def preview_title_template(*, channel: Dict[str, Any], template_body: str, release_date: date | None) -> PreviewResult:
    parsed = parse_template(template_body)
    validation_errors: List[Dict[str, str]] = []
    for err in parsed.errors:
        validation_errors.append({"code": "MTB_TEMPLATE_SYNTAX", "message": err})

    unknown_vars = sorted({token.value for token in parsed.tokens if token.kind == "var" and token.value not in _ALLOWED_BY_NAME})
    for var_name in unknown_vars:
        validation_errors.append({"code": "MTB_UNKNOWN_VARIABLE", "message": f"Unknown variable: {var_name}"})

    if validation_errors:
        return PreviewResult(render_status="ERROR", rendered_title=None, missing_variables=(), validation_errors=validation_errors)

    resolved_parts: List[str] = []
    missing_variables: List[str] = []
    for token in parsed.tokens:
        if token.kind == "text":
            resolved_parts.append(token.value)
            continue
        value = _resolve_variable(token.value, channel=channel, release_date=release_date)
        if value is None:
            missing_variables.append(token.value)
            resolved_parts.append(f"<<missing:{token.value}>>")
        else:
            resolved_parts.append(value)

    rendered = "".join(resolved_parts)
    if "\n" in rendered or "\r" in rendered or "\t" in rendered:
        validation_errors.append({"code": "MTB_TITLE_CONTROL_CHARS", "message": "Rendered title contains forbidden whitespace control characters"})
    if _CONTROL_CHAR_RE.search(rendered):
        validation_errors.append({"code": "MTB_TITLE_CONTROL_CHARS", "message": "Rendered title contains control characters"})

    normalized = normalize_whitespace(rendered)
    if not normalized:
        validation_errors.append({"code": "MTB_TITLE_EMPTY", "message": "Rendered title is empty after normalization"})
    if len(normalized) > 100:
        validation_errors.append({"code": "MTB_TITLE_TOO_LONG", "message": "Rendered title exceeds 100 characters"})

    if validation_errors:
        return PreviewResult(
            render_status="ERROR",
            rendered_title=normalized or None,
            missing_variables=tuple(sorted(set(missing_variables))),
            validation_errors=validation_errors,
        )

    status = "PARTIAL" if missing_variables else "FULL"
    return PreviewResult(
        render_status=status,
        rendered_title=normalized,
        missing_variables=tuple(sorted(set(missing_variables))),
        validation_errors=(),
    )


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _resolve_variable(name: str, *, channel: Dict[str, Any], release_date: date | None) -> str | None:
    if name == "channel_display_name":
        return str(channel.get("display_name") or "")
    if name == "channel_slug":
        return str(channel.get("slug") or "")
    if name == "channel_kind":
        return str(channel.get("kind") or "")
    if release_date is None:
        return None
    if name == "release_year":
        return f"{release_date.year:04d}"
    if name == "release_month_number":
        return f"{release_date.month:02d}"
    if name == "release_day_number":
        return f"{release_date.day:02d}"
    return None
