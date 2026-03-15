from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import re
import sqlite3
from typing import Any, Dict, List, Sequence

from services.common import db as dbm

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


class TemplateValidationError(Exception):
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


def create_title_template(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    template_name: str,
    template_body: str,
    make_default: bool,
) -> Dict[str, Any]:
    channel = dbm.get_channel_by_slug(conn, channel_slug)
    if not channel:
        raise TemplateValidationError(code="MTB_CHANNEL_NOT_FOUND", message="Channel not found")
    _validate_for_write(channel=channel, template_name=template_name, template_body=template_body)

    now_iso = _now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        if make_default:
            dbm.unset_active_default_title_template(conn, channel_slug=channel_slug)
        template_id = dbm.create_title_template(
            conn,
            channel_slug=channel_slug,
            template_name=template_name.strip(),
            template_body=template_body.strip(),
            status="ACTIVE",
            is_default=make_default,
            validation_status="VALID",
            validation_errors_json=None,
            last_validated_at=now_iso,
            created_at=now_iso,
            updated_at=now_iso,
            archived_at=None,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    row = dbm.get_title_template_by_id(conn, template_id)
    assert row is not None
    return _serialize_template(row)


def list_title_templates(
    conn: sqlite3.Connection,
    *,
    channel_slug: str | None,
    status_filter: str,
    q: str | None,
) -> List[Dict[str, Any]]:
    status: str | None
    if status_filter == "active":
        status = "ACTIVE"
    elif status_filter == "archived":
        status = "ARCHIVED"
    else:
        status = None
    rows = dbm.list_title_templates(conn, channel_slug=channel_slug, status=status, q=(q or None))
    return [_serialize_template(row) for row in rows]


def get_title_template(conn: sqlite3.Connection, *, template_id: int) -> Dict[str, Any]:
    row = dbm.get_title_template_by_id(conn, template_id)
    if not row:
        raise TemplateValidationError(code="MTB_TEMPLATE_NOT_FOUND", message="Template not found")
    return _serialize_template(row)


def update_title_template(
    conn: sqlite3.Connection,
    *,
    template_id: int,
    template_name: str | None,
    template_body: str | None,
) -> Dict[str, Any]:
    row = dbm.get_title_template_by_id(conn, template_id)
    if not row:
        raise TemplateValidationError(code="MTB_TEMPLATE_NOT_FOUND", message="Template not found")
    channel = dbm.get_channel_by_slug(conn, str(row.get("channel_slug") or ""))
    if not channel:
        raise TemplateValidationError(code="MTB_CHANNEL_NOT_FOUND", message="Channel not found")

    next_name = (template_name if template_name is not None else str(row.get("template_name") or "")).strip()
    next_body = (template_body if template_body is not None else str(row.get("template_body") or "")).strip()
    _validate_for_write(channel=channel, template_name=next_name, template_body=next_body)

    now_iso = _now_iso()
    dbm.update_title_template_fields(
        conn,
        template_id=template_id,
        template_name=next_name,
        template_body=next_body,
        validation_status="VALID",
        validation_errors_json=None,
        last_validated_at=now_iso,
        updated_at=now_iso,
    )
    saved = dbm.get_title_template_by_id(conn, template_id)
    assert saved is not None
    return _serialize_template(saved)


def set_default_title_template(conn: sqlite3.Connection, *, template_id: int) -> Dict[str, Any]:
    row = dbm.get_title_template_by_id(conn, template_id)
    if not row:
        raise TemplateValidationError(code="MTB_TEMPLATE_NOT_FOUND", message="Template not found")
    if str(row.get("status") or "") == "ARCHIVED":
        raise TemplateValidationError(
            code="MTB_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_DEFAULT",
            message="Archived template cannot be set as default",
        )
    if str(row.get("validation_status") or "") != "VALID":
        raise TemplateValidationError(
            code="MTB_INVALID_TEMPLATE_CANNOT_BE_DEFAULT",
            message="Invalid template cannot be set as default",
        )
    if bool(int(row.get("is_default") or 0)):
        return _serialize_template(row)

    now_iso = _now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        current = dbm.get_title_template_by_id(conn, template_id)
        if not current:
            raise TemplateValidationError(code="MTB_TEMPLATE_NOT_FOUND", message="Template not found")
        if str(current.get("status") or "") == "ARCHIVED":
            raise TemplateValidationError(
                code="MTB_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_DEFAULT",
                message="Archived template cannot be set as default",
            )
        if str(current.get("validation_status") or "") != "VALID":
            raise TemplateValidationError(
                code="MTB_INVALID_TEMPLATE_CANNOT_BE_DEFAULT",
                message="Invalid template cannot be set as default",
            )
        dbm.unset_active_default_title_template(conn, channel_slug=str(current.get("channel_slug") or ""))
        dbm.set_title_template_default_flag(conn, template_id=template_id, is_default=True, updated_at=now_iso)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    saved = dbm.get_title_template_by_id(conn, template_id)
    assert saved is not None
    return _serialize_template(saved)


def archive_title_template(conn: sqlite3.Connection, *, template_id: int) -> Dict[str, Any]:
    row = dbm.get_title_template_by_id(conn, template_id)
    if not row:
        raise TemplateValidationError(code="MTB_TEMPLATE_NOT_FOUND", message="Template not found")
    if str(row.get("status") or "") == "ARCHIVED":
        return _serialize_template(row)

    now_iso = _now_iso()
    dbm.archive_title_template(conn, template_id=template_id, updated_at=now_iso, archived_at=now_iso)
    saved = dbm.get_title_template_by_id(conn, template_id)
    assert saved is not None
    return _serialize_template(saved)


def activate_title_template(conn: sqlite3.Connection, *, template_id: int) -> Dict[str, Any]:
    row = dbm.get_title_template_by_id(conn, template_id)
    if not row:
        raise TemplateValidationError(code="MTB_TEMPLATE_NOT_FOUND", message="Template not found")
    if str(row.get("status") or "") == "ACTIVE":
        return _serialize_template(row)

    now_iso = _now_iso()
    dbm.activate_title_template(conn, template_id=template_id, updated_at=now_iso)
    saved = dbm.get_title_template_by_id(conn, template_id)
    assert saved is not None
    return _serialize_template(saved)


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


def _validate_for_write(*, channel: Dict[str, Any], template_name: str, template_body: str) -> None:
    if not template_name.strip():
        raise TemplateValidationError(code="MTB_TEMPLATE_NAME_REQUIRED", message="template_name is required")
    if not template_body.strip():
        raise TemplateValidationError(code="MTB_TEMPLATE_BODY_REQUIRED", message="template_body is required")

    parsed = parse_template(template_body)
    if parsed.errors:
        raise TemplateValidationError(code="MTB_TEMPLATE_SYNTAX_INVALID", message=parsed.errors[0])

    unknown_vars = sorted({token.value for token in parsed.tokens if token.kind == "var" and token.value not in _ALLOWED_BY_NAME})
    if unknown_vars:
        raise TemplateValidationError(
            code="MTB_TEMPLATE_VARIABLE_NOT_ALLOWED",
            message=f"Variable not allowed: {unknown_vars[0]}",
        )

    missing_channel_vars = sorted(
        {
            token.value
            for token in parsed.tokens
            if token.kind == "var"
            and token.value in {"channel_display_name", "channel_slug", "channel_kind"}
            and not _resolve_variable(token.value, channel=channel, release_date=None)
        }
    )
    if missing_channel_vars:
        raise TemplateValidationError(
            code="MTB_CHANNEL_CONTEXT_MISSING",
            message=f"Channel context missing for variable: {missing_channel_vars[0]}",
        )

    estimated = _estimate_rendered_title(channel=channel, parsed=parsed)
    if "\n" in estimated or "\r" in estimated or "\t" in estimated or _CONTROL_CHAR_RE.search(estimated):
        raise TemplateValidationError(code="MTB_TITLE_EMPTY", message="Title contains forbidden control characters")

    normalized = normalize_whitespace(estimated)
    if not normalized or not re.search(r"[A-Za-z0-9]", normalized):
        raise TemplateValidationError(code="MTB_TITLE_EMPTY", message="Title is empty after normalization")
    if len(normalized) > 100:
        raise TemplateValidationError(code="MTB_TITLE_TOO_LONG", message="Title exceeds 100 characters")


def _estimate_rendered_title(*, channel: Dict[str, Any], parsed: ParseResult) -> str:
    parts: list[str] = []
    for token in parsed.tokens:
        if token.kind == "text":
            parts.append(token.value)
            continue
        if token.value == "release_year":
            parts.append("9999")
            continue
        if token.value == "release_month_number":
            parts.append("12")
            continue
        if token.value == "release_day_number":
            parts.append("31")
            continue
        resolved = _resolve_variable(token.value, channel=channel, release_date=None)
        parts.append(resolved or "")
    return "".join(parts)


def _serialize_template(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "channel_slug": str(row["channel_slug"]),
        "template_name": str(row["template_name"]),
        "template_body": str(row["template_body"]),
        "status": str(row["status"]),
        "is_default": bool(int(row.get("is_default") or 0)),
        "validation_status": str(row["validation_status"]),
        "validation_errors": dbm.json_loads(str(row["validation_errors_json"])) if row.get("validation_errors_json") else [],
        "last_validated_at": row.get("last_validated_at"),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
        "archived_at": row.get("archived_at"),
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
