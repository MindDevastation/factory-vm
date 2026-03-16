from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import sqlite3
from typing import Any, Dict, List

from services.metadata import title_template_service

_DATE_VARIABLES = {"release_year", "release_month_number", "release_day_number"}


class TitleGenError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ContextResult:
    release_id: int
    channel_slug: str
    current_title: str
    has_existing_title: bool
    default_template: Dict[str, Any] | None
    active_templates: List[Dict[str, Any]]
    can_generate_with_default: bool
    channel: Dict[str, Any]
    planned_at: Any


@dataclass(frozen=True)
class GenerateResult:
    release_id: int
    channel_slug: str
    current_title: str
    has_existing_title: bool
    used_template: Dict[str, Any]
    proposed_title: str
    normalized_length: int
    overwrite_required: bool
    warnings: List[Dict[str, str]]
    generation_fingerprint: str


def load_titlegen_context(conn: sqlite3.Connection, *, release_id: int) -> ContextResult:
    release = conn.execute(
        """
        SELECT r.id, r.title, r.planned_at, c.slug AS channel_slug, c.display_name, c.kind
        FROM releases r
        JOIN channels c ON c.id = r.channel_id
        WHERE r.id = ?
        """,
        (release_id,),
    ).fetchone()
    if not release:
        raise TitleGenError(code="MTG_RELEASE_NOT_FOUND", message="Release not found")

    release_id_value = int(release["id"])
    current_title = str(release["title"] or "")
    channel_slug = str(release["channel_slug"])
    channel_row = {
        "slug": channel_slug,
        "display_name": str(release["display_name"] or ""),
        "kind": str(release["kind"] or ""),
    }

    default_template = conn.execute(
        """
        SELECT *
        FROM title_templates
        WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1
        ORDER BY id DESC
        LIMIT 1
        """,
        (channel_slug,),
    ).fetchone()

    active_template_rows = conn.execute(
        """
        SELECT id, template_name, status, is_default
        FROM title_templates
        WHERE channel_slug = ? AND status = 'ACTIVE'
        ORDER BY id DESC
        """,
        (channel_slug,),
    ).fetchall()
    active_templates = [
        {
            "id": int(row["id"]),
            "template_name": str(row["template_name"]),
            "status": str(row["status"] or ""),
            "is_default": bool(int(row["is_default"] or 0)),
        }
        for row in active_template_rows
    ]

    trimmed_title = current_title.strip()
    return ContextResult(
        release_id=release_id_value,
        channel_slug=channel_slug,
        current_title=current_title,
        has_existing_title=bool(trimmed_title),
        default_template=dict(default_template) if default_template else None,
        active_templates=active_templates,
        can_generate_with_default=_can_generate_with_default(
            channel=channel_row,
            planned_at=release["planned_at"],
            default_template=dict(default_template) if default_template else None,
        ),
        channel=channel_row,
        planned_at=release["planned_at"],
    )


def generate_title_preview(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    template_id: int | None,
) -> GenerateResult:
    context = load_titlegen_context(conn, release_id=release_id)

    if template_id is None:
        template = context.default_template
        if template is None:
            raise TitleGenError(
                code="MTG_DEFAULT_TEMPLATE_NOT_CONFIGURED",
                message="No active default title template configured for release channel",
            )
        source = "default"
    else:
        row = conn.execute("SELECT * FROM title_templates WHERE id = ?", (template_id,)).fetchone()
        if not row:
            raise TitleGenError(code="MTG_TEMPLATE_NOT_FOUND", message="Template not found")
        template = dict(row)
        source = "explicit"

    assert template is not None
    if str(template.get("channel_slug") or "") != context.channel_slug:
        raise TitleGenError(code="MTG_TEMPLATE_CHANNEL_MISMATCH", message="Template does not belong to release channel")
    if str(template.get("status") or "") != "ACTIVE":
        raise TitleGenError(code="MTG_TEMPLATE_NOT_ACTIVE", message="Template must be ACTIVE")
    if str(template.get("validation_status") or "") != "VALID":
        raise TitleGenError(code="MTG_TEMPLATE_INVALID", message="Template must be VALID")

    body = str(template.get("template_body") or "")
    parsed = title_template_service.parse_template(body)
    needs_release_date = any(token.kind == "var" and token.value in _DATE_VARIABLES for token in parsed.tokens)
    release_date = _parse_release_date(context.planned_at)
    if needs_release_date and release_date is None:
        raise TitleGenError(
            code="MTG_REQUIRED_CONTEXT_MISSING",
            message="Release scheduling datetime is required for template date variables",
        )

    preview = title_template_service.preview_title_template(
        channel=context.channel,
        template_body=body,
        release_date=release_date,
    )
    if preview.missing_variables:
        raise TitleGenError(
            code="MTG_REQUIRED_CONTEXT_MISSING",
            message=f"Missing required context for variables: {', '.join(preview.missing_variables)}",
        )
    if preview.render_status != "FULL" or preview.validation_errors or not preview.rendered_title:
        raise TitleGenError(code="MTG_RENDER_FAILED", message="Failed to render proposed title")

    warnings: List[Dict[str, str]] = []
    if context.has_existing_title:
        warnings.append(
            {
                "code": "MTG_OVERWRITE_REQUIRED",
                "message": "Release already has a non-empty title; apply would overwrite it",
            }
        )

    fingerprint = _build_generation_fingerprint(
        release_id=context.release_id,
        channel_slug=context.channel_slug,
        template_id=int(template["id"]),
        template_updated_at=template.get("updated_at"),
        effective_render_context=_build_effective_render_context(channel=context.channel, release_date=release_date, parsed_vars=parsed),
        proposed_title=preview.rendered_title,
    )
    return GenerateResult(
        release_id=context.release_id,
        channel_slug=context.channel_slug,
        current_title=context.current_title,
        has_existing_title=context.has_existing_title,
        used_template={
            "id": int(template["id"]),
            "template_name": str(template["template_name"]),
            "is_default_channel_template": bool(int(template.get("is_default") or 0)),
            "source": source,
        },
        proposed_title=preview.rendered_title,
        normalized_length=len(preview.rendered_title),
        overwrite_required=context.has_existing_title,
        warnings=warnings,
        generation_fingerprint=fingerprint,
    )


def _parse_release_date(value: Any) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _build_generation_fingerprint(
    *,
    release_id: int,
    channel_slug: str,
    template_id: int,
    template_updated_at: Any,
    effective_render_context: Dict[str, Any],
    proposed_title: str,
) -> str:
    payload = {
        "release_id": release_id,
        "channel_slug": channel_slug,
        "template_id": template_id,
        "template_updated_at": template_updated_at,
        "effective_render_context": effective_render_context,
        "proposed_title": proposed_title,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _build_effective_render_context(*, channel: Dict[str, Any], release_date: date | None, parsed_vars: title_template_service.ParseResult) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    used_vars = sorted({token.value for token in parsed_vars.tokens if token.kind == "var"})
    for var in used_vars:
        if var == "channel_display_name":
            values[var] = str(channel.get("display_name") or "")
        elif var == "channel_slug":
            values[var] = str(channel.get("slug") or "")
        elif var == "channel_kind":
            values[var] = str(channel.get("kind") or "")
        elif var == "release_year":
            values[var] = str(release_date.year) if release_date is not None else None
        elif var == "release_month_number":
            values[var] = str(release_date.month) if release_date is not None else None
        elif var == "release_day_number":
            values[var] = str(release_date.day) if release_date is not None else None
    return values


def _can_generate_with_default(*, channel: Dict[str, Any], planned_at: Any, default_template: Dict[str, Any] | None) -> bool:
    if default_template is None:
        return False
    if str(default_template.get("validation_status") or "") != "VALID":
        return False
    body = str(default_template.get("template_body") or "")
    parsed = title_template_service.parse_template(body)
    if parsed.errors:
        return False
    release_date = _parse_release_date(planned_at)
    preview = title_template_service.preview_title_template(channel=channel, template_body=body, release_date=release_date)
    return preview.render_status == "FULL" and not preview.validation_errors and bool(preview.rendered_title)
