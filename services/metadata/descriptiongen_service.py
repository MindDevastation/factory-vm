from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import sqlite3
from typing import Any, Dict, List

from services.metadata import description_template_service

_DATE_VARIABLES = {"release_year", "release_month_number", "release_day_number"}


class DescriptionGenError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ContextResult:
    release_id: int
    channel_slug: str
    current_description: str
    has_existing_description: bool
    default_template: Dict[str, Any] | None
    active_templates: List[Dict[str, Any]]
    can_generate_with_default: bool
    release_row: Dict[str, Any]


@dataclass(frozen=True)
class GenerateResult:
    release_id: int
    channel_slug: str
    current_description: str
    has_existing_description: bool
    used_template: Dict[str, Any]
    proposed_description: str
    normalized_length: int
    line_count: int
    overwrite_required: bool
    warnings: List[str]
    generation_fingerprint: str


def load_descriptiongen_context(conn: sqlite3.Connection, *, release_id: int) -> ContextResult:
    release = conn.execute(
        """
        SELECT r.id, r.title, r.description, r.planned_at, r.created_at, c.slug AS channel_slug
        FROM releases r
        JOIN channels c ON c.id = r.channel_id
        WHERE r.id = ?
        """,
        (release_id,),
    ).fetchone()
    if not release:
        raise DescriptionGenError(code="MTD_RELEASE_NOT_FOUND", message="Release not found")

    release_id_value = int(release["id"])
    channel_slug = str(release["channel_slug"])
    current_description = str(release["description"] or "")

    default_template = conn.execute(
        """
        SELECT *
        FROM description_templates
        WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1
        ORDER BY id DESC
        LIMIT 1
        """,
        (channel_slug,),
    ).fetchone()

    active_template_rows = conn.execute(
        """
        SELECT id, template_name, status, is_default
        FROM description_templates
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

    return ContextResult(
        release_id=release_id_value,
        channel_slug=channel_slug,
        current_description=current_description,
        has_existing_description=bool(current_description.strip()),
        default_template=dict(default_template) if default_template else None,
        active_templates=active_templates,
        can_generate_with_default=default_template is not None,
        release_row=dict(release),
    )


def generate_description_preview(
    conn: sqlite3.Connection,
    *,
    release_id: int,
    template_id: int | None,
) -> GenerateResult:
    context = load_descriptiongen_context(conn, release_id=release_id)

    if template_id is None:
        template = context.default_template
        if template is None:
            raise DescriptionGenError(
                code="MTD_DEFAULT_TEMPLATE_NOT_CONFIGURED",
                message="No active default description template configured for release channel",
            )
    else:
        row = conn.execute("SELECT * FROM description_templates WHERE id = ?", (template_id,)).fetchone()
        if not row:
            raise DescriptionGenError(code="MTD_TEMPLATE_NOT_FOUND", message="Template not found")
        template = dict(row)

    assert template is not None
    if str(template.get("channel_slug") or "") != context.channel_slug:
        raise DescriptionGenError(code="MTD_TEMPLATE_CHANNEL_MISMATCH", message="Template does not belong to release channel")
    if str(template.get("status") or "") != "ACTIVE":
        raise DescriptionGenError(code="MTD_TEMPLATE_NOT_ACTIVE", message="Template must be ACTIVE")
    if str(template.get("validation_status") or "") != "VALID":
        raise DescriptionGenError(code="MTD_TEMPLATE_INVALID", message="Template must be VALID")

    body = str(template.get("template_body") or "")
    parsed = description_template_service.parse_template(body)
    used_vars = {token.value for token in parsed.tokens if token.kind == "var"}

    if "release_title" in used_vars:
        title = str(context.release_row.get("title") or "").strip()
        if not title:
            raise DescriptionGenError(
                code="MTD_RELEASE_TITLE_NOT_USABLE",
                message="release.title is required and must be non-empty when template uses release_title",
            )

    release_date = _parse_release_date(context.release_row.get("planned_at"))
    if used_vars.intersection(_DATE_VARIABLES) and release_date is None:
        raise DescriptionGenError(
            code="MTD_RELEASE_DATE_CONTEXT_MISSING",
            message="Release scheduling datetime is required for template date variables",
        )

    channel = conn.execute(
        "SELECT slug, display_name, kind FROM channels WHERE slug = ?",
        (context.channel_slug,),
    ).fetchone()
    assert channel is not None

    preview = description_template_service.preview_description_template(
        channel=dict(channel),
        template_body=body,
        release_row=context.release_row,
    )

    if preview.missing_variables:
        raise DescriptionGenError(
            code="MTD_REQUIRED_CONTEXT_MISSING",
            message=f"Missing required context for variables: {', '.join(preview.missing_variables)}",
        )

    if preview.render_status != "FULL":
        raise DescriptionGenError(code="MTD_RENDER_FAILED", message="Failed to render proposed description")

    if preview.validation_errors or not preview.rendered_description_preview:
        if any(err.get("code") in {"MTD_RENDER_EMPTY", "MTD_RENDER_TOO_LONG", "MTD_RENDER_CONTROL_CHAR", "MTD_RENDER_TAB_NOT_ALLOWED"} for err in preview.validation_errors):
            raise DescriptionGenError(code="MTD_RENDER_FAILED", message="Rendered description failed validation")
        raise DescriptionGenError(code="MTD_REQUIRED_CONTEXT_MISSING", message="Required render context is missing")

    warnings: List[str] = []
    if context.has_existing_description:
        warnings.append("Applying this result will overwrite the existing release description.")

    rendered = preview.rendered_description_preview
    fingerprint = _build_generation_fingerprint(
        release_id=context.release_id,
        channel_slug=context.channel_slug,
        template_id=int(template["id"]),
        template_updated_at=template.get("updated_at"),
        release_context_version=context.release_row.get("created_at") or context.release_id,
        proposed_description=rendered,
    )

    return GenerateResult(
        release_id=context.release_id,
        channel_slug=context.channel_slug,
        current_description=context.current_description,
        has_existing_description=context.has_existing_description,
        used_template={
            "id": int(template["id"]),
            "template_name": str(template["template_name"]),
            "is_default_channel_template": bool(int(template.get("is_default") or 0)),
        },
        proposed_description=rendered,
        normalized_length=preview.normalized_length,
        line_count=preview.line_count,
        overwrite_required=context.has_existing_description,
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
    release_context_version: Any,
    proposed_description: str,
) -> str:
    payload = {
        "release_id": release_id,
        "channel_slug": channel_slug,
        "template_id": template_id,
        "template_updated_at": template_updated_at,
        "release_context_version": release_context_version,
        "proposed_description": proposed_description,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
