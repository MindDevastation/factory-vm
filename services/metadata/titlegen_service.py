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
    release: Dict[str, Any]
    channel: Dict[str, Any]
    default_template: Dict[str, Any] | None
    overwrite_required: bool


@dataclass(frozen=True)
class GenerateResult:
    release: Dict[str, Any]
    template: Dict[str, Any]
    template_source: str
    proposed_title: str
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

    release_row = {
        "id": int(release["id"]),
        "title": str(release["title"] or ""),
        "planned_at": release["planned_at"],
        "channel_slug": str(release["channel_slug"]),
    }
    channel_row = {
        "slug": str(release["channel_slug"]),
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
        (release_row["channel_slug"],),
    ).fetchone()

    current_title = str(release_row["title"]).strip()
    return ContextResult(
        release=release_row,
        channel=channel_row,
        default_template=dict(default_template) if default_template else None,
        overwrite_required=bool(current_title),
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
    if str(template.get("channel_slug") or "") != str(context.release["channel_slug"]):
        raise TitleGenError(code="MTG_TEMPLATE_CHANNEL_MISMATCH", message="Template does not belong to release channel")
    if str(template.get("status") or "") != "ACTIVE":
        raise TitleGenError(code="MTG_TEMPLATE_NOT_ACTIVE", message="Template must be ACTIVE")
    if str(template.get("validation_status") or "") != "VALID":
        raise TitleGenError(code="MTG_TEMPLATE_INVALID", message="Template must be VALID")

    body = str(template.get("template_body") or "")
    parsed = title_template_service.parse_template(body)
    needs_release_date = any(token.kind == "var" and token.value in _DATE_VARIABLES for token in parsed.tokens)
    release_date = _parse_release_date(context.release.get("planned_at"))
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
    if context.overwrite_required:
        warnings.append(
            {
                "code": "MTG_OVERWRITE_REQUIRED",
                "message": "Release already has a non-empty title; apply would overwrite it",
            }
        )

    fingerprint = _build_generation_fingerprint(
        release_id=context.release["id"],
        channel_slug=context.release["channel_slug"],
        planned_at=context.release.get("planned_at"),
        template_id=int(template["id"]),
        template_body=body,
        proposed_title=preview.rendered_title,
    )
    return GenerateResult(
        release=context.release,
        template=template,
        template_source=source,
        proposed_title=preview.rendered_title,
        overwrite_required=context.overwrite_required,
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
    planned_at: Any,
    template_id: int,
    template_body: str,
    proposed_title: str,
) -> str:
    payload = {
        "release_id": release_id,
        "channel_slug": channel_slug,
        "planned_at": planned_at,
        "template_id": template_id,
        "template_body": template_body,
        "proposed_title": proposed_title,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
