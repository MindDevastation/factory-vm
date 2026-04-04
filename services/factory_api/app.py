from __future__ import annotations

import html
import os
import json
import logging
import re
import secrets
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ConfigDict, Field

from services.common.env import Env
from services.common.disk_guard import classify_write_block, emit_disk_pressure_event, evaluate_disk_pressure_for_env
from services.common.disk_thresholds import DiskPressureLevel
from services.common import db as dbm
from services.common.pydeps import ensure_py_deps_on_sys_path
from services.factory_api.security import require_basic_auth
from services.common.paths import logs_path, outbox_dir, preview_path, qa_path, workspace_dir
from services.factory_api.ui_gdrive import run_preflight_for_job
from services.factory_api.ui_jobs_enqueue import check_ui_render_guard, enqueue_ui_render_job
from services.factory_api.db_viewer import create_db_viewer_router
from services.factory_api.planner import create_planner_router
from services.factory_api.publish_audit_status import create_publish_audit_status_router
from services.factory_api.publish_policy import create_publish_policy_router
from services.factory_api.publish_job_actions import create_publish_job_actions_router
from services.factory_api.publish_bulk_actions import create_publish_bulk_actions_router
from services.factory_api.publish_queue_read import create_publish_queue_read_router
from services.factory_api.publish_reconcile import create_publish_reconcile_router
from services.factory_api.approval_actions import approve_job, reject_job, mark_job_published
from services.factory_api.ux_registry import breadcrumb_context, control_center_entry, primary_nav_items, route_ownership_map
from services.factory_api.page_templates import page_template_contract
from services.factory_api.context_continuity import build_context_envelope, encode_context_token, resolve_incoming_context
from services.factory_api.ux_semantics import action_bar_semantics, filter_control_semantics, inline_message_semantics, readiness_indicator_semantics, severity_indicator_semantics, status_badge_semantics, table_list_semantics
from services.factory_api.ui_state_templates import state_template_catalog
from services.factory_api.interaction_presentation import interaction_presentation_contract_catalog
from services.factory_api.density_responsive import density_responsive_catalog
from services.factory_api.action_taxonomy import action_taxonomy_catalog
from services.factory_api.control_center_contracts import build_control_center_contract_skeleton, default_task_routing_contract
from services.factory_api.problem_readiness_contracts import problem_readiness_contract_catalog, problem_readiness_item_contract
from services.factory_api.problem_readiness_surface import build_grouped_problem_surface
from services.planner.release_job_creation_service import ReleaseJobCreationError, ReleaseJobCreationService
from services.planner import background_assignment_service
from services.planner import cover_assignment_service
from services.planner import visual_batch_service
from services.playlist_builder.api_adapter import (
    PlaylistBuilderValidationError,
    build_channel_settings_payload,
    channel_settings_row_to_patch,
    parse_override_json,
    resolve_playlist_brief,
)
from services.playlist_builder.models import PlaylistBriefOverrides, PlaylistChannelSettingsPatch
from services.playlist_builder.tags import list_builder_tag_options
from services.playlist_builder.workflow import (
    PreviewTimeBudgetExceeded,
    PlaylistBuilderApiError,
    apply_preview,
    build_preview_response,
    create_preview,
    create_preview_for_brief,
    write_committed_history_for_published,
)
from services.ui_jobs import (
    UiJobRetryNotFoundError,
    UiJobRetryStatusError,
    retry_failed_ui_job,
)
from services.track_analysis_report.report_service import (
    ChannelNotFoundError,
    InvalidChannelSlugError,
    TrackAnalysisReportError,
    build_channel_report,
)
from services.track_analysis_report.xlsx_export import export_report_to_xlsx_bytes, sanitize_sheet_name
from services.track_analyzer import track_jobs_db
from services.integrations.gdrive import DriveClient
from services.custom_tags import assignment_service, bulk_bindings_service, bulk_rules_service, catalog_service, reassign_service, rules_service, taxonomy_service
from services.metadata import (
    channel_visual_style_template_service,
    channel_defaults_service,
    description_template_service,
    descriptiongen_service,
    preview_apply_service,
    title_template_service,
    titlegen_service,
    video_tag_preset_service,
    video_tagsgen_service,
)
from services.factory_api.oauth_tokens import (
    build_authorization_url,
    ensure_token_dir,
    exchange_code_for_token_json,
    oauth_token_path,
    redirect_uri,
    sign_state,
    validate_oauth_config,
    verify_state,
)
import yaml


env = Env.load()
ensure_py_deps_on_sys_path(os.environ)
app = FastAPI(title="Factory VM API", version="0.0.1")
logger = logging.getLogger(__name__)
_render_all_channel_slug: ContextVar[Optional[str]] = ContextVar("render_all_channel_slug", default=None)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

templates.env.globals["factory_route_ownership_map"] = route_ownership_map()
templates.env.globals["factory_control_center_entry"] = control_center_entry()
templates.env.globals["factory_primary_nav_items"] = primary_nav_items
templates.env.globals["factory_breadcrumb_context"] = breadcrumb_context
templates.env.globals["factory_page_template_contract"] = page_template_contract

def _request_query_dict(request: Request) -> dict[str, str]:
    return {str(k): str(v) for k, v in request.query_params.multi_items()}


def _context_token_for_request(request: Request) -> str:
    envelope = build_context_envelope(
        current_path=str(request.url.path),
        parent_path=str(request.query_params.get("from") or "").strip() or None,
        raw_query=_request_query_dict(request),
    )
    return encode_context_token(envelope)


def _incoming_context_for_request(request: Request) -> dict[str, Any] | None:
    token = str(request.query_params.get("ctx") or "").strip() or None
    known_paths = set(route_ownership_map().keys())
    envelope = resolve_incoming_context(token=token, known_paths=known_paths)
    if envelope is None:
        return None
    return envelope.as_dict()


templates.env.globals["factory_context_token_for_request"] = _context_token_for_request
templates.env.globals["factory_incoming_context_for_request"] = _incoming_context_for_request


def _semantic_contract_catalog() -> dict[str, Any]:
    return {
        "status_badge": status_badge_semantics(status="DRAFT"),
        "severity_indicator": severity_indicator_semantics(severity="HIGH"),
        "readiness_indicator": readiness_indicator_semantics(readiness="NOT_READY"),
        "inline_message": inline_message_semantics(level="WARNING", text="sample"),
        "action_bar": action_bar_semantics(actions=[{"action": "refresh", "kind": "PRIMARY"}]),
        "filter_controls": filter_control_semantics(filters=["status", "channel", "time_window"]),
        "table_list_pattern": table_list_semantics(variant="TABLE"),
    }


templates.env.globals["factory_semantic_contract_catalog"] = _semantic_contract_catalog
templates.env.globals["factory_state_template_catalog"] = state_template_catalog
templates.env.globals["factory_interaction_presentation_catalog"] = interaction_presentation_contract_catalog
templates.env.globals["factory_density_responsive_catalog"] = density_responsive_catalog
templates.env.globals["factory_action_taxonomy_catalog"] = action_taxonomy_catalog
templates.env.globals["factory_problem_readiness_contract_catalog"] = problem_readiness_contract_catalog

# FastAPI/Starlette TemplateResponse expects (request, name, context, ...).
# Keep compatibility with existing call sites that pass (name, context, ...).
_original_template_response = templates.TemplateResponse


def _template_response_compat(first_arg, *args, **kwargs):
    if isinstance(first_arg, Request):
        return _original_template_response(first_arg, *args, **kwargs)

    name = first_arg
    context = args[0] if args else kwargs.get("context")
    if not isinstance(context, dict):
        raise TypeError("TemplateResponse context must be a dict")

    request = context.get("request")
    if not isinstance(request, Request):
        raise TypeError("TemplateResponse context must include a Request at context['request']")

    remaining_args = args[1:]
    return _original_template_response(request, name, context, *remaining_args, **kwargs)


templates.TemplateResponse = _template_response_compat
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
app.include_router(create_db_viewer_router(env))
app.include_router(create_planner_router(env))
app.include_router(create_publish_audit_status_router(env))
app.include_router(create_publish_policy_router(env))
app.include_router(create_publish_job_actions_router(env))
app.include_router(create_publish_bulk_actions_router(env))
app.include_router(create_publish_queue_read_router(env))
app.include_router(create_publish_reconcile_router(env))


def _create_drive_client(_env: Env) -> DriveClient:
    channel_slug = _render_all_channel_slug.get()
    token_path = _env.gdrive_oauth_token_json
    if channel_slug and _env.gdrive_tokens_dir:
        token_path = str(oauth_token_path(base_dir=_env.gdrive_tokens_dir, channel_slug=channel_slug))
    return DriveClient(
        service_account_json=_env.gdrive_sa_json,
        oauth_client_json=_env.gdrive_oauth_client_json,
        oauth_token_json=token_path,
    )


@app.get("/health")
def health():
    conn = dbm.connect(env)
    try:
        conn.execute("SELECT 1;")
    finally:
        conn.close()
    return {"ok": True, "db": "ok"}


@app.get("/v1/workers")
def api_workers(limit: int = 200, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        rows = dbm.list_workers(conn, limit=limit)
    finally:
        conn.close()
    # parse details_json
    for r in rows:
        try:
            r["details"] = json.loads(r.get("details_json") or "{}")
        except Exception:
            r["details"] = {}
    return {"workers": rows}


@app.get("/v1/channels")
def api_channels(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        rows = conn.execute(
            "SELECT id, slug, display_name FROM channels ORDER BY display_name ASC, slug ASC"
        ).fetchall()
    finally:
        conn.close()
    return rows


@app.post("/v1/releases/{release_id}/jobs/create-or-select")
def api_release_jobs_create_or_select(release_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        svc = ReleaseJobCreationService(conn)
        try:
            result = svc.create_or_select(release_id=release_id)
        except ReleaseJobCreationError as exc:
            status_code = 422
            if exc.code == "PRJ_RELEASE_NOT_FOUND":
                status_code = 404
            elif exc.code == "PRJ_CONCURRENCY_CONFLICT":
                status_code = 409
            elif exc.code == "PRJ_JOB_CREATE_FAILED":
                status_code = 500
            return _prj_error(status_code, exc.code, exc.message, release_id)
    finally:
        conn.close()

    return {
        "release_id": result.release_id,
        "result": result.result,
        "job": result.job,
        "current_open_relation": result.current_open_relation,
        "job_creation_state_summary": result.job_creation_state_summary,
        "open_job_diagnostics": result.open_job_diagnostics,
    }


def _plb_error(status_code: int, code: str, message: str, diagnostics: dict[str, Any] | None = None) -> JSONResponse:
    payload: dict[str, Any] = {"error": {"code": code, "message": message}}
    if diagnostics:
        payload["error"]["diagnostics"] = diagnostics
    return JSONResponse(status_code=status_code, content=payload)


@app.get("/v1/playlist-builder/tags/options")
def api_playlist_builder_tags_options(channel_slug: str | None = None, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        options = list_builder_tag_options(conn, channel_slug=channel_slug)
    finally:
        conn.close()

    custom_count = sum(1 for item in options if item.source == "custom")
    yamnet_count = sum(1 for item in options if item.source == "yamnet")
    semantic_count = sum(1 for item in options if item.source == "semantic")

    reason = "ok"
    if not channel_slug:
        reason = "missing_channel_slug"
    elif not options:
        reason = "no_tags_found_for_channel"

    logger.info(
        "playlist_builder.tags.options",
        extra={
            "channel_slug": channel_slug,
            "custom_count": custom_count,
            "yamnet_count": yamnet_count,
            "semantic_count": semantic_count,
            "final_count": len(options),
            "reason": reason,
        },
    )

    return {
        "options": [
            {
                "source": item.source,
                "value": item.value,
                "label": item.label,
                "group": item.group,
                "count": item.count,
            }
            for item in options
        ],
        "meta": {
            "channel_slug": channel_slug,
            "custom_count": custom_count,
            "yamnet_count": yamnet_count,
            "semantic_count": semantic_count,
            "final_count": len(options),
            "reason": reason,
        },
    }


def _mtb_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _mtg_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _cvst_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _prj_error(status_code: int, code: str, message: str, release_id: int) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "release_id": release_id,
            "result": "FAILED",
            "error": {"code": code, "message": message},
        },
    )


def _vbg_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _vcover_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _vbatch_error(status_code: int, code: str, message: str, details: Dict[str, Any] | None = None) -> JSONResponse:
    err: Dict[str, Any] = {"code": code, "message": message}
    if details:
        err["details"] = details
    return JSONResponse(status_code=status_code, content={"error": err})


class MetadataTitleTemplatePreviewRequest(BaseModel):
    channel_slug: str = Field(min_length=1)
    template_body: str
    release_date: str | None = None


class MetadataTitleTemplateCreateRequest(BaseModel):
    channel_slug: str = Field(min_length=1)
    template_name: str
    template_body: str
    make_default: bool = False


class MetadataTitleTemplatePatchRequest(BaseModel):
    template_name: str | None = None
    template_body: str | None = None


class MetadataDescriptionTemplatePreviewRequest(BaseModel):
    channel_slug: str = Field(min_length=1)
    template_body: str
    release_id: int | None = None


class MetadataVideoTagPresetPreviewRequest(BaseModel):
    channel_slug: str = Field(min_length=1)
    preset_body: list[str]
    release_id: int | None = None


class MetadataVideoTagPresetCreateRequest(BaseModel):
    channel_slug: str = Field(min_length=1)
    preset_name: str
    preset_body: list[str]
    make_default: bool = False


class MetadataVideoTagPresetPatchRequest(BaseModel):
    preset_name: str | None = None
    preset_body: list[str] | None = None


class ChannelVisualStyleTemplateCreateRequest(BaseModel):
    channel_slug: str = Field(min_length=1)
    template_name: str
    template_payload: Dict[str, Any]
    make_default: bool = False


class ChannelVisualStyleTemplatePatchRequest(BaseModel):
    template_name: str | None = None
    template_payload: Dict[str, Any] | None = None


class ChannelVisualStyleTemplateReleaseOverrideRequest(BaseModel):
    template_id: int


class MetadataDescriptionTemplateCreateRequest(BaseModel):
    channel_slug: str = Field(min_length=1)
    template_name: str
    template_body: str
    make_default: bool = False


class MetadataDescriptionTemplatePatchRequest(BaseModel):
    template_name: str | None = None
    template_body: str | None = None


class MetadataTitleGenGenerateRequest(BaseModel):
    template_id: int | None = None


class MetadataDescriptionGenGenerateRequest(BaseModel):
    template_id: int | None = None


class MetadataVideoTagsGenGenerateRequest(BaseModel):
    preset_id: int | None = None


class MetadataVideoTagsGenApplyRequest(BaseModel):
    preset_id: int | None = None
    generation_fingerprint: str = Field(min_length=1)
    overwrite_confirmed: bool = False


class MetadataTitleGenApplyRequest(BaseModel):
    template_id: int | None = None
    generation_fingerprint: str = Field(min_length=1)
    overwrite_confirmed: bool = False


class MetadataDescriptionGenApplyRequest(BaseModel):
    template_id: int | None = None
    generation_fingerprint: str = Field(min_length=1)
    overwrite_confirmed: bool = False


class MetadataPreviewApplySourcesRequest(BaseModel):
    title_template_id: int | None = None
    description_template_id: int | None = None
    video_tag_preset_id: int | None = None


class MetadataPreviewApplyPreviewRequest(BaseModel):
    fields: list[str] | None = None
    sources: MetadataPreviewApplySourcesRequest = Field(default_factory=MetadataPreviewApplySourcesRequest)


class MetadataPreviewApplyApplyRequest(BaseModel):
    selected_fields: list[str]
    overwrite_confirmed_fields: list[str] = Field(default_factory=list)


class MetadataChannelDefaultsUpdateRequest(BaseModel):
    default_title_template_id: int | None = None
    default_description_template_id: int | None = None
    default_video_tag_preset_id: int | None = None


class BackgroundPreviewRequest(BaseModel):
    background_asset_id: int | None = None
    source_family: str | None = None
    source_reference: str | None = None
    template_assisted: bool = False


class BackgroundApproveRequest(BaseModel):
    preview_id: str = Field(min_length=1)


class CoverInputPayloadRequest(BaseModel):
    provider_family: str = Field(min_length=1)
    input_payload: Dict[str, Any] = Field(default_factory=dict)
    template_ref: Dict[str, Any] | None = None


class CoverCandidateCreateRequest(BaseModel):
    cover_asset_id: int
    source_provider_family: str = Field(min_length=1)
    source_reference: str | None = None
    input_payload_id: int | None = None
    selection_mode: str = "manual"
    template_ref: Dict[str, Any] | None = None


class CoverCandidateSelectRequest(BaseModel):
    candidate_id: str = Field(min_length=1)


class CoverApproveRequest(BaseModel):
    candidate_id: str | None = None


class VisualApplyRequest(BaseModel):
    reuse_override_confirmed: bool = False
    stale_token: str = Field(min_length=1)
    conflict_token: str = Field(min_length=1)


class VisualBatchPreviewRequest(BaseModel):
    action_type: str = Field(min_length=1)
    selected_release_ids: list[int] = Field(min_length=1)
    action_payload: Dict[str, Any] = Field(default_factory=dict)


class VisualBatchExecuteRequest(BaseModel):
    preview_session_id: str = Field(min_length=1)
    selected_release_ids: list[int] = Field(min_length=1)
    overwrite_confirmed: bool = False
    reuse_override_confirmed: bool = False


def _mdo_sources_from_defaults(defaults: Dict[str, Any]) -> List[Dict[str, Any]]:
    field_pairs = (
        ("title", "title_template"),
        ("description", "description_template"),
        ("tags", "video_tag_preset"),
    )
    refs: List[Dict[str, Any]] = []
    for field_name, source_type in field_pairs:
        source = defaults.get(source_type)
        refs.append(
            {
                "field_name": field_name,
                "source_type": source_type,
                "source_id": int(source["id"]) if source else None,
            }
        )
    return refs


@app.get("/v1/metadata/releases/{release_id}/titlegen/context")
def api_metadata_titlegen_context(release_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            context = titlegen_service.load_titlegen_context(conn, release_id=release_id)
        except titlegen_service.TitleGenError as exc:
            status_code = 404 if exc.code == "MTG_RELEASE_NOT_FOUND" else 422
            logger.info(
                "metadata.titlegen.context_failed",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "template_id": None,
                    "overwrite_required": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtg_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    default_item = None
    if context.default_template is not None:
        default_item = {
            "id": int(context.default_template["id"]),
            "template_name": str(context.default_template["template_name"]),
            "status": str(context.default_template.get("status") or ""),
            "is_default": bool(int(context.default_template.get("is_default") or 0)),
        }

    payload = {
        "release_id": context.release_id,
        "channel_slug": context.channel_slug,
        "current_title": context.current_title,
        "has_existing_title": context.has_existing_title,
        "default_template": default_item,
        "active_templates": context.active_templates,
        "can_generate_with_default": context.can_generate_with_default,
    }
    logger.info(
        "metadata.titlegen.context_loaded",
        extra={
            "release_id": context.release_id,
            "channel_slug": context.channel_slug,
            "template_id": default_item["id"] if default_item else None,
            "overwrite_required": context.has_existing_title,
            "result_status": "ok",
            "error_codes": [],
        },
    )
    return payload


@app.get("/v1/metadata/releases/{release_id}/descriptiongen/context")
def api_metadata_descriptiongen_context(release_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            context = descriptiongen_service.load_descriptiongen_context(conn, release_id=release_id)
        except descriptiongen_service.DescriptionGenError as exc:
            status_code = 404 if exc.code == "MTD_RELEASE_NOT_FOUND" else 422
            logger.info(
                "metadata.descriptiongen.context_loaded",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "template_id": None,
                    "overwrite_required": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtb_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    default_item = None
    if context.default_template is not None:
        default_item = {
            "id": int(context.default_template["id"]),
            "template_name": str(context.default_template["template_name"]),
            "status": str(context.default_template.get("status") or ""),
            "is_default": bool(int(context.default_template.get("is_default") or 0)),
        }

    payload = {
        "release_id": context.release_id,
        "channel_slug": context.channel_slug,
        "current_description": context.current_description,
        "has_existing_description": context.has_existing_description,
        "default_template": default_item,
        "active_templates": context.active_templates,
        "can_generate_with_default": context.can_generate_with_default,
    }
    logger.info(
        "metadata.descriptiongen.context_loaded",
        extra={
            "release_id": context.release_id,
            "channel_slug": context.channel_slug,
            "template_id": default_item["id"] if default_item else None,
            "overwrite_required": context.has_existing_description,
            "result_status": "ok",
            "error_codes": [],
        },
    )
    return payload


@app.get("/v1/metadata/releases/{release_id}/video-tags/context")
def api_metadata_video_tags_context(release_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            context = video_tagsgen_service.load_video_tags_context(conn, release_id=release_id)
        except video_tagsgen_service.VideoTagsGenError as exc:
            status_code = 404 if exc.code == "MTV_RELEASE_NOT_FOUND" else 422
            logger.info(
                "metadata.video_tags.generate_failed",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "preset_id": None,
                    "overwrite_required": None,
                    "dropped_empty_count": None,
                    "removed_duplicates_count": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    default_item = None
    if context.default_preset is not None:
        default_item = {
            "id": int(context.default_preset["id"]),
            "preset_name": str(context.default_preset["preset_name"]),
            "status": str(context.default_preset.get("status") or ""),
            "is_default": bool(int(context.default_preset.get("is_default") or 0)),
        }
    return {
        "release_id": context.release_id,
        "channel_slug": context.channel_slug,
        "current_tags_json": context.current_tags_json,
        "has_existing_tags": context.has_existing_tags,
        "default_preset": default_item,
        "active_presets": context.active_presets,
        "can_generate_with_default": context.can_generate_with_default,
    }


@app.get("/v1/metadata/releases/{release_id}/preview-apply/context")
def api_metadata_preview_apply_context(release_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            context = preview_apply_service.load_preview_apply_context(conn, release_id=release_id)
        except preview_apply_service.MetadataPreviewApplyError as exc:
            logger.info(
                "metadata.preview_apply.context_loaded",
                extra={
                    "session_id": None,
                    "release_id": release_id,
                    "channel_slug": None,
                    "requested_fields": [],
                    "prepared_fields": [],
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            status_code = 404 if exc.code == "MPA_RELEASE_NOT_FOUND" else 422
            return _mpa_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    logger.info(
        "metadata.preview_apply.context_loaded",
        extra={
            "session_id": None,
            "release_id": context.release_id,
            "channel_slug": context.channel_slug,
            "requested_fields": [],
            "prepared_fields": [],
            "result_status": "ok",
            "error_codes": [],
        },
    )
    return {
        "release_id": context.release_id,
        "channel_slug": context.channel_slug,
        "current": context.current,
        "defaults": context.defaults,
        "active_sources": context.active_sources,
    }


@app.post("/v1/metadata/releases/{release_id}/preview-apply/preview")
def api_metadata_preview_apply_preview(
    release_id: int,
    payload: MetadataPreviewApplyPreviewRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = preview_apply_service.create_preview_session(
                conn,
                release_id=release_id,
                requested_fields=payload.fields,
                sources=payload.sources.model_dump(),
                created_by=env.basic_user,
                ttl_seconds=env.metadata_preview_ttl_sec,
            )
        except preview_apply_service.MetadataPreviewApplyError as exc:
            logger.info(
                "metadata.preview_apply.preview_failed",
                extra={
                    "session_id": None,
                    "release_id": release_id,
                    "channel_slug": None,
                    "requested_fields": payload.fields or ["title", "description", "tags"],
                    "prepared_fields": [],
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            status_code = 404 if exc.code == "MPA_RELEASE_NOT_FOUND" else 422
            return _mpa_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    logger.info(
        "metadata.preview_apply.preview_created",
        extra={
            "session_id": body["session_id"],
            "release_id": body["release_id"],
            "channel_slug": body["channel_slug"],
            "requested_fields": body["summary"]["requested_fields"],
            "prepared_fields": body["summary"]["prepared_fields"],
            "result_status": "ok",
            "error_codes": [],
        },
    )
    return body


@app.get("/v1/metadata/channels/{channel_slug}/defaults")
def api_metadata_channel_defaults_read(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            payload = channel_defaults_service.read_channel_defaults(conn, channel_slug=channel_slug)
        except channel_defaults_service.MetadataDefaultsError as exc:
            status_code = 404 if exc.code == "MDO_CHANNEL_NOT_FOUND" else 422
            logger.info(
                "metadata.defaults.read",
                extra={
                    "channel_slug": channel_slug,
                    "field_name": None,
                    "source_type": None,
                    "source_id": None,
                    "source_refs": [],
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mdo_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.defaults.read",
        extra={
            "channel_slug": channel_slug,
            "field_name": "multiple",
            "source_type": "multiple",
            "source_id": None,
            "source_refs": _mdo_sources_from_defaults(payload["defaults"]),
            "result_status": "success",
            "error_codes": [],
        },
    )
    return payload


@app.put("/v1/metadata/channels/{channel_slug}/defaults")
def api_metadata_channel_defaults_update(
    channel_slug: str,
    payload: MetadataChannelDefaultsUpdateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    previous_defaults: dict[str, Any] | None = None
    request_sources = [
        {"field_name": "title", "source_type": "title_template", "source_id": payload.default_title_template_id},
        {"field_name": "description", "source_type": "description_template", "source_id": payload.default_description_template_id},
        {"field_name": "tags", "source_type": "video_tag_preset", "source_id": payload.default_video_tag_preset_id},
    ]
    try:
        try:
            previous_defaults = channel_defaults_service.read_channel_defaults(conn, channel_slug=channel_slug)["defaults"]
            result = channel_defaults_service.update_channel_defaults(
                conn,
                channel_slug=channel_slug,
                default_title_template_id=payload.default_title_template_id,
                default_description_template_id=payload.default_description_template_id,
                default_video_tag_preset_id=payload.default_video_tag_preset_id,
            )
        except channel_defaults_service.MetadataDefaultsError as exc:
            status_code = 404 if exc.code in {"MDO_CHANNEL_NOT_FOUND", "MDO_DEFAULT_SOURCE_NOT_FOUND"} else 422
            logger.info(
                "metadata.defaults.updated",
                extra={
                    "channel_slug": channel_slug,
                    "field_name": exc.field_name,
                    "source_type": exc.source_type,
                    "source_id": exc.source_id,
                    "source_refs": request_sources,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mdo_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    if previous_defaults is not None:
        cleared_fields: list[dict[str, Any]] = []
        for field_name, source_type, response_key in [
            ("title", "title_template", "title_template"),
            ("description", "description_template", "description_template"),
            ("tags", "video_tag_preset", "video_tag_preset"),
        ]:
            previous_source = previous_defaults.get(response_key)
            current_source = result["defaults"].get(response_key)
            if previous_source is not None and current_source is None:
                cleared_fields.append(
                    {
                        "field_name": field_name,
                        "source_type": source_type,
                        "source_id": previous_source.get("id"),
                    }
                )
        for cleared in cleared_fields:
            logger.info(
                "metadata.defaults.cleared",
                extra={
                    "channel_slug": channel_slug,
                    "field_name": cleared["field_name"],
                    "source_type": cleared["source_type"],
                    "source_id": cleared["source_id"],
                    "source_refs": _mdo_sources_from_defaults(result["defaults"]),
                    "result_status": "success",
                    "error_codes": [],
                },
            )

    logger.info(
        "metadata.defaults.updated",
        extra={
            "channel_slug": channel_slug,
            "field_name": "multiple",
            "source_type": "multiple",
            "source_id": None,
            "source_refs": _mdo_sources_from_defaults(result["defaults"]),
            "result_status": "success",
            "error_codes": [],
        },
    )
    return result


@app.get("/v1/metadata/preview-apply/sessions/{session_id}")
def api_metadata_preview_apply_session(session_id: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    session_row = conn.execute("SELECT release_id, channel_slug FROM metadata_preview_sessions WHERE id = ?", (session_id,)).fetchone()
    release_id_hint = int(session_row["release_id"]) if session_row else None
    channel_slug_hint = str(session_row["channel_slug"]) if session_row else None
    try:
        try:
            body = preview_apply_service.get_preview_session(conn, session_id=session_id)
        except preview_apply_service.MetadataPreviewApplyError as exc:
            event = "metadata.preview_apply.apply_failed"
            if exc.code == "MPA_SESSION_EXPIRED":
                event = "metadata.preview_apply.session_expired"
            logger.info(
                event,
                extra={
                    "session_id": session_id,
                    "release_id": release_id_hint,
                    "channel_slug": channel_slug_hint,
                    "selected_apply_fields": [],
                    "overwrite_confirmed_fields": [],
                    "stale_fields": list(exc.details.get("stale_fields") or []),
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            status_code = 404 if exc.code in {"MPA_SESSION_NOT_FOUND", "MPA_RELEASE_NOT_FOUND"} else 422
            return _mpa_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    if body["session_status"] == "EXPIRED":
        logger.info(
            "metadata.preview_apply.session_expired",
            extra={
                "session_id": body["session_id"],
                "release_id": body["release_id"],
                "channel_slug": body["channel_slug"],
                "selected_apply_fields": [],
                "overwrite_confirmed_fields": [],
                "stale_fields": [],
                "result_status": "ok",
                "error_codes": [],
            },
        )
    stale_fields = [field for field, rec in body["fields"].items() if rec.get("status") == "STALE"]
    if stale_fields:
        logger.info(
            "metadata.preview_apply.stale_detected",
            extra={
                "session_id": body["session_id"],
                "release_id": body["release_id"],
                "channel_slug": body["channel_slug"],
                "selected_apply_fields": [],
                "overwrite_confirmed_fields": [],
                "stale_fields": stale_fields,
                "result_status": "ok",
                "error_codes": [],
            },
        )
    return body


@app.post("/v1/metadata/preview-apply/sessions/{session_id}/apply")
def api_metadata_preview_apply_apply(
    session_id: str,
    payload: MetadataPreviewApplyApplyRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    session_row = conn.execute("SELECT release_id, channel_slug FROM metadata_preview_sessions WHERE id = ?", (session_id,)).fetchone()
    release_id_hint = int(session_row["release_id"]) if session_row else None
    channel_slug_hint = str(session_row["channel_slug"]) if session_row else None
    try:
        try:
            body = preview_apply_service.apply_preview_session(
                conn,
                session_id=session_id,
                selected_fields=payload.selected_fields,
                overwrite_confirmed_fields=payload.overwrite_confirmed_fields,
            )
        except preview_apply_service.MetadataPreviewApplyError as exc:
            event = "metadata.preview_apply.apply_failed"
            if exc.code == "MPA_SESSION_EXPIRED":
                event = "metadata.preview_apply.session_expired"
            if exc.code == "MPA_PREVIEW_STALE":
                event = "metadata.preview_apply.stale_detected"
            logger.info(
                event,
                extra={
                    "session_id": session_id,
                    "release_id": release_id_hint,
                    "channel_slug": channel_slug_hint,
                    "selected_apply_fields": payload.selected_fields,
                    "overwrite_confirmed_fields": payload.overwrite_confirmed_fields,
                    "stale_fields": list(exc.details.get("stale_fields") or []),
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            status_code = 404 if exc.code in {"MPA_SESSION_NOT_FOUND", "MPA_RELEASE_NOT_FOUND"} else 422
            return _mpa_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    logger.info(
        "metadata.preview_apply.applied",
        extra={
            "session_id": body["session_id"],
            "release_id": body["release_id"],
            "channel_slug": body["channel_slug"],
            "selected_apply_fields": payload.selected_fields,
            "overwrite_confirmed_fields": payload.overwrite_confirmed_fields,
            "stale_fields": body.get("stale_fields", []),
            "result_status": body["result"],
            "error_codes": [],
        },
    )
    return body


@app.get("/v1/visual/releases/{release_id}/background/candidates")
def api_visual_background_candidates(
    release_id: int,
    template_assisted: bool = False,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = background_assignment_service.list_background_candidates(
                conn,
                release_id=release_id,
                template_assisted=template_assisted,
            )
        except background_assignment_service.BackgroundAssignmentError as exc:
            status_code = 404 if exc.code == "VBG_RELEASE_NOT_FOUND" else 422
            return _vbg_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/releases/{release_id}/background/preview")
def api_visual_background_preview(
    release_id: int,
    payload: BackgroundPreviewRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = background_assignment_service.preview_background_assignment(
                conn,
                release_id=release_id,
                background_asset_id=payload.background_asset_id,
                source_family=payload.source_family,
                source_reference=payload.source_reference,
                template_assisted=payload.template_assisted,
                selected_by=env.basic_user,
            )
        except background_assignment_service.BackgroundAssignmentError as exc:
            status_code = 404 if exc.code in {"VBG_RELEASE_NOT_FOUND", "VBG_PREVIEW_NOT_FOUND"} else 422
            return _vbg_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/releases/{release_id}/background/approve")
def api_visual_background_approve(
    release_id: int,
    payload: BackgroundApproveRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = background_assignment_service.approve_background_assignment(
                conn,
                release_id=release_id,
                preview_id=payload.preview_id,
                approved_by=env.basic_user,
            )
        except background_assignment_service.BackgroundAssignmentError as exc:
            status_code = 404 if exc.code in {"VBG_RELEASE_NOT_FOUND", "VBG_PREVIEW_NOT_FOUND"} else 422
            return _vbg_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/releases/{release_id}/background/apply")
def api_visual_background_apply(
    release_id: int,
    payload: VisualApplyRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = background_assignment_service.apply_background_assignment(
                conn,
                release_id=release_id,
                applied_by=env.basic_user,
                reuse_override_confirmed=bool(payload.reuse_override_confirmed),
                stale_token=payload.stale_token,
                conflict_token=payload.conflict_token,
            )
        except background_assignment_service.BackgroundAssignmentError as exc:
            status_code = 404 if exc.code in {"VBG_RELEASE_NOT_FOUND", "VBG_PREVIEW_NOT_FOUND"} else 422
            return _vbg_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/releases/{release_id}/cover/input-payload")
def api_visual_cover_input_payload_create(
    release_id: int,
    payload: CoverInputPayloadRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = cover_assignment_service.create_cover_selection_input(
                conn,
                release_id=release_id,
                provider_family=payload.provider_family,
                input_payload=dict(payload.input_payload),
                template_ref=dict(payload.template_ref) if payload.template_ref is not None else None,
                created_by=env.basic_user,
            )
        except cover_assignment_service.CoverAssignmentError as exc:
            status_code = 404 if exc.code == "VCOVER_RELEASE_NOT_FOUND" else 422
            return _vcover_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/releases/{release_id}/cover/candidates")
def api_visual_cover_candidate_create(
    release_id: int,
    payload: CoverCandidateCreateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = cover_assignment_service.create_cover_candidate_reference(
                conn,
                release_id=release_id,
                cover_asset_id=payload.cover_asset_id,
                source_provider_family=payload.source_provider_family,
                source_reference=payload.source_reference,
                input_payload_id=payload.input_payload_id,
                selection_mode=payload.selection_mode,
                template_ref=dict(payload.template_ref) if payload.template_ref is not None else None,
                created_by=env.basic_user,
            )
        except cover_assignment_service.CoverAssignmentError as exc:
            status_code = 404 if exc.code in {"VCOVER_RELEASE_NOT_FOUND", "VCOVER_ASSET_NOT_FOUND", "VCOVER_CANDIDATE_NOT_FOUND"} else 422
            return _vcover_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.get("/v1/visual/releases/{release_id}/cover/candidates")
def api_visual_cover_candidates_list(
    release_id: int,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = cover_assignment_service.list_cover_candidates(conn, release_id=release_id)
        except cover_assignment_service.CoverAssignmentError as exc:
            status_code = 404 if exc.code == "VCOVER_RELEASE_NOT_FOUND" else 422
            return _vcover_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.get("/v1/visual/releases/{release_id}/cover/candidates/{candidate_id}/preview")
def api_visual_cover_candidate_preview(
    release_id: int,
    candidate_id: str,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = cover_assignment_service.preview_cover_candidate(conn, release_id=release_id, candidate_id=candidate_id)
        except cover_assignment_service.CoverAssignmentError as exc:
            status_code = 404 if exc.code in {"VCOVER_RELEASE_NOT_FOUND", "VCOVER_CANDIDATE_NOT_FOUND"} else 422
            return _vcover_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/releases/{release_id}/cover/select")
def api_visual_cover_candidate_select(
    release_id: int,
    payload: CoverCandidateSelectRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = cover_assignment_service.select_cover_candidate_for_approval(
                conn,
                release_id=release_id,
                candidate_id=payload.candidate_id,
                selected_by=env.basic_user,
            )
        except cover_assignment_service.CoverAssignmentError as exc:
            status_code = 404 if exc.code in {"VCOVER_RELEASE_NOT_FOUND", "VCOVER_CANDIDATE_NOT_FOUND"} else 422
            return _vcover_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/releases/{release_id}/cover/approve")
def api_visual_cover_candidate_approve(
    release_id: int,
    payload: CoverApproveRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = cover_assignment_service.approve_cover_candidate(
                conn,
                release_id=release_id,
                candidate_id=payload.candidate_id,
                approved_by=env.basic_user,
            )
        except cover_assignment_service.CoverAssignmentError as exc:
            status_code = 404 if exc.code in {"VCOVER_RELEASE_NOT_FOUND", "VCOVER_CANDIDATE_NOT_FOUND", "VCOVER_PREVIEW_NOT_FOUND"} else 422
            return _vcover_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/releases/{release_id}/cover/apply")
def api_visual_cover_candidate_apply(
    release_id: int,
    payload: VisualApplyRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = cover_assignment_service.apply_cover_candidate(
                conn,
                release_id=release_id,
                applied_by=env.basic_user,
                reuse_override_confirmed=bool(payload.reuse_override_confirmed),
                stale_token=payload.stale_token,
                conflict_token=payload.conflict_token,
            )
        except cover_assignment_service.CoverAssignmentError as exc:
            status_code = 404 if exc.code in {"VCOVER_RELEASE_NOT_FOUND", "VCOVER_PREVIEW_NOT_FOUND"} else 422
            return _vcover_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return body


@app.get("/v1/visual/releases/{release_id}/history")
def api_visual_release_history(
    release_id: int,
    limit: int = 20,
    _: bool = Depends(require_basic_auth(env)),
):
    limit_clamped = max(1, min(int(limit), 100))
    conn = dbm.connect(env)
    try:
        release = conn.execute("SELECT id FROM releases WHERE id = ?", (release_id,)).fetchone()
        if not release:
            return _vcover_error(404, "VVIS_RELEASE_NOT_FOUND", "visual release not found")
        rows = conn.execute(
            """
            SELECT id, preview_scope, history_stage, preview_id, background_asset_id, cover_asset_id, decision_mode, reuse_warning_json, actor, created_at
            FROM release_visual_history_events
            WHERE release_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (release_id, limit_clamped),
        ).fetchall()
    finally:
        conn.close()
    return {"release_id": release_id, "items": [dict(row) for row in rows]}


@app.post("/v1/visual/batch/preview")
def api_visual_batch_preview(
    payload: VisualBatchPreviewRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = visual_batch_service.create_visual_batch_preview_session(
                conn,
                action_type=payload.action_type,
                selected_release_ids=list(payload.selected_release_ids),
                created_by=env.basic_user,
                action_payload=dict(payload.action_payload),
            )
            conn.commit()
        except visual_batch_service.VisualBatchError as exc:
            conn.rollback()
            status_code = 404 if exc.code == "VBATCH_RELEASES_NOT_FOUND" else 422
            return _vbatch_error(status_code, exc.code, exc.message, exc.details)
    finally:
        conn.close()
    return body


@app.post("/v1/visual/batch/execute")
def api_visual_batch_execute(
    payload: VisualBatchExecuteRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            body = visual_batch_service.execute_visual_batch_preview_session(
                conn,
                preview_session_id=payload.preview_session_id,
                selected_release_ids=list(payload.selected_release_ids),
                overwrite_confirmed=bool(payload.overwrite_confirmed),
                reuse_override_confirmed=bool(payload.reuse_override_confirmed),
                executed_by=env.basic_user,
            )
            conn.commit()
        except visual_batch_service.VisualBatchError as exc:
            conn.rollback()
            status_code = 404 if exc.code == "VBATCH_SESSION_NOT_FOUND" else 409 if exc.code.startswith("VBATCH_PREVIEW_") else 422
            return _vbatch_error(status_code, exc.code, exc.message, exc.details)
    finally:
        conn.close()
    return body


@app.post("/v1/metadata/releases/{release_id}/video-tags/generate")
def api_metadata_video_tags_generate(
    release_id: int,
    payload: MetadataVideoTagsGenGenerateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = video_tagsgen_service.generate_video_tags_preview(
                conn,
                release_id=release_id,
                preset_id=payload.preset_id,
            )
        except video_tagsgen_service.VideoTagsGenError as exc:
            status_code = 404 if exc.code in {"MTV_RELEASE_NOT_FOUND", "MTV_PRESET_NOT_FOUND"} else 422
            logger.info(
                "metadata.video_tags.generate_failed",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "preset_id": payload.preset_id,
                    "overwrite_required": None,
                    "dropped_empty_count": None,
                    "removed_duplicates_count": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    body = {
        "release_id": result.release_id,
        "used_preset": {
            "id": int(result.used_preset["id"]),
            "preset_name": str(result.used_preset["preset_name"]),
            "is_default_channel_preset": bool(result.used_preset["is_default_channel_preset"]),
        },
        "current_tags_json": result.current_tags_json,
        "has_existing_tags": result.has_existing_tags,
        "overwrite_required": result.overwrite_required,
        "rendered_items_before_normalization": result.rendered_items_before_normalization,
        "dropped_empty_items": result.dropped_empty_items,
        "removed_duplicates": result.removed_duplicates,
        "proposed_tags_json": result.proposed_tags_json,
        "normalized_count": result.normalized_count,
        "generation_fingerprint": result.generation_fingerprint,
        "warnings": result.warnings,
    }
    logger.info(
        "metadata.video_tags.generated",
        extra={
            "release_id": result.release_id,
            "channel_slug": result.channel_slug,
            "preset_id": int(result.used_preset["id"]),
            "overwrite_required": result.overwrite_required,
            "dropped_empty_count": len(result.dropped_empty_items),
            "removed_duplicates_count": len(result.removed_duplicates),
            "result_status": "ok",
            "error_codes": [],
        },
    )
    return body


@app.post("/v1/metadata/releases/{release_id}/video-tags/apply")
def api_metadata_video_tags_apply(
    release_id: int,
    payload: MetadataVideoTagsGenApplyRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = video_tagsgen_service.apply_generated_video_tags(
                conn,
                release_id=release_id,
                preset_id=payload.preset_id,
                generation_fingerprint=payload.generation_fingerprint,
                overwrite_confirmed=payload.overwrite_confirmed,
            )
        except video_tagsgen_service.VideoTagsGenError as exc:
            status_code = 404 if exc.code in {"MTV_RELEASE_NOT_FOUND", "MTV_PRESET_NOT_FOUND"} else 422
            logger.info(
                "metadata.video_tags.apply_failed",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "preset_id": payload.preset_id,
                    "overwrite_required": None,
                    "dropped_empty_count": None,
                    "removed_duplicates_count": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    body = {
        "release_id": result.release_id,
        "tags_updated": result.tags_updated,
        "tags_before": result.tags_before,
        "tags_after": result.tags_after,
        "used_preset_id": result.used_preset_id,
    }
    if result.message is not None:
        body["message"] = result.message
    logger.info(
        "metadata.video_tags.applied",
        extra={
            "release_id": result.release_id,
            "channel_slug": result.channel_slug,
            "preset_id": result.used_preset_id,
            "overwrite_required": result.overwrite_required,
            "dropped_empty_count": None,
            "removed_duplicates_count": None,
            "result_status": "no_op" if not result.tags_updated else "ok",
            "error_codes": [],
        },
    )
    return body


@app.post("/v1/metadata/releases/{release_id}/descriptiongen/generate")
def api_metadata_descriptiongen_generate(
    release_id: int,
    payload: MetadataDescriptionGenGenerateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = descriptiongen_service.generate_description_preview(
                conn,
                release_id=release_id,
                template_id=payload.template_id,
            )
        except descriptiongen_service.DescriptionGenError as exc:
            status_code = 404 if exc.code in {"MTD_RELEASE_NOT_FOUND", "MTD_TEMPLATE_NOT_FOUND"} else 422
            logger.info(
                "metadata.descriptiongen.generate_failed",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "template_id": payload.template_id,
                    "overwrite_required": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtb_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    body = {
        "release_id": result.release_id,
        "used_template": {
            "id": int(result.used_template["id"]),
            "template_name": str(result.used_template["template_name"]),
            "is_default_channel_template": bool(result.used_template["is_default_channel_template"]),
        },
        "current_description": result.current_description,
        "has_existing_description": result.has_existing_description,
        "overwrite_required": result.overwrite_required,
        "proposed_description": result.proposed_description,
        "normalized_length": result.normalized_length,
        "line_count": result.line_count,
        "generation_fingerprint": result.generation_fingerprint,
        "warnings": result.warnings,
    }
    logger.info(
        "metadata.descriptiongen.generated",
        extra={
            "release_id": result.release_id,
            "channel_slug": result.channel_slug,
            "template_id": int(result.used_template["id"]),
            "overwrite_required": result.overwrite_required,
            "result_status": "ok",
            "error_codes": [],
        },
    )
    return body


@app.post("/v1/metadata/releases/{release_id}/titlegen/generate")
def api_metadata_titlegen_generate(
    release_id: int,
    payload: MetadataTitleGenGenerateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=payload.template_id)
        except titlegen_service.TitleGenError as exc:
            status_code = 404 if exc.code in {"MTG_RELEASE_NOT_FOUND", "MTG_TEMPLATE_NOT_FOUND"} else 422
            logger.info(
                "metadata.titlegen.generate_failed",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "template_id": payload.template_id,
                    "overwrite_required": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtg_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    body = {
        "release_id": result.release_id,
        "used_template": {
            "id": int(result.used_template["id"]),
            "template_name": str(result.used_template["template_name"]),
            "is_default_channel_template": bool(result.used_template["is_default_channel_template"]),
        },
        "current_title": result.current_title,
        "has_existing_title": result.has_existing_title,
        "overwrite_required": result.overwrite_required,
        "proposed_title": result.proposed_title,
        "normalized_length": result.normalized_length,
        "generation_fingerprint": result.generation_fingerprint,
        "warnings": result.warnings,
    }
    logger.info(
        "metadata.titlegen.generated",
        extra={
            "release_id": result.release_id,
            "channel_slug": result.channel_slug,
            "template_id": int(result.used_template["id"]),
            "overwrite_required": result.overwrite_required,
            "result_status": "ok",
            "error_codes": [],
        },
    )
    return body


@app.post("/v1/metadata/releases/{release_id}/titlegen/apply")
def api_metadata_titlegen_apply(
    release_id: int,
    payload: MetadataTitleGenApplyRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = titlegen_service.apply_generated_title(
                conn,
                release_id=release_id,
                template_id=payload.template_id,
                generation_fingerprint=payload.generation_fingerprint,
                overwrite_confirmed=payload.overwrite_confirmed,
            )
        except titlegen_service.TitleGenError as exc:
            status_code = 404 if exc.code in {"MTG_RELEASE_NOT_FOUND", "MTG_TEMPLATE_NOT_FOUND"} else 422
            logger.info(
                "metadata.titlegen.apply_failed",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "template_id": payload.template_id,
                    "overwrite_required": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtg_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    body = {
        "release_id": result.release_id,
        "title_updated": result.title_updated,
        "title_before": result.title_before,
        "title_after": result.title_after,
        "used_template_id": result.used_template_id,
    }
    if result.message is not None:
        body["message"] = result.message
    logger.info(
        "metadata.titlegen.applied",
        extra={
            "release_id": result.release_id,
            "channel_slug": result.channel_slug,
            "template_id": result.used_template_id,
            "overwrite_required": result.overwrite_required,
            "result_status": "no_op" if not result.title_updated else "ok",
            "error_codes": [],
        },
    )
    return body


@app.post("/v1/metadata/releases/{release_id}/descriptiongen/apply")
def api_metadata_descriptiongen_apply(
    release_id: int,
    payload: MetadataDescriptionGenApplyRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = descriptiongen_service.apply_generated_description(
                conn,
                release_id=release_id,
                template_id=payload.template_id,
                generation_fingerprint=payload.generation_fingerprint,
                overwrite_confirmed=payload.overwrite_confirmed,
            )
        except descriptiongen_service.DescriptionGenError as exc:
            status_code = 404 if exc.code in {"MTD_RELEASE_NOT_FOUND", "MTD_TEMPLATE_NOT_FOUND"} else 422
            logger.info(
                "metadata.descriptiongen.apply_failed",
                extra={
                    "release_id": release_id,
                    "channel_slug": None,
                    "template_id": payload.template_id,
                    "overwrite_required": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtb_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    body = {
        "release_id": result.release_id,
        "description_updated": result.description_updated,
        "description_before": result.description_before,
        "description_after": result.description_after,
        "used_template_id": result.used_template_id,
    }
    if result.message is not None:
        body["message"] = result.message
    logger.info(
        "metadata.descriptiongen.applied",
        extra={
            "release_id": result.release_id,
            "channel_slug": result.channel_slug,
            "template_id": result.used_template_id,
            "overwrite_required": result.overwrite_required,
            "result_status": "no_op" if not result.description_updated else "ok",
            "error_codes": [],
        },
    )
    return body


@app.get("/v1/metadata/title-templates/variables")
def api_metadata_title_templates_variables(_: bool = Depends(require_basic_auth(env))):
    return {"variables": title_template_service.allowed_variables_catalog()}


@app.post("/v1/metadata/title-templates/preview")
def api_metadata_title_templates_preview(
    payload: MetadataTitleTemplatePreviewRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    parsed_release_date: date | None = None
    if payload.release_date is not None:
        try:
            parsed_release_date = date.fromisoformat(payload.release_date)
        except ValueError:
            return _mtb_error(422, "MTB_INVALID_RELEASE_DATE", "release_date must use YYYY-MM-DD format")

    conn = dbm.connect(env)
    try:
        channel = dbm.get_channel_by_slug(conn, payload.channel_slug)
    finally:
        conn.close()

    if not channel:
        return _mtb_error(404, "MTB_CHANNEL_NOT_FOUND", "Channel not found")

    preview = title_template_service.preview_title_template(
        channel=channel,
        template_body=payload.template_body,
        release_date=parsed_release_date,
    )
    log_payload: Dict[str, Any] = {
        "channel_slug": payload.channel_slug,
        "render_status": preview.render_status,
        "missing_variables": list(preview.missing_variables),
        "validation_error_codes": [error["code"] for error in preview.validation_errors],
        "template_length": len(payload.template_body),
    }
    logger.info("metadata.title_template.previewed", extra=log_payload)
    if preview.validation_errors:
        logger.info("metadata.title_template.validation_failed", extra=log_payload)

    return preview.to_dict()


def _mtd_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _mtv_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _mdo_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _mpa_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


@app.get("/v1/metadata/description-templates/variables")
def api_metadata_description_templates_variables(_: bool = Depends(require_basic_auth(env))):
    return {"variables": description_template_service.allowed_variables_catalog()}


@app.post("/v1/metadata/description-templates/preview")
def api_metadata_description_templates_preview(
    payload: MetadataDescriptionTemplatePreviewRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        channel = dbm.get_channel_by_slug(conn, payload.channel_slug)
        if not channel:
            return _mtd_error(404, "MTD_CHANNEL_NOT_FOUND", "Channel not found")
        try:
            release_row = description_template_service.load_preview_release_context(
                conn,
                channel_slug=payload.channel_slug,
                release_id=payload.release_id,
            )
        except description_template_service.DescriptionTemplateError as exc:
            status_code = 404 if exc.code == "MTD_RELEASE_NOT_FOUND" else 422
            return _mtd_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    preview = description_template_service.preview_description_template(
        channel=channel,
        template_body=payload.template_body,
        release_row=release_row,
    )
    log_payload: Dict[str, Any] = {
        "channel_slug": payload.channel_slug,
        "release_id": payload.release_id,
        "render_status": preview.render_status,
        "missing_variables": list(preview.missing_variables),
        "used_variables": list(preview.used_variables),
        "validation_error_codes": [item["code"] for item in preview.validation_errors],
        "template_length": len(payload.template_body),
        "normalized_length": preview.normalized_length,
        "line_count": preview.line_count,
    }
    logger.info("metadata.description_template.previewed", extra=log_payload)
    if preview.validation_errors:
        logger.info("metadata.description_template.validation_failed", extra=log_payload)
    return preview.to_dict()


@app.get("/v1/metadata/video-tag-presets/variables")
def api_metadata_video_tag_presets_variables(_: bool = Depends(require_basic_auth(env))):
    return {"variables": video_tag_preset_service.allowed_variables_catalog()}


@app.post("/v1/metadata/video-tag-presets/preview")
def api_metadata_video_tag_presets_preview(
    payload: MetadataVideoTagPresetPreviewRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        channel = dbm.get_channel_by_slug(conn, payload.channel_slug)
        if not channel:
            return _mtv_error(404, "MTV_CHANNEL_NOT_FOUND", "Channel not found")
        try:
            release_row = video_tag_preset_service.load_preview_release_context(
                conn,
                channel_slug=payload.channel_slug,
                release_id=payload.release_id,
            )
        except video_tag_preset_service.VideoTagPresetError as exc:
            status_code = 404 if exc.code == "MTV_RELEASE_NOT_FOUND" else 422
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    preview = video_tag_preset_service.preview_video_tag_preset(
        channel=channel,
        preset_body=payload.preset_body,
        release_row=release_row,
    )
    log_payload: Dict[str, Any] = {
        "channel_slug": payload.channel_slug,
        "release_id": payload.release_id,
        "render_status": preview.render_status,
        "missing_variables": list(preview.missing_variables),
        "used_variables": list(preview.used_variables),
        "validation_error_codes": [item["code"] for item in preview.validation_errors],
        "preset_item_count": len(payload.preset_body),
        "normalized_count": preview.normalized_count,
    }
    logger.info("metadata.video_tag_preset.previewed", extra=log_payload)
    if preview.validation_errors:
        logger.info("metadata.video_tag_preset.validation_failed", extra=log_payload)
    return preview.to_dict()


@app.post("/v1/metadata/video-tag-presets")
def api_metadata_video_tag_presets_create(
    payload: MetadataVideoTagPresetCreateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = video_tag_preset_service.create_video_tag_preset(
                conn,
                channel_slug=payload.channel_slug,
                preset_name=payload.preset_name,
                preset_body=payload.preset_body,
                make_default=payload.make_default,
            )
        except video_tag_preset_service.VideoTagPresetError as exc:
            status_code = 404 if exc.code in {"MTV_CHANNEL_NOT_FOUND", "MTV_PRESET_NOT_FOUND"} else 422
            logger.info(
                "metadata.video_tag_preset.validation_failed",
                extra={
                    "preset_id": None,
                    "channel_slug": payload.channel_slug,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.video_tag_preset.created",
        extra={
            "preset_id": result["id"],
            "channel_slug": result["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return result


@app.get("/v1/metadata/video-tag-presets")
def api_metadata_video_tag_presets_list(
    channel_slug: str | None = None,
    status: str = "active",
    q: str | None = None,
    _: bool = Depends(require_basic_auth(env)),
):
    status_filter = (status or "active").lower()
    if status_filter not in {"active", "archived", "all"}:
        return _mtv_error(422, "MTV_INVALID_STATUS_FILTER", "status must be active|archived|all")
    conn = dbm.connect(env)
    try:
        rows = video_tag_preset_service.list_video_tag_presets(
            conn,
            channel_slug=channel_slug,
            status_filter=status_filter,
            q=q,
        )
    finally:
        conn.close()
    return {"items": rows}


@app.get("/v1/metadata/video-tag-presets/{preset_id}")
def api_metadata_video_tag_presets_detail(preset_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = video_tag_preset_service.get_video_tag_preset(conn, preset_id=preset_id)
        except video_tag_preset_service.VideoTagPresetError as exc:
            status_code = 404 if exc.code in {"MTV_CHANNEL_NOT_FOUND", "MTV_PRESET_NOT_FOUND"} else 422
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return item


@app.patch("/v1/metadata/video-tag-presets/{preset_id}")
def api_metadata_video_tag_presets_patch(
    preset_id: int,
    payload: MetadataVideoTagPresetPatchRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            item = video_tag_preset_service.update_video_tag_preset(
                conn,
                preset_id=preset_id,
                preset_name=payload.preset_name,
                preset_body=payload.preset_body,
            )
            conn.commit()
        except video_tag_preset_service.VideoTagPresetError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"MTV_CHANNEL_NOT_FOUND", "MTV_PRESET_NOT_FOUND"} else 422
            logger.info(
                "metadata.video_tag_preset.validation_failed",
                extra={
                    "preset_id": preset_id,
                    "channel_slug": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.video_tag_preset.updated",
        extra={
            "preset_id": item["id"],
            "channel_slug": item["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/video-tag-presets/{preset_id}/set-default")
def api_metadata_video_tag_presets_set_default(preset_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = video_tag_preset_service.set_default_video_tag_preset(conn, preset_id=preset_id)
            conn.commit()
        except video_tag_preset_service.VideoTagPresetError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"MTV_CHANNEL_NOT_FOUND", "MTV_PRESET_NOT_FOUND"} else 422
            logger.info(
                "metadata.video_tag_preset.validation_failed",
                extra={
                    "preset_id": preset_id,
                    "channel_slug": None,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.video_tag_preset.default_changed",
        extra={
            "preset_id": item["id"],
            "channel_slug": item["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/video-tag-presets/{preset_id}/archive")
def api_metadata_video_tag_presets_archive(preset_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = video_tag_preset_service.archive_video_tag_preset(conn, preset_id=preset_id)
            conn.commit()
        except video_tag_preset_service.VideoTagPresetError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"MTV_CHANNEL_NOT_FOUND", "MTV_PRESET_NOT_FOUND"} else 422
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.video_tag_preset.archived",
        extra={
            "preset_id": item["id"],
            "channel_slug": item["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/video-tag-presets/{preset_id}/activate")
def api_metadata_video_tag_presets_activate(preset_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = video_tag_preset_service.activate_video_tag_preset(conn, preset_id=preset_id)
            conn.commit()
        except video_tag_preset_service.VideoTagPresetError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"MTV_CHANNEL_NOT_FOUND", "MTV_PRESET_NOT_FOUND"} else 422
            return _mtv_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.video_tag_preset.activated",
        extra={
            "preset_id": item["id"],
            "channel_slug": item["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/channel-visual-style-templates")
def api_metadata_channel_visual_style_templates_create(
    payload: ChannelVisualStyleTemplateCreateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = channel_visual_style_template_service.create_channel_visual_style_template(
                conn,
                channel_slug=payload.channel_slug,
                template_name=payload.template_name,
                template_payload=payload.template_payload,
                make_default=payload.make_default,
            )
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            status_code = 404 if exc.code in {"CVST_CHANNEL_NOT_FOUND", "CVST_TEMPLATE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return result


@app.get("/v1/metadata/channel-visual-style-templates")
def api_metadata_channel_visual_style_templates_list(
    channel_slug: str | None = None,
    status: str = "active",
    q: str | None = None,
    _: bool = Depends(require_basic_auth(env)),
):
    status_filter = (status or "active").lower()
    if status_filter not in {"active", "archived", "all"}:
        return _cvst_error(422, "CVST_INVALID_STATUS_FILTER", "status must be active|archived|all")
    conn = dbm.connect(env)
    try:
        rows = channel_visual_style_template_service.list_channel_visual_style_templates(
            conn,
            channel_slug=channel_slug,
            status_filter=status_filter,
            q=q,
        )
    finally:
        conn.close()
    return {"items": rows}


@app.get("/v1/metadata/channel-visual-style-templates/{template_id}")
def api_metadata_channel_visual_style_templates_detail(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = channel_visual_style_template_service.get_channel_visual_style_template(conn, template_id=template_id)
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            status_code = 404 if exc.code in {"CVST_TEMPLATE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return item


@app.patch("/v1/metadata/channel-visual-style-templates/{template_id}")
def api_metadata_channel_visual_style_templates_patch(
    template_id: int,
    payload: ChannelVisualStyleTemplatePatchRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            item = channel_visual_style_template_service.update_channel_visual_style_template(
                conn,
                template_id=template_id,
                template_name=payload.template_name,
                template_payload=payload.template_payload,
            )
            conn.commit()
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"CVST_TEMPLATE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return item


@app.post("/v1/metadata/channel-visual-style-templates/{template_id}/archive")
def api_metadata_channel_visual_style_templates_archive(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = channel_visual_style_template_service.archive_channel_visual_style_template(conn, template_id=template_id)
            conn.commit()
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"CVST_TEMPLATE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return item


@app.post("/v1/metadata/channel-visual-style-templates/{template_id}/activate")
def api_metadata_channel_visual_style_templates_activate(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = channel_visual_style_template_service.activate_channel_visual_style_template(conn, template_id=template_id)
            conn.commit()
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"CVST_TEMPLATE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return item


@app.post("/v1/metadata/channel-visual-style-templates/{template_id}/set-default")
def api_metadata_channel_visual_style_templates_set_default(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = channel_visual_style_template_service.set_default_channel_visual_style_template(conn, template_id=template_id)
            conn.commit()
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"CVST_TEMPLATE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return item


@app.post("/v1/metadata/channel-visual-style-templates/releases/{release_id}/override")
def api_metadata_channel_visual_style_templates_release_override_set(
    release_id: int,
    payload: ChannelVisualStyleTemplateReleaseOverrideRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            out = channel_visual_style_template_service.set_release_visual_style_template_override(
                conn,
                release_id=release_id,
                template_id=payload.template_id,
            )
            conn.commit()
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"CVST_RELEASE_NOT_FOUND", "CVST_TEMPLATE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return out


@app.post("/v1/metadata/channel-visual-style-templates/releases/{release_id}/override/clear")
def api_metadata_channel_visual_style_templates_release_override_clear(
    release_id: int,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            out = channel_visual_style_template_service.clear_release_visual_style_template_override(
                conn,
                release_id=release_id,
            )
            conn.commit()
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            conn.rollback()
            status_code = 404 if exc.code in {"CVST_RELEASE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return out


@app.get("/v1/metadata/channel-visual-style-templates/releases/{release_id}/effective")
def api_metadata_channel_visual_style_templates_release_effective(
    release_id: int,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            out = channel_visual_style_template_service.resolve_effective_channel_visual_style_template_for_release(
                conn,
                release_id=release_id,
            )
        except channel_visual_style_template_service.ChannelVisualStyleTemplateError as exc:
            status_code = 404 if exc.code in {"CVST_RELEASE_NOT_FOUND"} else 422
            return _cvst_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return out


@app.post("/v1/metadata/description-templates")
def api_metadata_description_templates_create(
    payload: MetadataDescriptionTemplateCreateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = description_template_service.create_description_template(
                conn,
                channel_slug=payload.channel_slug,
                template_name=payload.template_name,
                template_body=payload.template_body,
                make_default=payload.make_default,
            )
        except description_template_service.DescriptionTemplateError as exc:
            status_code = 404 if exc.code in {"MTD_CHANNEL_NOT_FOUND", "MTD_TEMPLATE_NOT_FOUND"} else 422
            logger.info(
                "metadata.description_template.validation_failed",
                extra={
                    "template_id": None,
                    "channel_slug": payload.channel_slug,
                    "result_status": "error",
                    "error_codes": [exc.code],
                },
            )
            return _mtd_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.description_template.created",
        extra={
            "template_id": result["id"],
            "channel_slug": result["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return result


@app.get("/v1/metadata/description-templates")
def api_metadata_description_templates_list(
    channel_slug: str | None = None,
    status: str = "active",
    q: str | None = None,
    _: bool = Depends(require_basic_auth(env)),
):
    status_filter = (status or "active").lower()
    if status_filter not in {"active", "archived", "all"}:
        return _mtd_error(422, "MTD_INVALID_STATUS_FILTER", "status must be active|archived|all")
    conn = dbm.connect(env)
    try:
        rows = description_template_service.list_description_templates(
            conn,
            channel_slug=channel_slug,
            status_filter=status_filter,
            q=q,
        )
    finally:
        conn.close()
    return {"items": rows}


@app.get("/v1/metadata/description-templates/{template_id}")
def api_metadata_description_templates_detail(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = description_template_service.get_description_template(conn, template_id=template_id)
        except description_template_service.DescriptionTemplateError as exc:
            status_code = 404 if exc.code in {"MTD_CHANNEL_NOT_FOUND", "MTD_TEMPLATE_NOT_FOUND"} else 422
            return _mtd_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return item


@app.patch("/v1/metadata/description-templates/{template_id}")
def api_metadata_description_templates_patch(
    template_id: int,
    payload: MetadataDescriptionTemplatePatchRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            item = description_template_service.update_description_template(
                conn,
                template_id=template_id,
                template_name=payload.template_name,
                template_body=payload.template_body,
            )
            conn.commit()
        except description_template_service.DescriptionTemplateError as exc:
            status_code = 404 if exc.code in {"MTD_CHANNEL_NOT_FOUND", "MTD_TEMPLATE_NOT_FOUND"} else 422
            if status_code == 422:
                logger.info(
                    "metadata.description_template.validation_failed",
                    extra={
                        "template_id": template_id,
                        "channel_slug": None,
                        "result_status": "error",
                        "error_codes": [exc.code],
                    },
                )
            return _mtd_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.description_template.updated",
        extra={
            "template_id": item["id"],
            "channel_slug": item["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/description-templates/{template_id}/set-default")
def api_metadata_description_templates_set_default(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = description_template_service.set_default_description_template(conn, template_id=template_id)
            conn.commit()
        except description_template_service.DescriptionTemplateError as exc:
            status_code = 404 if exc.code in {"MTD_TEMPLATE_NOT_FOUND"} else 422
            return _mtd_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.description_template.default_changed",
        extra={
            "template_id": item["id"],
            "channel_slug": item["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/description-templates/{template_id}/archive")
def api_metadata_description_templates_archive(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = description_template_service.archive_description_template(conn, template_id=template_id)
            conn.commit()
        except description_template_service.DescriptionTemplateError as exc:
            status_code = 404 if exc.code in {"MTD_TEMPLATE_NOT_FOUND"} else 422
            return _mtd_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.description_template.archived",
        extra={
            "template_id": item["id"],
            "channel_slug": item["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/description-templates/{template_id}/activate")
def api_metadata_description_templates_activate(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = description_template_service.activate_description_template(conn, template_id=template_id)
            conn.commit()
        except description_template_service.DescriptionTemplateError as exc:
            status_code = 404 if exc.code in {"MTD_TEMPLATE_NOT_FOUND"} else 422
            return _mtd_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.description_template.activated",
        extra={
            "template_id": item["id"],
            "channel_slug": item["channel_slug"],
            "result_status": "success",
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/title-templates")
def api_metadata_title_templates_create(
    payload: MetadataTitleTemplateCreateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            result = title_template_service.create_title_template(
                conn,
                channel_slug=payload.channel_slug,
                template_name=payload.template_name,
                template_body=payload.template_body,
                make_default=payload.make_default,
            )
        except title_template_service.TemplateValidationError as exc:
            if exc.code in {"MTB_CHANNEL_NOT_FOUND", "MTB_TEMPLATE_NOT_FOUND"}:
                return _mtb_error(404, exc.code, exc.message)
            logger.info(
                "metadata.title_template.validation_failed",
                extra={
                    "template_id": None,
                    "channel_slug": payload.channel_slug,
                    "template_name": payload.template_name,
                    "status": "ACTIVE",
                    "is_default": payload.make_default,
                    "validation_status": "INVALID",
                    "error_codes": [exc.code],
                },
            )
            return _mtb_error(422, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.title_template.created",
        extra={
            "template_id": result["id"],
            "channel_slug": result["channel_slug"],
            "template_name": result["template_name"],
            "status": result["status"],
            "is_default": result["is_default"],
            "validation_status": result["validation_status"],
            "error_codes": [],
        },
    )
    return result


@app.get("/v1/metadata/title-templates")
def api_metadata_title_templates_list(
    channel_slug: str | None = None,
    status: str = "active",
    q: str | None = None,
    _: bool = Depends(require_basic_auth(env)),
):
    status_filter = (status or "active").lower()
    if status_filter not in {"active", "archived", "all"}:
        return _mtb_error(422, "MTB_INVALID_STATUS_FILTER", "status must be active|archived|all")
    conn = dbm.connect(env)
    try:
        rows = title_template_service.list_title_templates(
            conn,
            channel_slug=channel_slug,
            status_filter=status_filter,
            q=q,
        )
    finally:
        conn.close()
    return {"items": rows}


@app.get("/v1/metadata/title-templates/{template_id}")
def api_metadata_title_templates_detail(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = title_template_service.get_title_template(conn, template_id=template_id)
        except title_template_service.TemplateValidationError as exc:
            status_code = 404 if exc.code in {"MTB_CHANNEL_NOT_FOUND", "MTB_TEMPLATE_NOT_FOUND"} else 422
            return _mtb_error(status_code, exc.code, exc.message)
    finally:
        conn.close()
    return item


@app.patch("/v1/metadata/title-templates/{template_id}")
def api_metadata_title_templates_patch(
    template_id: int,
    payload: MetadataTitleTemplatePatchRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            item = title_template_service.update_title_template(
                conn,
                template_id=template_id,
                template_name=payload.template_name,
                template_body=payload.template_body,
            )
            conn.commit()
        except title_template_service.TemplateValidationError as exc:
            status_code = 404 if exc.code in {"MTB_CHANNEL_NOT_FOUND", "MTB_TEMPLATE_NOT_FOUND"} else 422
            if status_code == 422:
                logger.info(
                    "metadata.title_template.validation_failed",
                    extra={
                        "template_id": template_id,
                        "channel_slug": None,
                        "template_name": payload.template_name,
                        "status": None,
                        "is_default": None,
                        "validation_status": "INVALID",
                        "error_codes": [exc.code],
                    },
                )
            return _mtb_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.title_template.updated",
        extra={
            "template_id": item["id"],
            "channel_slug": item["channel_slug"],
            "template_name": item["template_name"],
            "status": item["status"],
            "is_default": item["is_default"],
            "validation_status": item["validation_status"],
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/title-templates/{template_id}/set-default")
def api_metadata_title_templates_set_default(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = title_template_service.set_default_title_template(conn, template_id=template_id)
            conn.commit()
        except title_template_service.TemplateValidationError as exc:
            status_code = 404 if exc.code in {"MTB_TEMPLATE_NOT_FOUND"} else 422
            return _mtb_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.title_template.default_changed",
        extra={
            "template_id": item["id"],
            "channel_slug": item["channel_slug"],
            "template_name": item["template_name"],
            "status": item["status"],
            "is_default": item["is_default"],
            "validation_status": item["validation_status"],
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/title-templates/{template_id}/archive")
def api_metadata_title_templates_archive(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = title_template_service.archive_title_template(conn, template_id=template_id)
            conn.commit()
        except title_template_service.TemplateValidationError as exc:
            status_code = 404 if exc.code in {"MTB_TEMPLATE_NOT_FOUND"} else 422
            return _mtb_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.title_template.archived",
        extra={
            "template_id": item["id"],
            "channel_slug": item["channel_slug"],
            "template_name": item["template_name"],
            "status": item["status"],
            "is_default": item["is_default"],
            "validation_status": item["validation_status"],
            "error_codes": [],
        },
    )
    return item


@app.post("/v1/metadata/title-templates/{template_id}/activate")
def api_metadata_title_templates_activate(template_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            item = title_template_service.activate_title_template(conn, template_id=template_id)
            conn.commit()
        except title_template_service.TemplateValidationError as exc:
            status_code = 404 if exc.code in {"MTB_TEMPLATE_NOT_FOUND"} else 422
            return _mtb_error(status_code, exc.code, exc.message)
    finally:
        conn.close()

    logger.info(
        "metadata.title_template.activated",
        extra={
            "template_id": item["id"],
            "channel_slug": item["channel_slug"],
            "template_name": item["template_name"],
            "status": item["status"],
            "is_default": item["is_default"],
            "validation_status": item["validation_status"],
            "error_codes": [],
        },
    )
    return item


@app.get("/v1/playlist-builder/channels/{channel_slug}/settings")
def api_playlist_builder_channel_settings_get(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        row = dbm.get_playlist_builder_channel_settings(conn, channel_slug)
    finally:
        conn.close()
    if not row:
        return _plb_error(404, "PLB_CHANNEL_SETTINGS_NOT_FOUND", "Playlist builder channel settings not found")
    return build_channel_settings_payload(channel_slug=channel_slug, row=row)


@app.put("/v1/playlist-builder/channels/{channel_slug}/settings")
def api_playlist_builder_channel_settings_put(
    channel_slug: str,
    payload: PlaylistChannelSettingsPatch,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        channel = dbm.get_channel_by_slug(conn, channel_slug)
        if not channel:
            return _plb_error(404, "PLB_CHANNEL_SETTINGS_NOT_FOUND", "Playlist builder channel settings not found")
        existing = dbm.get_playlist_builder_channel_settings(conn, channel_slug)
        merged_patch = {
            **channel_settings_row_to_patch(existing),
            **payload.as_patch_dict(),
        }
        brief = resolve_playlist_brief(
            channel_slug=channel_slug,
            job_id=None,
            channel_settings=merged_patch,
            job_override=None,
            request_override=None,
        )
        dbm.upsert_playlist_builder_channel_settings(
            conn,
            channel_slug=channel_slug,
            default_generation_mode=brief.generation_mode,
            min_duration_min=brief.min_duration_min,
            max_duration_min=brief.max_duration_min,
            tolerance_min=brief.tolerance_min,
            preferred_month_batch=brief.preferred_month_batch,
            preferred_batch_ratio=brief.preferred_batch_ratio,
            allow_cross_channel=brief.allow_cross_channel,
            novelty_target_min=brief.novelty_target_min,
            novelty_target_max=brief.novelty_target_max,
            position_memory_window=brief.position_memory_window,
            strictness_mode=brief.strictness_mode,
            vocal_policy=brief.vocal_policy,
            reuse_policy=brief.reuse_policy,
        )
        conn.commit()
        saved = dbm.get_playlist_builder_channel_settings(conn, channel_slug)
    except PlaylistBuilderValidationError as exc:
        return _plb_error(422, "PLB_INVALID_BRIEF", str(exc))
    finally:
        conn.close()
    assert saved is not None
    return build_channel_settings_payload(channel_slug=channel_slug, row=saved)


@app.get("/v1/playlist-builder/jobs/{job_id}/brief")
def api_playlist_builder_job_brief_get(job_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        draft = dbm.get_ui_job_draft(conn, job_id)
        if not job or not draft:
            return _plb_error(404, "PLB_JOB_NOT_FOUND", "UI job not found")

        channel_slug = str(job.get("channel_slug") or "")
        settings_row = dbm.get_playlist_builder_channel_settings(conn, channel_slug)
        settings_patch = channel_settings_row_to_patch(settings_row)
        job_override = parse_override_json(draft.get("playlist_builder_override_json"))
        brief = resolve_playlist_brief(
            channel_slug=channel_slug,
            job_id=job_id,
            channel_settings=settings_patch,
            job_override=job_override,
            request_override=None,
        )
    except PlaylistBuilderValidationError as exc:
        return _plb_error(422, "PLB_INVALID_BRIEF", str(exc))
    finally:
        conn.close()

    return {"brief": brief.to_api_dict()}


@app.patch("/v1/ui/jobs/{job_id}/playlist-builder/override")
def api_playlist_builder_job_override_patch(
    job_id: int,
    payload: PlaylistBriefOverrides,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        draft = dbm.get_ui_job_draft(conn, job_id)
        if not job or not draft:
            return _plb_error(404, "PLB_JOB_NOT_FOUND", "UI job not found")
        channel_slug = str(job.get("channel_slug") or "")
        settings_row = dbm.get_playlist_builder_channel_settings(conn, channel_slug)
        settings_patch = channel_settings_row_to_patch(settings_row)

        existing_override = parse_override_json(draft.get("playlist_builder_override_json"))
        override_patch = payload.as_patch_dict()
        merged_override = {
            **existing_override,
            **override_patch,
        }
        resolve_playlist_brief(
            channel_slug=channel_slug,
            job_id=job_id,
            channel_settings=settings_patch,
            job_override=merged_override,
            request_override=None,
        )

        dbm.update_ui_job_playlist_builder_override_json(
            conn,
            job_id=job_id,
            playlist_builder_override_json=json.dumps(merged_override, sort_keys=True),
        )
        conn.commit()
    except PlaylistBuilderValidationError as exc:
        return _plb_error(422, "PLB_INVALID_BRIEF", str(exc))
    finally:
        conn.close()

    return {"job_id": str(job_id), "override": merged_override}


class PlaylistBuilderPreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    override: dict[str, Any] = Field(default_factory=dict)


class PlaylistBuilderApplyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    preview_id: str


@app.post("/v1/playlist-builder/jobs/{job_id}/preview")
def api_playlist_builder_preview_post(
    job_id: int,
    payload: PlaylistBuilderPreviewRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        logger.info("playlist_builder.preview.started", extra={"job_id": job_id})
        envelope = create_preview(conn, job_id=job_id, override=payload.override, created_by=env.basic_user)
        response_started = time.perf_counter()
        response = build_preview_response(envelope)
        diagnostics = response.get("summary", {}).get("diagnostics") or {}
        diagnostics["response_serialization_ms"] = round((time.perf_counter() - response_started) * 1000.0, 3)
        response["summary"]["diagnostics"] = diagnostics
        conn.commit()
        logger.info(
            "playlist_builder.preview.completed",
            extra={
                "job_id": job_id,
                "channel_slug": envelope.brief.channel_slug,
                "generation_mode": envelope.brief.generation_mode,
                "strictness_mode": envelope.brief.strictness_mode,
                "candidate_pool_size": envelope.preview_result.candidate_pool_size,
                "selected_tracks_count": len(envelope.tracks),
                "achieved_duration": envelope.preview_result.achieved_duration_sec,
                "achieved_novelty": envelope.preview_result.achieved_novelty,
                "achieved_batch_ratio": envelope.preview_result.achieved_batch_ratio,
                "relaxations": envelope.preview_result.relaxations,
                "relaxations_structured": [item.model_dump() for item in envelope.preview_result.relaxations_structured],
                "warnings": envelope.preview_result.warnings,
            },
        )
        logger.info(
            "playlist_builder.preview.pipeline",
            extra={
                "job_id": job_id,
                "diagnostics": envelope.preview_result.diagnostics or {},
            },
        )
        for relaxation in envelope.preview_result.relaxations_structured:
            logger.info("playlist_builder.relaxation.applied", extra={"job_id": job_id, "relaxation": relaxation.model_dump()})
        return response
    except PlaylistBuilderApiError as exc:
        conn.rollback()
        status = {
            "PLB_INVALID_BRIEF": 422,
            "PLB_JOB_NOT_FOUND": 404,
            "PLB_NO_CANDIDATES": 422,
            "PLB_NO_VALID_PLAYLIST": 422,
            "PLB_CURATED_LIMIT_EXCEEDED": 422,
            "PLB_PREVIEW_TIMEOUT": 422,
        }.get(exc.code, 409)
        logger.info("playlist_builder.preview.pipeline", extra={"job_id": job_id, "diagnostics": exc.diagnostics})
        return _plb_error(status, exc.code, exc.message, diagnostics=exc.diagnostics)
    except PreviewTimeBudgetExceeded as exc:
        conn.rollback()
        diagnostics = {
            "reason": str(exc),
            "timeout_stage": exc.stage,
            "preview_total_ms": exc.elapsed_ms,
        }
        logger.info("playlist_builder.preview.pipeline", extra={"job_id": job_id, "diagnostics": diagnostics})
        return _plb_error(422, "PLB_PREVIEW_TIMEOUT", str(exc), diagnostics=diagnostics)
    except Exception as exc:
        conn.rollback()
        diagnostics = {"reason": str(exc), "job_id": job_id}
        logger.exception("playlist_builder.preview.failed", extra={"job_id": job_id})
        return _plb_error(500, "PLB_PREVIEW_FAILED", "Playlist preview failed", diagnostics=diagnostics)
    finally:
        conn.close()


@app.post("/v1/playlist-builder/jobs/{job_id}/apply")
def api_playlist_builder_apply_post(
    job_id: int,
    payload: PlaylistBuilderApplyRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        applied = apply_preview(conn, job_id=job_id, preview_id=payload.preview_id)
        logger.info("playlist_builder.apply.completed", extra={"job_id": job_id, "preview_id": payload.preview_id})
        if applied.get("history_written"):
            logger.info(
                "playlist_builder.history.draft_written",
                extra={"job_id": job_id, "preview_id": payload.preview_id, "draft_history_id": applied["draft_history_id"]},
            )
        return {
            "job_id": applied["job_id"],
            "playlist_applied": applied["playlist_applied"],
            "draft_history_id": applied["draft_history_id"],
        }
    except PlaylistBuilderApiError as exc:
        conn.rollback()
        status = {
            "PLB_JOB_NOT_FOUND": 404,
            "PLB_PREVIEW_NOT_FOUND": 404,
            "PLB_PREVIEW_EXPIRED": 409,
            "PLB_APPLY_CONFLICT": 409,
            "PLB_HISTORY_WRITE_FAILED": 500,
        }.get(exc.code, 409)
        return _plb_error(status, exc.code, exc.message)
    finally:
        conn.close()


def _require_channel(channel_slug: str) -> None:
    conn = dbm.connect(env)
    try:
        existing = dbm.get_channel_by_slug(conn, channel_slug)
    finally:
        conn.close()
    if not existing:
        raise HTTPException(404, "channel not found")


def _oauth_start(kind: str, channel_slug: str) -> dict:
    _require_channel(channel_slug)
    client_secret_path, tokens_dir, scope = validate_oauth_config(env, kind=kind)
    ensure_token_dir(oauth_token_path(base_dir=tokens_dir, channel_slug=channel_slug))
    state = sign_state(secret=env.oauth_state_secret, kind=kind, channel_slug=channel_slug)
    url = build_authorization_url(
        client_secret_path=client_secret_path,
        scope=scope,
        redirect_uri=redirect_uri(env, kind),
        state=state,
    )
    return {"auth_url": url}


def _oauth_callback(kind: str, code: str, state: str) -> HTMLResponse:
    client_secret_path, tokens_dir, scope = validate_oauth_config(env, kind=kind)
    payload = verify_state(secret=env.oauth_state_secret, expected_kind=kind, state=state)
    channel_slug = str(payload["channel_slug"])
    _require_channel(channel_slug)

    token_json = exchange_code_for_token_json(
        client_secret_path=client_secret_path,
        scope=scope,
        redirect_uri=redirect_uri(env, kind),
        code=code,
    )
    token_path = oauth_token_path(base_dir=tokens_dir, channel_slug=channel_slug)
    ensure_token_dir(token_path)
    token_path.write_text(token_json, encoding="utf-8")
    token_path.chmod(0o600)
    return HTMLResponse(
        content=(
            "<html><body><h3>OAuth token saved</h3>"
            f"<p>kind={kind}, channel={channel_slug}</p>"
            "<p>You can close this tab.</p></body></html>"
        )
    )




def _storage_tmp_oauth_dir() -> Path:
    root = Path(env.storage_root).expanduser()
    return root / "tmp" / "oauth"


def _write_temp_oauth_token(nonce: str, token_json: str) -> Path:
    tmp_dir = _storage_tmp_oauth_dir()
    tmp_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    token_path = tmp_dir / f"{nonce}.json"
    token_path.write_text(token_json, encoding="utf-8")
    token_path.chmod(0o600)
    return token_path


def _read_temp_oauth_token(nonce: str) -> str:
    token_path = _storage_tmp_oauth_dir() / f"{nonce}.json"
    if not token_path.is_file():
        raise HTTPException(400, "oauth session expired")
    return token_path.read_text(encoding="utf-8")


def _delete_temp_oauth_token(nonce: str) -> None:
    token_path = _storage_tmp_oauth_dir() / f"{nonce}.json"
    if token_path.is_file():
        token_path.unlink()


def _youtube_channels_from_token_json(token_json: str) -> list[dict[str, str]]:
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    creds = Credentials.from_authorized_user_info(json.loads(token_json), ["https://www.googleapis.com/auth/youtube.upload"])
    service = build("youtube", "v3", credentials=creds, cache_discovery=False)
    resp = service.channels().list(part="snippet", mine=True).execute()
    channels = []
    for item in resp.get("items", []):
        cid = str(item.get("id") or "").strip()
        title = str(((item.get("snippet") or {}).get("title")) or "").strip()
        if cid and title:
            channels.append({"id": cid, "title": title})
    if not channels:
        raise HTTPException(400, "no youtube channels found for this account")
    return channels


def _slugify_channel_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower())
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "channel"


def _next_available_slug(conn, base_slug: str) -> str:
    if dbm.get_channel_by_slug(conn, base_slug) is None:
        return base_slug
    idx = 2
    while True:
        candidate = f"{base_slug}-{idx}"
        if dbm.get_channel_by_slug(conn, candidate) is None:
            return candidate
        idx += 1


def _connect_youtube_channel(*, youtube_channel_id: str, display_name: str, token_json: str) -> tuple[bool, str]:
    conn = dbm.connect(env)
    try:
        existing = dbm.get_channel_by_youtube_channel_id(conn, youtube_channel_id)
        if existing:
            return False, str(existing["slug"])

        base_slug = _slugify_channel_name(display_name)
        slug = _next_available_slug(conn, base_slug)
        token_path = oauth_token_path(base_dir=env.yt_tokens_dir, channel_slug=slug)
        ensure_token_dir(token_path)
        token_path.write_text(token_json, encoding="utf-8")
        token_path.chmod(0o600)
        dbm.create_channel(conn, slug=slug, display_name=display_name, youtube_channel_id=youtube_channel_id)
        return True, slug
    finally:
        conn.close()

def _token_status_for(channel_slug: str, base_dir: str) -> tuple[bool, str | None]:
    token_path = oauth_token_path(base_dir=base_dir, channel_slug=channel_slug)
    if not token_path.is_file():
        return False, None
    return True, str(token_path.stat().st_mtime)


def _yamnet_error_with_guidance(raw_error: str | None) -> str | None:
    if not raw_error:
        return raw_error

    lowered = raw_error.lower()
    numpy_abi_mismatch = (
        "compiled using numpy 1" in lowered
        or "numpy 2" in lowered
        or "numpy.core.umath failed to import" in lowered
    )
    if numpy_abi_mismatch and "numpy<2" not in lowered:
        return f"{raw_error}; reinstall YamNet deps with numpy<2 (requirements-yamnet.txt)."
    return raw_error


def _yamnet_import_status() -> dict[str, Any]:
    target_dir = ensure_py_deps_on_sys_path(os.environ)
    import_tf = False
    import_hub = False
    error: str | None = None
    try:
        import tensorflow  # noqa: F401

        import_tf = True
    except Exception as exc:
        error = str(exc)
    try:
        import tensorflow_hub  # noqa: F401

        import_hub = True
    except Exception as exc:
        if not error:
            error = str(exc)
    return {
        "installed": import_tf and import_hub,
        "target_dir": target_dir,
        "import_tf": import_tf,
        "import_hub": import_hub,
        "error": _yamnet_error_with_guidance(error),
    }


def _run_yamnet_installer(*, target_dir: str) -> tuple[bool, str]:
    repo_root = Path(__file__).resolve().parents[2]
    installer_path = repo_root / "scripts" / "install_yamnet.py"
    cmd = [sys.executable, str(installer_path), "--target", target_dir]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=1800)
    combined = "\n".join(filter(None, [proc.stdout, proc.stderr])).strip()
    output = combined or "yamnet dependencies installed"
    tail_lines = output.splitlines()[-40:]
    tail = "\n".join(tail_lines)
    if proc.returncode != 0:
        return False, tail[:4000]
    return True, tail[:4000]


@app.get("/v1/admin/yamnet/status")
def api_admin_yamnet_status(_: bool = Depends(require_basic_auth(env))):
    return _yamnet_import_status()


@app.post("/v1/admin/yamnet/install")
def api_admin_install_yamnet(_: bool = Depends(require_basic_auth(env))):
    status = _yamnet_import_status()
    if status["installed"]:
        return {"ok": True, "target_dir": status["target_dir"], "installed": True, "output_tail": "already installed"}

    target_dir = str(status["target_dir"])
    try:
        ok, output_tail = _run_yamnet_installer(target_dir=target_dir)
    except subprocess.TimeoutExpired:
        logger.warning("yamnet_install timed_out target_dir=%s", target_dir)
        return {"ok": False, "target_dir": target_dir, "installed": False, "output_tail": "installer timed out"}
    except Exception:
        logger.exception("yamnet_install failed target_dir=%s", target_dir)
        return {"ok": False, "target_dir": target_dir, "installed": False, "output_tail": "installer failed"}

    if not ok:
        logger.warning("yamnet_install failed target_dir=%s detail=%s", target_dir, output_tail)
        return {"ok": False, "target_dir": target_dir, "installed": False, "output_tail": output_tail or "installer failed"}

    status = _yamnet_import_status()
    return {
        "ok": True,
        "target_dir": target_dir,
        "installed": bool(status["installed"]),
        "output_tail": output_tail,
    }


@app.post("/v1/oauth/gdrive/{channel_slug}/start")
def api_oauth_gdrive_start(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
    return _oauth_start("gdrive", channel_slug)


@app.get("/v1/oauth/gdrive/callback", response_class=HTMLResponse)
def api_oauth_gdrive_callback(code: str, state: str, _: bool = Depends(require_basic_auth(env))):
    return _oauth_callback("gdrive", code, state)


@app.post("/v1/oauth/youtube/{channel_slug}/start")
def api_oauth_youtube_start(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
    if channel_slug == "add_channel":
        return api_oauth_youtube_add_channel_start(_)
    return _oauth_start("youtube", channel_slug)


@app.post("/v1/oauth/youtube/add_channel/start")
def api_oauth_youtube_add_channel_start(_: bool = Depends(require_basic_auth(env))):
    client_secret_path, _tokens_dir, scope = validate_oauth_config(env, kind="youtube")
    state = sign_state(secret=env.oauth_state_secret, kind="youtube_add_channel")
    url = build_authorization_url(
        client_secret_path=client_secret_path,
        scope=scope,
        redirect_uri=redirect_uri(env, "youtube/add_channel"),
        state=state,
    )
    return {"auth_url": url}


@app.get("/v1/oauth/youtube/add_channel/callback", response_class=HTMLResponse)
def api_oauth_youtube_add_channel_callback(code: str, state: str, _: bool = Depends(require_basic_auth(env))):
    client_secret_path, _tokens_dir, scope = validate_oauth_config(env, kind="youtube")
    payload = verify_state(secret=env.oauth_state_secret, expected_kind="youtube_add_channel", state=state, require_channel_slug=False)
    token_json = exchange_code_for_token_json(
        client_secret_path=client_secret_path,
        scope=scope,
        redirect_uri=redirect_uri(env, "youtube/add_channel"),
        code=code,
    )
    channels = _youtube_channels_from_token_json(token_json)
    nonce = str(payload.get("nonce") or "")
    if not nonce:
        raise HTTPException(400, "invalid oauth state")
    _write_temp_oauth_token(nonce, token_json)
    if len(channels) == 1:
        only = channels[0]
        created, slug = _connect_youtube_channel(youtube_channel_id=only["id"], display_name=only["title"], token_json=token_json)
        _delete_temp_oauth_token(nonce)
        if not created:
            return HTMLResponse(content="<html><body><h3>Channel already connected</h3><p>This YouTube channel is already connected.</p><p>You can close this tab and refresh dashboard.</p></body></html>")
        return HTMLResponse(content=f"<html><body><h3>Channel connected</h3><p>Connected: {html.escape(only['title'])} ({html.escape(slug)})</p><p>You can close this tab and refresh dashboard.</p></body></html>")

    options = "".join(
        f'<label><input type="radio" name="youtube_channel_id" value="{html.escape(c["id"])}" required> {html.escape(c["title"])} ({html.escape(c["id"])})</label><br/>'
        for c in channels
    )
    confirm_state = sign_state(secret=env.oauth_state_secret, kind="youtube_add_channel_confirm", extra={"nonce": nonce})
    page = (
        "<html><body><h3>Select YouTube Channel</h3>"
        "<form method='post' action='/v1/oauth/youtube/add_channel/confirm'>"
        f"<input type='hidden' name='state' value='{html.escape(confirm_state)}'>"
        f"{options}<button type='submit'>Connect channel</button></form>"
        "</body></html>"
    )
    return HTMLResponse(content=page)


@app.post("/v1/oauth/youtube/add_channel/confirm", response_class=HTMLResponse)
async def api_oauth_youtube_add_channel_confirm(request: Request, _: bool = Depends(require_basic_auth(env))):
    from urllib.parse import parse_qs

    raw = request.scope.get("query_string", b"").decode("utf-8")
    values = parse_qs(raw, keep_blank_values=False)
    if not values:
        body = await request.body()
        values = parse_qs(body.decode("utf-8"), keep_blank_values=False)

    state = (values.get("state") or [""])[0]
    youtube_channel_id = (values.get("youtube_channel_id") or [""])[0]
    if not state or not youtube_channel_id:
        raise HTTPException(422, "state and youtube_channel_id are required")
    payload = verify_state(secret=env.oauth_state_secret, expected_kind="youtube_add_channel_confirm", state=state, require_channel_slug=False)
    nonce = str(payload.get("nonce") or "").strip()
    if not nonce:
        raise HTTPException(400, "invalid oauth state")
    token_json = _read_temp_oauth_token(nonce)
    channels = _youtube_channels_from_token_json(token_json)
    selected = None
    for c in channels:
        if c["id"] == youtube_channel_id:
            selected = c
            break
    if not selected:
        raise HTTPException(400, "invalid youtube channel selection")

    created, slug = _connect_youtube_channel(youtube_channel_id=selected["id"], display_name=selected["title"], token_json=token_json)
    _delete_temp_oauth_token(nonce)
    if not created:
        return HTMLResponse(content="<html><body><h3>Channel already connected</h3><p>This YouTube channel is already connected.</p><p>You can close this tab and refresh dashboard.</p></body></html>")
    return HTMLResponse(content=f"<html><body><h3>Channel connected</h3><p>Connected: {html.escape(selected['title'])} ({html.escape(slug)})</p><p>You can close this tab and refresh dashboard.</p></body></html>")


@app.get("/v1/oauth/youtube/callback", response_class=HTMLResponse)
def api_oauth_youtube_callback(code: str, state: str, _: bool = Depends(require_basic_auth(env))):
    return _oauth_callback("youtube", code, state)


@app.get("/v1/oauth/status")
def api_oauth_status(_: bool = Depends(require_basic_auth(env))):
    _, gdrive_tokens_dir, _ = validate_oauth_config(env, kind="gdrive")
    _, yt_tokens_dir, _ = validate_oauth_config(env, kind="youtube")

    conn = dbm.connect(env)
    try:
        rows = conn.execute(
            "SELECT slug, display_name FROM channels ORDER BY display_name ASC, slug ASC"
        ).fetchall()
    finally:
        conn.close()

    payload = []
    for row in rows:
        slug = str(row["slug"])
        drive_present, drive_mtime = _token_status_for(slug, gdrive_tokens_dir)
        yt_present, yt_mtime = _token_status_for(slug, yt_tokens_dir)
        payload.append(
            {
                "slug": slug,
                "display_name": str(row["display_name"]),
                "drive_token_present": drive_present,
                "drive_token_mtime": drive_mtime,
                "yt_token_present": yt_present,
                "yt_token_mtime": yt_mtime,
            }
        )
    return {"channels": payload}


@app.get("/v1/channels/export/yaml", response_class=PlainTextResponse)
def api_export_channels_yaml(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        rows = conn.execute(
            "SELECT slug, display_name FROM channels ORDER BY display_name ASC, slug ASC"
        ).fetchall()
    finally:
        conn.close()

    payload = {
        "channels": [
            {
                "slug": str(row["slug"]),
                "display_name": str(row["display_name"]),
            }
            for row in rows
        ]
    }
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)




_SLUG_RE = re.compile(r"^[a-z0-9-]{3,64}$")


class CreateChannelPayload(BaseModel):
    slug: str = Field(min_length=3, max_length=64)
    display_name: str = Field(min_length=1, max_length=200)


class UpdateChannelPayload(BaseModel):
    display_name: str = Field(min_length=1, max_length=200)


class DiscoverTrackJobPayload(BaseModel):
    channel_slug: str = Field(min_length=1, max_length=200)


class AnalyzeTrackJobPayload(BaseModel):
    channel_slug: str = Field(min_length=1, max_length=200)
    scope: str = Field(default="pending", min_length=1, max_length=50)
    max_tracks: int = 0
    force: bool = False


class CustomTagCatalogCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str
    label: str
    category: str
    description: str | None = None
    is_active: bool = True


class CustomTagCatalogPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str | None = None
    label: str | None = None
    description: str | None = None
    is_active: bool | None = None
    category: str | None = None


class CustomTagBulkCatalogItemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    category: str
    slug: str
    name: str
    description: str | None = None
    is_active: bool = True


class CustomTagBulkCatalogRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[CustomTagBulkCatalogItemRequest]


class CustomTagBulkBindingsItemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tag_code: str
    channel_slug: str
    is_active: bool


class CustomTagBulkBindingsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[CustomTagBulkBindingsItemRequest]


class CustomTagBulkRulesItemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tag_code: str
    source_path: str
    operator: str
    value_json: str
    priority: int = 100
    weight: float | None = None
    required: bool = False
    stop_after_match: bool = False
    is_active: bool = True
    match_mode: str = "ALL"


class CustomTagBulkRulesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[CustomTagBulkRulesItemRequest]


class CustomTagRuleCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tag_id: int
    source_path: str
    operator: str
    value_json: str
    match_mode: str = "ALL"
    priority: int = 100
    weight: float | None = None
    required: bool = False
    stop_after_match: bool = False
    is_active: bool = True


class CustomTagRulePatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tag_id: int | None = None
    source_path: str | None = None
    operator: str | None = None
    value_json: str | None = None
    match_mode: str | None = None
    priority: int | None = None
    weight: float | None = None
    required: bool | None = None
    stop_after_match: bool | None = None
    is_active: bool | None = None


class CustomTagChannelBindingCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tag_id: int
    channel_slug: str


class CustomTagModalBindingsReplaceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    channel_slugs: list[str]


class CustomTagAssignmentUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tag_id: int | None = None
    tag_code: str | None = None
    category: str | None = None




class CustomTagCloneRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str
    label: str
    description: str | None = None
    include_rules: bool = True
    include_bindings: bool = True
    is_active: bool = True


class CustomTagRulesCloneRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_tag_id: int
    replace_all: bool = False


class CustomTagBulkSetActiveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ids: list[int]
    is_active: bool


class CustomTagBulkBindingsSetEnabledItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tag_id: int
    channel_slug: str
    is_enabled: bool


class CustomTagBulkBindingsSetEnabledRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[CustomTagBulkBindingsSetEnabledItem]


class CustomTagTaxonomyImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: str
    exported_at: str | None = None
    tags: list[dict[str, Any]]
    bindings: list[dict[str, Any]]
    rules: list[dict[str, Any]]

class CustomTagRulePreviewScopeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    channel_slug: str | None = None


class CustomTagRulePreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tag_code: str
    rule: dict[str, Any]
    scope: CustomTagRulePreviewScopeRequest | None = None


class CustomTagReassignScopeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    channel_slug: str | None = None
    tag_code: str | None = None


def _normalize_display_name(value: str) -> str:
    return value.strip()


def _require_track_channel_and_canon(conn, channel_slug: str) -> None:
    existing = dbm.get_channel_by_slug(conn, channel_slug)
    if not existing:
        raise HTTPException(404, "channel not found")

    in_canon_channels = conn.execute(
        "SELECT 1 FROM canon_channels WHERE value = ? LIMIT 1", (channel_slug,)
    ).fetchone()
    in_canon_thresholds = conn.execute(
        "SELECT 1 FROM canon_thresholds WHERE value = ? LIMIT 1", (channel_slug,)
    ).fetchone()
    if in_canon_channels is None or in_canon_thresholds is None:
        raise HTTPException(404, "CHANNEL_NOT_IN_CANON")


def _tar_error(
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    payload: dict[str, Any] = {"error": {"code": code, "message": message}}
    if details:
        payload["error"]["details"] = details
    return JSONResponse(status_code=status_code, content=payload)


@app.get("/v1/track-catalog/analysis-report/channels")
def api_track_analysis_report_channels(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        rows = conn.execute(
            """
            SELECT c.slug, c.display_name
            FROM channels c
            JOIN canon_channels cc ON cc.value = c.slug
            JOIN canon_thresholds ct ON ct.value = c.slug
            ORDER BY c.slug ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return {
        "channels": [
            {
                "channel_slug": str(row["slug"]),
                "display_name": str(row.get("display_name") or ""),
            }
            for row in rows
        ]
    }


@app.get("/v1/track-catalog/analysis-report")
def api_track_analysis_report(
    channel_slug: str = "",
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            return build_channel_report(conn, channel_slug)
        except InvalidChannelSlugError:
            return _tar_error(
                400,
                "TAR_INVALID_CHANNEL",
                "channel_slug is required",
            )
        except ChannelNotFoundError:
            return _tar_error(
                404,
                "TAR_CHANNEL_NOT_FOUND",
                "channel not found",
                details={"channel_slug": str(channel_slug or "")},
            )
        except TrackAnalysisReportError as exc:
            logger.exception("track analysis report build failed: channel_slug=%s", channel_slug)
            return _tar_error(
                500,
                "TAR_REPORT_BUILD_FAILED",
                "failed to build analysis report",
                details={"reason": str(exc)},
            )
        except Exception:
            logger.exception("track analysis report build failed (unexpected): channel_slug=%s", channel_slug)
            return _tar_error(
                500,
                "TAR_REPORT_BUILD_FAILED",
                "failed to build analysis report",
            )
    finally:
        conn.close()


@app.get("/v1/track-catalog/analysis-report.xlsx")
def api_track_analysis_report_xlsx(
    channel_slug: str = "",
    _: bool = Depends(require_basic_auth(env)),
):
    blocked = _disk_guard_write_heavy(operation="track_analysis_report_xlsx")
    if blocked is not None:
        return blocked
    conn = dbm.connect(env)
    try:
        try:
            report = build_channel_report(conn, channel_slug)
            channel_row = conn.execute(
                "SELECT display_name FROM channels WHERE slug = ? LIMIT 1",
                (report["channel_slug"],),
            ).fetchone()
            sheet_source = report["channel_slug"]
            if channel_row is not None and channel_row["display_name"]:
                sheet_source = str(channel_row["display_name"])
            sheet_name = sanitize_sheet_name(sheet_source)
            content = export_report_to_xlsx_bytes(report, sheet_name)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"analysis_report__{report['channel_slug']}__{timestamp}.xlsx"
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )
        except InvalidChannelSlugError:
            return _tar_error(
                400,
                "TAR_INVALID_CHANNEL",
                "channel_slug is required",
            )
        except ChannelNotFoundError:
            return _tar_error(
                404,
                "TAR_CHANNEL_NOT_FOUND",
                "channel not found",
                details={"channel_slug": str(channel_slug or "")},
            )
        except TrackAnalysisReportError as exc:
            logger.exception("track analysis report xlsx build failed: channel_slug=%s", channel_slug)
            return _tar_error(
                500,
                "TAR_REPORT_BUILD_FAILED",
                "failed to build analysis report",
                details={"reason": str(exc)},
            )
        except Exception:
            logger.exception("track analysis report xlsx build failed (unexpected): channel_slug=%s", channel_slug)
            return _tar_error(
                500,
                "TAR_REPORT_BUILD_FAILED",
                "failed to build analysis report",
            )
    finally:
        conn.close()



def _safe_json_loads(raw: Any) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def _track_catalog_row_to_item(row: dict[str, Any]) -> dict[str, Any]:
    feature_payload = _safe_json_loads(row.get("features_payload_json"))
    tag_payload = _safe_json_loads(row.get("tags_payload_json"))
    score_payload = _safe_json_loads(row.get("scores_payload_json"))
    return {
        "track_pk": int(row["track_pk"]),
        "channel_slug": str(row["channel_slug"]),
        "track_id": str(row.get("track_id") or ""),
        "status": "ANALYZED" if row.get("analyzed_at") is not None else "DISCOVERED",
        "title": row.get("title"),
        "artist": row.get("artist"),
        "filename": row.get("filename"),
        "duration_sec": row.get("duration_sec"),
        "discovered_at": row.get("discovered_at"),
        "analyzed_at": row.get("analyzed_at"),
        "features": {
            "scene": feature_payload.get("scene"),
            "mood": feature_payload.get("mood"),
            "raw": feature_payload,
        },
        "tags": {
            "scene": tag_payload.get("scene"),
            "mood": tag_payload.get("mood"),
            "raw": tag_payload,
        },
        "scores": {
            "safety": score_payload.get("safety"),
            "scene_match": score_payload.get("scene_match"),
            "raw": score_payload,
        },
    }


def _passes_track_catalog_filters(
    item: dict[str, Any],
    *,
    status: str,
    scene: str,
    mood: str,
    min_safety: float | None,
    min_scene_match: float | None,
) -> bool:
    item_status = str(item.get("status") or "").strip().upper()
    if status and item_status != status:
        return False

    if scene:
        feature_scene = str((item.get("features") or {}).get("scene") or "").strip().lower()
        tag_scene = str((item.get("tags") or {}).get("scene") or "").strip().lower()
        if feature_scene != scene and tag_scene != scene:
            return False

    if mood:
        feature_mood = str((item.get("features") or {}).get("mood") or "").strip().lower()
        tag_mood = str((item.get("tags") or {}).get("mood") or "").strip().lower()
        if feature_mood != mood and tag_mood != mood:
            return False

    if min_safety is not None:
        safety = (item.get("scores") or {}).get("safety")
        if not isinstance(safety, (int, float)) or float(safety) < min_safety:
            return False

    if min_scene_match is not None:
        scene_match = (item.get("scores") or {}).get("scene_match")
        if not isinstance(scene_match, (int, float)) or float(scene_match) < min_scene_match:
            return False

    return True


@app.get("/v1/track_catalog/channels")
def api_track_catalog_channels(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        rows = conn.execute(
            """
            SELECT c.slug, c.display_name
            FROM channels c
            JOIN canon_channels cc ON cc.value = c.slug
            JOIN canon_thresholds ct ON ct.value = c.slug
            ORDER BY c.slug ASC
            """
        ).fetchall()
    finally:
        conn.close()
    return {
        "channels": [
            {
                "slug": str(row["slug"]),
                "display_name": str(row.get("display_name") or ""),
            }
            for row in rows
        ]
    }



@app.post("/v1/track_catalog/{channel_slug}/enable")
def api_track_catalog_enable(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        existing = dbm.get_channel_by_slug(conn, channel_slug)
        if not existing:
            raise HTTPException(404, "channel not found")
        dbm.enable_track_catalog_for_channel(conn, channel_slug)
    finally:
        conn.close()
    return {"ok": True, "channel_slug": channel_slug, "track_catalog_enabled": True}


@app.delete("/v1/track_catalog/{channel_slug}/enable")
def api_track_catalog_disable(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        existing = dbm.get_channel_by_slug(conn, channel_slug)
        if not existing:
            raise HTTPException(404, "channel not found")
        dbm.disable_track_catalog_for_channel(conn, channel_slug)
    finally:
        conn.close()
    return {"ok": True, "channel_slug": channel_slug, "track_catalog_enabled": False}


@app.get("/v1/track_catalog/{channel_slug}/tracks")
def api_track_catalog_tracks(
    channel_slug: str,
    status: str = "",
    scene: str = "",
    mood: str = "",
    min_safety: float | None = None,
    min_scene_match: float | None = None,
    _: bool = Depends(require_basic_auth(env)),
):
    normalized_status = status.strip().upper()
    normalized_scene = scene.strip().lower()
    normalized_mood = mood.strip().lower()

    conn = dbm.connect(env)
    try:
        _require_track_channel_and_canon(conn, channel_slug)
        rows = conn.execute(
            """
            SELECT
                t.id AS track_pk,
                t.channel_slug,
                t.track_id,
                t.filename,
                t.title,
                t.artist,
                t.duration_sec,
                t.discovered_at,
                t.analyzed_at,
                tf.payload_json AS features_payload_json,
                tt.payload_json AS tags_payload_json,
                ts.payload_json AS scores_payload_json
            FROM tracks t
            LEFT JOIN track_features tf ON tf.track_pk = t.id
            LEFT JOIN track_tags tt ON tt.track_pk = t.id
            LEFT JOIN track_scores ts ON ts.track_pk = t.id
            WHERE t.channel_slug = ?
            ORDER BY t.id ASC
            """,
            (channel_slug,),
        ).fetchall()
    finally:
        conn.close()

    tracks = []
    for row in rows:
        item = _track_catalog_row_to_item(row)
        if _passes_track_catalog_filters(
            item,
            status=normalized_status,
            scene=normalized_scene,
            mood=normalized_mood,
            min_safety=min_safety,
            min_scene_match=min_scene_match,
        ):
            tracks.append(item)
    return {"channel_slug": channel_slug, "tracks": tracks}


@app.get("/v1/track_catalog/tracks/{track_pk}")
def api_track_catalog_track_detail(track_pk: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        row = conn.execute(
            """
            SELECT
                t.id AS track_pk,
                t.channel_slug,
                t.track_id,
                t.filename,
                t.title,
                t.artist,
                t.duration_sec,
                t.discovered_at,
                t.analyzed_at,
                tf.payload_json AS features_payload_json,
                tt.payload_json AS tags_payload_json,
                ts.payload_json AS scores_payload_json
            FROM tracks t
            LEFT JOIN track_features tf ON tf.track_pk = t.id
            LEFT JOIN track_tags tt ON tt.track_pk = t.id
            LEFT JOIN track_scores ts ON ts.track_pk = t.id
            WHERE t.id = ?
            LIMIT 1
            """,
            (track_pk,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        raise HTTPException(404, "track not found")
    return {"track": _track_catalog_row_to_item(row)}



def _cta_error_response(
    err: catalog_service.CatalogError
    | rules_service.RulesError
    | assignment_service.AssignmentError
    | bulk_bindings_service.BulkBindingsError
    | bulk_rules_service.BulkRulesError
    | reassign_service.ReassignError
    | taxonomy_service.TaxonomyError,
) -> JSONResponse:
    return JSONResponse(
        status_code=err.status_code,
        content={"error": {"code": err.code, "message": err.message, "details": err.details or {}}},
    )


def _ctu_invalid_payload_response(message: str, details: dict[str, Any] | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={"error": {"code": "CTU_INVALID_PAYLOAD", "message": message, "details": details or {}}},
    )


_CTA_CATALOG_ENDPOINTS = {
    ("GET", "/v1/track-catalog/custom-tags"),
    ("GET", "/v1/track-catalog/custom-tags/catalog"),
    ("POST", "/v1/track-catalog/custom-tags/catalog"),
    ("PATCH", "/v1/track-catalog/custom-tags/catalog/{tag_id}"),
    ("POST", "/v1/track-catalog/custom-tags/catalog/import"),
    ("POST", "/v1/track-catalog/custom-tags/catalog/export"),
    ("POST", "/v1/track-catalog/custom-tags/bulk/preview"),
    ("POST", "/v1/track-catalog/custom-tags/bulk/confirm"),
    ("POST", "/v1/track-catalog/custom-tags/bulk-bindings/preview"),
    ("POST", "/v1/track-catalog/custom-tags/bulk-bindings/confirm"),
    ("POST", "/v1/track-catalog/custom-tags/bulk-rules/preview"),
    ("POST", "/v1/track-catalog/custom-tags/bulk-rules/confirm"),
    ("POST", "/v1/track-catalog/custom-tags/export-seed"),
    ("POST", "/v1/track-catalog/custom-tags/import-seed"),
    ("GET", "/v1/track-catalog/custom-tags/rules"),
    ("POST", "/v1/track-catalog/custom-tags/rules"),
    ("PATCH", "/v1/track-catalog/custom-tags/rules/{rule_id}"),
    ("DELETE", "/v1/track-catalog/custom-tags/rules/{rule_id}"),
    ("GET", "/v1/track-catalog/custom-tags/channel-bindings"),
    ("POST", "/v1/track-catalog/custom-tags/channel-bindings"),
    ("DELETE", "/v1/track-catalog/custom-tags/channel-bindings/{binding_id}"),
    ("GET", "/v1/track-catalog/custom-tags/{tag_id}/rules"),
    ("POST", "/v1/track-catalog/custom-tags/{tag_id}/rules"),
    ("PATCH", "/v1/track-catalog/custom-tags/{tag_id}/rules/{rule_id}"),
    ("DELETE", "/v1/track-catalog/custom-tags/{tag_id}/rules/{rule_id}"),
    ("PUT", "/v1/track-catalog/custom-tags/{tag_id}/rules/replace-all"),
    ("GET", "/v1/track-catalog/custom-tags/{tag_id}/bindings"),
    ("PUT", "/v1/track-catalog/custom-tags/{tag_id}/bindings"),
    ("GET", "/v1/track-catalog/tracks/{track_pk}/custom-tags"),
    ("POST", "/v1/track-catalog/tracks/{track_pk}/custom-tags"),
    ("DELETE", "/v1/track-catalog/tracks/{track_pk}/custom-tags/{tag_id}"),
    ("POST", "/v1/track-catalog/custom-tags/rules/preview-matches"),
    ("POST", "/v1/track-catalog/custom-tags/reassign/preview"),
    ("POST", "/v1/track-catalog/custom-tags/reassign/execute"),
    ("POST", "/v1/track-catalog/custom-tags/{tag_id}/clone"),
    ("POST", "/v1/track-catalog/custom-tags/{tag_id}/rules/clone"),
    ("POST", "/v1/track-catalog/custom-tags/tags/bulk-set-active"),
    ("POST", "/v1/track-catalog/custom-tags/rules/bulk-set-active"),
    ("POST", "/v1/track-catalog/custom-tags/bindings/bulk-set-enabled"),
    ("GET", "/v1/track-catalog/custom-tags/taxonomy/export"),
    ("POST", "/v1/track-catalog/custom-tags/taxonomy/import/preview"),
    ("POST", "/v1/track-catalog/custom-tags/taxonomy/import/confirm"),
    ("GET", "/v1/track-catalog/custom-tags/dashboard/{channel_slug}"),
}


def _is_cta_catalog_route_validation_error(request: Request) -> bool:
    route = request.scope.get("route")
    route_path = getattr(route, "path", None)
    method = request.method.upper()
    return isinstance(route_path, str) and (method, route_path) in _CTA_CATALOG_ENDPOINTS


@app.exception_handler(RequestValidationError)
async def _handle_request_validation_error(request: Request, exc: RequestValidationError):
    if _is_cta_catalog_route_validation_error(request):
        return JSONResponse(
            status_code=400,
            content={
                "error": {
                    "code": "CTA_INVALID_INPUT",
                    "message": "invalid request payload",
                    "details": {"errors": exc.errors()},
                }
            },
        )
    return await request_validation_exception_handler(request, exc)




@app.get("/v1/track-catalog/custom-tags")
def api_custom_tags_listing(
    category: str | None = None,
    tag_id: str | None = None,
    q: str | None = None,
    include_bindings: bool = True,
    include_rules_summary: bool = True,
    include_usage: bool = False,
    _: bool = Depends(require_basic_auth(env)),
):
    category_norm = category.strip().upper() if isinstance(category, str) and category.strip() else None
    if tag_id is not None:
        if not str(tag_id).isdigit():
            return _cta_error_response(catalog_service.InvalidInputError("tag_id must be numeric", {"field": "tag_id"}))
        tag_id_norm = int(tag_id)
    else:
        tag_id_norm = None

    conn = dbm.connect(env)
    try:
        try:
            tags = catalog_service.list_custom_tags_enriched(
                conn,
                category=category_norm,
                tag_id=tag_id_norm,
                q=q,
                include_bindings=include_bindings,
                include_rules_summary=include_rules_summary,
                include_usage=include_usage,
            )
        except catalog_service.CatalogError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags enriched list failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"tags": tags}

@app.get("/v1/track-catalog/custom-tags/catalog")
def api_custom_tags_catalog(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        tags = catalog_service.list_catalog(conn)
    finally:
        conn.close()
    return {"tags": tags}


@app.post("/v1/track-catalog/custom-tags/catalog")
def api_custom_tags_catalog_create(payload: CustomTagCatalogCreateRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            tag = catalog_service.create_tag(conn, payload.model_dump())
        except catalog_service.CatalogError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags create failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"tag": tag}


@app.patch("/v1/track-catalog/custom-tags/catalog/{tag_id}")
def api_custom_tags_catalog_patch(tag_id: int, payload: CustomTagCatalogPatchRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            tag = catalog_service.update_tag(conn, tag_id=tag_id, payload=payload.model_dump(exclude_none=True))
        except catalog_service.CatalogError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags patch failed", extra={"tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"tag": tag}


@app.post("/v1/track-catalog/custom-tags/catalog/import")
def api_custom_tags_catalog_import(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = catalog_service.import_catalog(conn, seed_dir=env.custom_tags_seed_dir)
        except catalog_service.CatalogError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags import failed", extra={"seed_dir": env.custom_tags_seed_dir})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"ok": True, **result}


@app.post("/v1/track-catalog/custom-tags/catalog/export")
def api_custom_tags_catalog_export(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = catalog_service.export_catalog(conn, seed_dir=env.custom_tags_seed_dir)
        except catalog_service.CatalogError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags export failed", extra={"seed_dir": env.custom_tags_seed_dir})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"ok": True, **result}


@app.post("/v1/track-catalog/custom-tags/bulk/preview")
def api_custom_tags_catalog_bulk_preview(payload: CustomTagBulkCatalogRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = catalog_service.preview_bulk_custom_tags(conn, items=[item.model_dump() for item in payload.items])
        except catalog_service.CatalogError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags bulk preview failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/bulk/confirm")
def api_custom_tags_catalog_bulk_confirm(payload: CustomTagBulkCatalogRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = catalog_service.confirm_bulk_custom_tags(conn, items=[item.model_dump() for item in payload.items])
        except catalog_service.CatalogError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags bulk confirm failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result






@app.post("/v1/track-catalog/custom-tags/bulk-bindings/preview")
def api_custom_tags_bulk_bindings_preview(payload: CustomTagBulkBindingsRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = bulk_bindings_service.preview_bulk_bindings(conn, items=[item.model_dump() for item in payload.items])
        except bulk_bindings_service.BulkBindingsError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags bulk bindings preview failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/bulk-bindings/confirm")
def api_custom_tags_bulk_bindings_confirm(payload: CustomTagBulkBindingsRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = bulk_bindings_service.confirm_bulk_bindings(conn, items=[item.model_dump() for item in payload.items])
        except bulk_bindings_service.BulkBindingsError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags bulk bindings confirm failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/bulk-rules/preview")
def api_custom_tags_bulk_rules_preview(payload: CustomTagBulkRulesRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = bulk_rules_service.preview_bulk_rules(conn, items=[item.model_dump() for item in payload.items])
        except bulk_rules_service.BulkRulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags bulk rules preview failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/bulk-rules/confirm")
def api_custom_tags_bulk_rules_confirm(payload: CustomTagBulkRulesRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = bulk_rules_service.confirm_bulk_rules(conn, items=[item.model_dump() for item in payload.items])
        except bulk_rules_service.BulkRulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags bulk rules confirm failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/export-seed")
def api_custom_tags_export_seed(_: bool = Depends(require_basic_auth(env))):
    return api_custom_tags_catalog_export(_)


@app.post("/v1/track-catalog/custom-tags/import-seed")
def api_custom_tags_import_seed(_: bool = Depends(require_basic_auth(env))):
    return api_custom_tags_catalog_import(_)


@app.post("/v1/track-catalog/custom-tags/rules/preview-matches")
def api_custom_tags_rule_preview_matches(payload: CustomTagRulePreviewRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = reassign_service.preview_rule_matches(
                conn,
                tag_code=payload.tag_code,
                rule=payload.rule,
                channel_slug=payload.scope.channel_slug if payload.scope is not None else None,
            )
        except reassign_service.ReassignError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags rule preview matches failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/reassign/preview")
def api_custom_tags_reassign_preview(payload: CustomTagReassignScopeRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = reassign_service.preview_reassign(conn, channel_slug=payload.channel_slug, tag_code=payload.tag_code)
        except reassign_service.ReassignError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags reassign preview failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/reassign/execute")
def api_custom_tags_reassign_execute(payload: CustomTagReassignScopeRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = reassign_service.execute_reassign(conn, channel_slug=payload.channel_slug, tag_code=payload.tag_code)
        except reassign_service.ReassignError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags reassign execute failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result



@app.post("/v1/track-catalog/custom-tags/{tag_id}/clone")
def api_custom_tag_clone(tag_id: int, payload: CustomTagCloneRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.clone_tag(conn, source_tag_id=tag_id, **payload.model_dump())
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags clone failed", extra={"tag_id": tag_id})
            return JSONResponse(status_code=500, content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}})
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/{tag_id}/rules/clone")
def api_custom_tag_rules_clone(tag_id: int, payload: CustomTagRulesCloneRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.clone_rules(conn, source_tag_id=payload.source_tag_id, target_tag_id=tag_id, replace_all=payload.replace_all)
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags rules clone failed", extra={"tag_id": tag_id})
            return JSONResponse(status_code=500, content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}})
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/tags/bulk-set-active")
def api_custom_tags_bulk_set_active(payload: CustomTagBulkSetActiveRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.bulk_set_tags_active(conn, tag_ids=payload.ids, is_active=payload.is_active)
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/rules/bulk-set-active")
def api_custom_tag_rules_bulk_set_active(payload: CustomTagBulkSetActiveRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.bulk_set_rules_active(conn, rule_ids=payload.ids, is_active=payload.is_active)
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/bindings/bulk-set-enabled")
def api_custom_tag_bindings_bulk_set_enabled(payload: CustomTagBulkBindingsSetEnabledRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.bulk_set_bindings_enabled(conn, items=[item.model_dump() for item in payload.items])
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
    finally:
        conn.close()
    return result


@app.get("/v1/track-catalog/custom-tags/taxonomy/export")
def api_custom_tags_taxonomy_export(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.export_taxonomy(conn)
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/taxonomy/import/preview")
def api_custom_tags_taxonomy_import_preview(payload: CustomTagTaxonomyImportRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.import_taxonomy_preview(conn, payload=payload.model_dump())
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
    finally:
        conn.close()
    return result


@app.post("/v1/track-catalog/custom-tags/taxonomy/import/confirm")
def api_custom_tags_taxonomy_import_confirm(payload: CustomTagTaxonomyImportRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.import_taxonomy_confirm(conn, payload=payload.model_dump())
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
    finally:
        conn.close()
    return result


@app.get("/v1/track-catalog/custom-tags/dashboard/{channel_slug}")
def api_custom_tags_dashboard(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = taxonomy_service.channel_dashboard(conn, channel_slug=channel_slug)
        except taxonomy_service.TaxonomyError as err:
            return _cta_error_response(err)
    finally:
        conn.close()
    return result

@app.get("/v1/track-catalog/custom-tags/rules")
def api_custom_tag_rules(tag_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            rules = rules_service.list_rules(conn, tag_id=tag_id)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags rules list failed", extra={"tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"rules": rules}


@app.post("/v1/track-catalog/custom-tags/rules")
def api_custom_tag_rules_create(payload: CustomTagRuleCreateRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            rule = rules_service.create_rule(conn, payload=payload.model_dump())
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags rules create failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"rule": rule}


@app.patch("/v1/track-catalog/custom-tags/rules/{rule_id}")
def api_custom_tag_rules_patch(rule_id: int, payload: CustomTagRulePatchRequest, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            rule = rules_service.update_rule(conn, rule_id=rule_id, payload=payload.model_dump(exclude_none=True))
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags rules patch failed", extra={"rule_id": rule_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"rule": rule}


@app.delete("/v1/track-catalog/custom-tags/rules/{rule_id}")
def api_custom_tag_rules_delete(rule_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            rules_service.delete_rule(conn, rule_id=rule_id)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags rules delete failed", extra={"rule_id": rule_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"ok": True}


@app.get("/v1/track-catalog/custom-tags/channel-bindings")
def api_custom_tag_channel_bindings(tag_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            bindings = rules_service.list_channel_bindings(conn, tag_id=tag_id)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags channel bindings list failed", extra={"tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"bindings": bindings}


@app.get("/v1/track-catalog/custom-tags/bindings/by-channel/{channel_slug}")
def api_custom_tag_channel_bindings_by_channel(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            bindings = rules_service.list_bindings_by_channel(conn, channel_slug=channel_slug)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags channel bindings by channel failed", extra={"channel_slug": channel_slug})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"channel_slug": channel_slug, "bindings": bindings}


@app.post("/v1/track-catalog/custom-tags/channel-bindings")
def api_custom_tag_channel_bindings_create(
    payload: CustomTagChannelBindingCreateRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            binding = rules_service.create_channel_binding(conn, payload=payload.model_dump())
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags channel bindings create failed")
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"binding": binding}


@app.delete("/v1/track-catalog/custom-tags/channel-bindings/{binding_id}")
def api_custom_tag_channel_bindings_delete(binding_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            rules_service.delete_channel_binding(conn, binding_id=binding_id)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags channel bindings delete failed", extra={"binding_id": binding_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"ok": True}


@app.get("/v1/track-catalog/custom-tags/{tag_id}/rules")
def api_custom_tag_rules_modal(tag_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            rules = rules_service.list_rules_for_modal(conn, tag_id=tag_id)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags modal rules list failed", extra={"tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"rules": rules}


@app.post("/v1/track-catalog/custom-tags/{tag_id}/rules")
def api_custom_tag_rules_modal_create(tag_id: int, payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            rule = rules_service.create_rule_for_modal(conn, tag_id=tag_id, payload=payload)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags modal rules create failed", extra={"tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"rule": rule}


@app.patch("/v1/track-catalog/custom-tags/{tag_id}/rules/{rule_id}")
def api_custom_tag_rules_modal_patch(
    tag_id: int,
    rule_id: int,
    payload: dict[str, Any],
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            rule = rules_service.update_rule_for_modal(conn, tag_id=tag_id, rule_id=rule_id, payload=payload)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags modal rules patch failed", extra={"tag_id": tag_id, "rule_id": rule_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"rule": rule}


@app.delete("/v1/track-catalog/custom-tags/{tag_id}/rules/{rule_id}")
def api_custom_tag_rules_modal_delete(tag_id: int, rule_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            rules_service.delete_rule_for_modal(conn, tag_id=tag_id, rule_id=rule_id)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags modal rules delete failed", extra={"tag_id": tag_id, "rule_id": rule_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"ok": True}


@app.put("/v1/track-catalog/custom-tags/{tag_id}/rules/replace-all")
def api_custom_tag_rules_modal_replace_all(tag_id: int, payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
    rules_payload = payload.get("rules")
    if not isinstance(rules_payload, list):
        return _ctu_invalid_payload_response("rules must be a list", {"field": "rules"})
    conn = dbm.connect(env)
    try:
        try:
            rules = rules_service.replace_all_rules_for_modal(conn, tag_id=tag_id, rules=rules_payload)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags modal rules replace-all failed", extra={"tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"rules": rules}


@app.get("/v1/track-catalog/custom-tags/{tag_id}/bindings")
def api_custom_tag_bindings_modal(tag_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            bindings = rules_service.list_bindings_for_modal(conn, tag_id=tag_id)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags modal bindings list failed", extra={"tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"bindings": bindings}


@app.put("/v1/track-catalog/custom-tags/{tag_id}/bindings")
def api_custom_tag_bindings_modal_replace(
    tag_id: int,
    payload: CustomTagModalBindingsReplaceRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        try:
            bindings = rules_service.replace_bindings_for_modal(conn, tag_id=tag_id, channel_slugs=payload.channel_slugs)
        except rules_service.RulesError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags modal bindings replace failed", extra={"tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return {"bindings": bindings}


@app.get("/v1/track-catalog/tracks/{track_pk}/custom-tags")
def api_track_custom_tag_assignments(track_pk: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            return assignment_service.get_track_custom_tags(conn, track_pk=track_pk)
        except assignment_service.AssignmentError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags assignments get failed", extra={"track_pk": track_pk})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()


@app.post("/v1/track-catalog/tracks/{track_pk}/custom-tags")
def api_track_custom_tag_assignments_upsert(
    track_pk: int,
    payload: CustomTagAssignmentUpsertRequest,
    _: bool = Depends(require_basic_auth(env)),
):
    tag_id = payload.tag_id
    tag_code = payload.tag_code
    category = payload.category
    has_tag_id = tag_id is not None
    has_code_selector = tag_code is not None or category is not None
    if has_tag_id and has_code_selector:
        return _cta_error_response(assignment_service.InvalidInputError("provide either tag_id or tag_code+category"))
    if not has_tag_id and not has_code_selector:
        return _cta_error_response(assignment_service.InvalidInputError("tag selector is required"))
    if has_code_selector and (tag_code is None or category is None):
        return _cta_error_response(assignment_service.InvalidInputError("tag_code and category must be provided together"))

    conn = dbm.connect(env)
    try:
        try:
            result = assignment_service.upsert_manual_assignment(
                conn,
                track_pk=track_pk,
                tag_id=tag_id,
                tag_code=tag_code,
                category=category,
            )
        except assignment_service.AssignmentError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags assignments upsert failed", extra={"track_pk": track_pk})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.delete("/v1/track-catalog/tracks/{track_pk}/custom-tags/{tag_id}")
def api_track_custom_tag_assignments_delete(track_pk: int, tag_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        try:
            result = assignment_service.suppress_assignment(conn, track_pk=track_pk, tag_id=tag_id)
        except assignment_service.AssignmentError as err:
            return _cta_error_response(err)
        except Exception:
            logger.exception("custom-tags assignments delete failed", extra={"track_pk": track_pk, "tag_id": tag_id})
            return JSONResponse(
                status_code=500,
                content={"error": {"code": "CTA_INTERNAL", "message": "internal error", "details": {}}},
            )
    finally:
        conn.close()
    return result


@app.post("/v1/channels")
def api_create_channel(payload: CreateChannelPayload, _: bool = Depends(require_basic_auth(env))):
    slug = payload.slug.strip()
    if not _SLUG_RE.fullmatch(slug):
        raise HTTPException(422, "slug must match ^[a-z0-9-]{3,64}$")

    display_name = _normalize_display_name(payload.display_name)
    if not display_name:
        raise HTTPException(422, "display_name must be between 1 and 200 characters")

    conn = dbm.connect(env)
    try:
        existing = dbm.get_channel_by_slug(conn, slug)
        if existing:
            raise HTTPException(409, "channel slug already exists")
        created = dbm.create_channel(conn, slug=slug, display_name=display_name)
    finally:
        conn.close()
    return created


@app.patch("/v1/channels/{slug}")
def api_update_channel(slug: str, payload: UpdateChannelPayload, _: bool = Depends(require_basic_auth(env))):
    display_name = _normalize_display_name(payload.display_name)
    if not display_name:
        raise HTTPException(422, "display_name must be between 1 and 200 characters")

    conn = dbm.connect(env)
    try:
        existing = dbm.get_channel_by_slug(conn, slug)
        if not existing:
            raise HTTPException(404, "channel not found")
        updated = dbm.update_channel_display_name(conn, slug=slug, display_name=display_name)
    finally:
        conn.close()

    assert updated is not None
    return updated


@app.delete("/v1/channels/{slug}")
def api_delete_channel(slug: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        existing = dbm.get_channel_by_slug(conn, slug)
        if not existing:
            raise HTTPException(404, "channel not found")

        channel_id = int(existing["id"])
        if dbm.channel_has_jobs(conn, channel_id):
            raise HTTPException(409, "cannot delete channel: jobs exist for this channel")

        deleted = dbm.delete_channel_by_slug(conn, slug)
        if deleted == 0:
            raise HTTPException(404, "channel not found")
    finally:
        conn.close()

    return {"ok": True, "slug": slug}


@app.post("/v1/track_jobs/discover", status_code=202)
def api_track_jobs_discover(payload: DiscoverTrackJobPayload, _: bool = Depends(require_basic_auth(env))):
    channel_slug = payload.channel_slug.strip()
    conn = dbm.connect(env)
    try:
        _require_track_channel_and_canon(conn, channel_slug)
        if track_jobs_db.has_already_running(conn, job_type="SCAN_TRACKS", channel_slug=channel_slug):
            raise HTTPException(409, "TRACK_JOB_ALREADY_RUNNING")
        job_id = track_jobs_db.enqueue_job(conn, job_type="SCAN_TRACKS", channel_slug=channel_slug, payload={})
    finally:
        conn.close()
    return {"job_id": str(job_id), "status": "QUEUED"}


@app.post("/v1/track_jobs/analyze", status_code=202)
def api_track_jobs_analyze(payload: AnalyzeTrackJobPayload, _: bool = Depends(require_basic_auth(env))):
    blocked = _disk_guard_write_heavy(operation="track_jobs_analyze")
    if blocked is not None:
        return blocked
    channel_slug = payload.channel_slug.strip()
    conn = dbm.connect(env)
    try:
        _require_track_channel_and_canon(conn, channel_slug)
        if track_jobs_db.has_already_running(conn, job_type="ANALYZE_TRACKS", channel_slug=channel_slug):
            raise HTTPException(409, "TRACK_JOB_ALREADY_RUNNING")
        job_id = track_jobs_db.enqueue_job(
            conn,
            job_type="ANALYZE_TRACKS",
            channel_slug=channel_slug,
            payload={
                "scope": payload.scope,
                "max_tracks": int(payload.max_tracks),
                "force": bool(payload.force),
            },
        )
    finally:
        conn.close()
    return {"job_id": str(job_id), "status": "QUEUED"}


@app.get("/v1/track_jobs/{job_id}")
def api_track_job_get(job_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        job = track_jobs_db.get_job(conn, job_id)
    finally:
        conn.close()

    if not job:
        raise HTTPException(404)

    payload_json = str(job.get("payload_json") or "")
    try:
        payload = json.loads(payload_json) if payload_json else {}
    except Exception:
        payload = {}
    return {
        "job": {
            "id": int(job["id"]),
            "job_type": str(job["job_type"]),
            "channel_slug": job.get("channel_slug"),
            "status": str(job["status"]),
            "payload": payload,
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at"),
        }
    }


@app.get("/v1/track_jobs/{job_id}/logs")
def api_track_job_logs(job_id: int, tail: int = 200, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        job = track_jobs_db.get_job(conn, job_id)
        if not job:
            raise HTTPException(404)
        logs = track_jobs_db.list_logs(conn, job_id=job_id, tail=tail)
    finally:
        conn.close()

    return {
        "job_id": job_id,
        "logs": [
            {
                "id": int(row["id"]),
                "level": row.get("level"),
                "message": str(row["message"]),
                "ts": row.get("ts"),
            }
            for row in logs
        ],
    }


class ApprovePayload(BaseModel):
    comment: str = Field(default="approved", max_length=500)


class RejectPayload(BaseModel):
    comment: str = Field(min_length=1, max_length=1000)




class CancelPayload(BaseModel):
    reason: str = Field(default='cancelled by user', max_length=500)


class RecoveryActionPayload(BaseModel):
    reason: str = Field(default="operator action", max_length=500)
    confirm: bool = False


class UiJobDraftPayload(BaseModel):
    channel_id: int
    title: str
    description: str = ""
    tags_csv: str = ""
    cover_name: str = ""
    cover_ext: str = ""
    background_name: str
    background_ext: str
    audio_ids_text: str


class UiPlaylistBuilderDraftPayload(BaseModel):
    channel_id: int
    title: str
    description: str = ""
    tags_csv: str = ""
    cover_name: str = ""
    cover_ext: str = ""
    background_name: str = ""
    background_ext: str = ""


class UiJobsRenderSelectedPayload(BaseModel):
    job_ids: Optional[list[str]] = None


class UiJobsBulkJsonPayload(BaseModel):
    mode: str
    items: list[dict[str, Any]]


def _ui_validate(payload: UiJobDraftPayload) -> Dict[str, List[str]]:
    errors: Dict[str, List[str]] = {
        "project": [],
        "title": [],
        "audio": [],
        "background": [],
        "cover": [],
        "tags": [],
    }
    if payload.channel_id <= 0:
        errors["project"].append("project is required")
    if not payload.title.strip():
        errors["title"].append("title is required")
    if not payload.audio_ids_text.strip():
        errors["audio"].append("audio ids are required")
    if not payload.background_name.strip() or not payload.background_ext.strip():
        errors["background"].append("background name/ext are required")
    if "#" in payload.tags_csv:
        errors["tags"].append("tags must not contain #")
    return {k: v for k, v in errors.items() if v}


def _ui_validate_playlist_builder_draft(payload: UiPlaylistBuilderDraftPayload) -> Dict[str, List[str]]:
    errors: Dict[str, List[str]] = {
        "project": [],
        "title": [],
        "tags": [],
    }
    if payload.channel_id <= 0:
        errors["project"].append("project is required")
    if not payload.title.strip():
        errors["title"].append("title is required")
    if "#" in payload.tags_csv:
        errors["tags"].append("tags must not contain #")
    return {k: v for k, v in errors.items() if v}


def _bulk_json_invalid(message: str) -> JSONResponse:
    return _uij_error(400, "UIJ_BULK_INVALID_INPUT", message)


_RECOVERY_FAIL_STATES = {"FAILED", "RENDER_FAILED", "QA_FAILED", "UPLOAD_FAILED"}
_RECOVERY_STALE_STATES = {"FETCHING_INPUTS", "RENDERING"}
_RECOVERY_ACTIONABLE = {"retryable", "cancellable", "reclaimable", "cleanupable", "restartable"}


def _recovery_audit_path() -> Path:
    return Path(env.storage_root).resolve() / "logs" / "recovery_audit.jsonl"


def _append_recovery_audit(entry: dict[str, Any]) -> None:
    path = _recovery_audit_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")


def _load_recovery_audit(limit: int = 50) -> list[dict[str, Any]]:
    path = _recovery_audit_path()
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    records: list[dict[str, Any]] = []
    for line in reversed(lines):
        if len(records) >= limit:
            break
        try:
            rec = json.loads(line)
        except Exception:
            continue
        if isinstance(rec, dict):
            records.append(rec)
    return records


def _build_recovery_job(job: dict[str, Any], *, now_ts: float, lock_ttl_sec: int) -> dict[str, Any]:
    state = str(job.get("state") or "")
    error_reason = str(job.get("error_reason") or "")
    retry_child_exists = bool(job.get("retry_child_job_id"))
    locked_at_raw = job.get("locked_at")
    locked_at = float(locked_at_raw) if locked_at_raw is not None else None
    stale_locked = bool(
        state in _RECOVERY_STALE_STATES
        and job.get("locked_by")
        and locked_at is not None
        and locked_at < (now_ts - float(lock_ttl_sec))
    )
    cleanup_pending = bool(state == "PUBLISHED" and job.get("delete_mp4_at") is not None)
    artifact_issue = any(token in error_reason.lower() for token in ("artifact", "mp4", "missing", "cleanup"))

    actions = {
        "retryable": bool(state in _RECOVERY_FAIL_STATES and not retry_child_exists),
        "cancellable": bool(state not in {"PUBLISHED", "REJECTED", "APPROVED", "CANCELLED", "CLEANED"}),
        "reclaimable": bool(stale_locked),
        "cleanupable": bool(state in _RECOVERY_FAIL_STATES or cleanup_pending or artifact_issue),
        "restartable": bool(state == "DRAFT"),
    }
    issue_flags = {
        "failed": bool(state in _RECOVERY_FAIL_STATES),
        "stale_or_stuck": bool(stale_locked),
        "cleanup_pending": bool(cleanup_pending),
        "artifact_issue": bool(artifact_issue),
    }
    return {
        "id": int(job["id"]),
        "channel_slug": job.get("channel_slug"),
        "channel_name": job.get("channel_name"),
        "release_title": job.get("release_title"),
        "state": state,
        "stage": job.get("stage"),
        "updated_at": job.get("updated_at"),
        "error_reason": error_reason,
        "locked_by": job.get("locked_by"),
        "locked_at": locked_at_raw,
        "retry_child_job_id": job.get("retry_child_job_id"),
        "issue_flags": issue_flags,
        "actions": actions,
    }


def _list_recovery_jobs(conn: Any) -> list[dict[str, Any]]:
    jobs = dbm.list_jobs(conn, limit=500)
    job_ids = [int(job["id"]) for job in jobs]
    retry_child_by_parent_id: dict[int, int] = {}
    if job_ids:
        placeholders = ",".join("?" for _ in job_ids)
        rows = conn.execute(
            f"SELECT id, retry_of_job_id FROM jobs WHERE retry_of_job_id IN ({placeholders})",
            job_ids,
        ).fetchall()
        retry_child_by_parent_id = {
            int(row["retry_of_job_id"]): int(row["id"])
            for row in rows
            if row.get("retry_of_job_id") is not None
        }

    now = dbm.now_ts()
    recovery_jobs = []
    for job in jobs:
        job["retry_child_job_id"] = retry_child_by_parent_id.get(int(job["id"]))
        payload = _build_recovery_job(job, now_ts=now, lock_ttl_sec=env.job_lock_ttl_sec)
        if any(payload["issue_flags"].values()) or any(payload["actions"].values()):
            recovery_jobs.append(payload)
    return recovery_jobs


def _force_cleanup_job_artifacts(job_id: int) -> dict[str, Any]:
    removed: list[str] = []
    missing: list[str] = []
    targets = [
        workspace_dir(env, job_id),
        outbox_dir(env, job_id),
        preview_path(env, job_id),
        qa_path(env, job_id),
    ]
    for target in targets:
        if not target.exists():
            missing.append(str(target))
            continue
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        removed.append(str(target))
    return {"removed": removed, "missing": missing}


def _record_recovery_action(*, job_id: int, action: str, reason: str, result: str, details: dict[str, Any] | None = None) -> None:
    _append_recovery_audit(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "actor": env.basic_user,
            "job_id": int(job_id),
            "action": action,
            "reason": reason,
            "result": result,
            "details": details or {},
        }
    )


def _manual_reclaim_job(conn: Any, *, job_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT id, state, locked_by, locked_at FROM jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "job not found")
    state = str(row.get("state") or "")
    locked_by = row.get("locked_by")
    locked_at = row.get("locked_at")
    now = dbm.now_ts()
    stale_before = now - float(env.job_lock_ttl_sec)
    if state not in _RECOVERY_STALE_STATES or not locked_by or locked_at is None or float(locked_at) >= stale_before:
        raise HTTPException(409, "job is not reclaimable")

    attempt = dbm.increment_attempt(conn, job_id)
    reclaim_reason = f"reclaimed stale lock from {state}"
    if attempt < env.max_render_attempts:
        dbm.schedule_retry(
            conn,
            job_id,
            next_state="READY_FOR_RENDER",
            stage="FETCH",
            error_reason=f"attempt={attempt} retry: {reclaim_reason}",
            backoff_sec=env.retry_backoff_sec,
        )
        next_state = "READY_FOR_RENDER"
    else:
        dbm.update_job_state(
            conn,
            job_id,
            state="RENDER_FAILED",
            stage="RENDER",
            error_reason=f"attempt={attempt} terminal: {reclaim_reason}",
        )
        dbm.clear_retry(conn, job_id)
        dbm.force_unlock(conn, job_id)
        next_state = "RENDER_FAILED"
    return {"attempt": attempt, "next_state": next_state}


def _validate_create_item(conn: Any, item: dict[str, Any]) -> tuple[UiJobDraftPayload | None, dict[str, Any] | None]:
    try:
        payload = UiJobDraftPayload.model_validate(item)
    except Exception as exc:
        return None, {"code": "UIJ_INVALID_INPUT", "message": str(exc)}

    field_errors = _ui_validate(payload)
    if field_errors:
        return None, {"code": "UIJ_INVALID_INPUT", "field_errors": field_errors}

    ch = dbm.get_channel_by_id(conn, payload.channel_id)
    if not ch:
        return None, {"code": "UIJ_INVALID_INPUT", "field_errors": {"project": ["project does not exist"]}}
    return payload, None


def _parse_bulk_playlist_builder_request(item: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if "playlist_builder" not in item:
        return None, None

    raw = item.get("playlist_builder")
    if raw is False or raw is None:
        return None, None
    if raw is True:
        return {}, None
    if not isinstance(raw, dict):
        return None, {"code": "UIJ_INVALID_INPUT", "message": "playlist_builder must be boolean or object"}

    try:
        overrides = PlaylistBriefOverrides.model_validate(raw).as_patch_dict()
    except Exception as exc:
        return None, {"code": "UIJ_INVALID_INPUT", "message": f"playlist_builder invalid: {exc}"}
    return overrides, None


def _validate_create_item_with_playlist(conn: Any, item: dict[str, Any]) -> tuple[UiJobDraftPayload | None, dict[str, Any] | None, dict[str, Any] | None]:
    payload, item_error = _validate_create_item(conn, item)
    if item_error:
        return None, None, item_error

    overrides, override_error = _parse_bulk_playlist_builder_request(item)
    if override_error:
        return None, None, override_error

    return payload, overrides, None


def _bulk_preview_playlist_builder(conn: Any, *, channel_id: int, overrides: dict[str, Any] | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if overrides is None:
        return None, None

    channel = dbm.get_channel_by_id(conn, int(channel_id))
    if not channel:
        return None, {"code": "UIJ_INVALID_INPUT", "message": "project does not exist"}

    try:
        settings_row = dbm.get_playlist_builder_channel_settings(conn, str(channel["slug"]))
        settings_patch = channel_settings_row_to_patch(settings_row)
        brief = resolve_playlist_brief(
            channel_slug=str(channel["slug"]),
            job_id=None,
            channel_settings=settings_patch,
            job_override={},
            request_override=overrides,
        )
        envelope = create_preview_for_brief(conn, brief=brief, created_by=env.basic_user)
        summary = build_preview_response(envelope).get("summary", {})
        return {
            "requested": True,
            "ok": True,
            "preview_id": envelope.preview_id,
            "summary": {
                "generation_mode": summary.get("generation_mode"),
                "strictness_mode": summary.get("strictness_mode"),
                "tracks_count": summary.get("tracks_count"),
                "duration": summary.get("duration"),
                "warnings": summary.get("warnings"),
            },
        }, None
    except PlaylistBuilderValidationError as exc:
        return None, {"code": "UIJ_INVALID_INPUT", "message": f"playlist_builder invalid: {exc}"}
    except PlaylistBuilderApiError as exc:
        return None, {"code": exc.code, "message": exc.message}


def _bulk_apply_playlist_builder_for_job(conn: Any, *, job_id: int, overrides: dict[str, Any] | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if overrides is None:
        return None, None

    try:
        envelope = create_preview(conn, job_id=job_id, override=overrides, created_by=env.basic_user)
        applied = apply_preview(conn, job_id=job_id, preview_id=envelope.preview_id, manage_transaction=False)
        if overrides:
            dbm.update_ui_job_playlist_builder_override_json(
                conn,
                job_id=job_id,
                playlist_builder_override_json=json.dumps(overrides, sort_keys=True),
            )
        return {
            "requested": True,
            "ok": True,
            "preview_id": envelope.preview_id,
            "draft_history_id": applied.get("draft_history_id"),
        }, None
    except PlaylistBuilderApiError as exc:
        return None, {"code": exc.code, "message": exc.message}


def _preview_enqueue_existing_item(conn: Any, item: dict[str, Any]) -> dict[str, Any]:
    job_id_value = item.get("job_id")
    try:
        job_id = int(job_id_value)
    except (TypeError, ValueError):
        return {"job_id": str(job_id_value), "error": {"code": "UIJ_JOB_NOT_FOUND", "message": "UI job not found"}}

    guard = check_ui_render_guard(conn, job_id=job_id)
    if guard.reason == "not_found":
        return {"job_id": str(job_id), "error": {"code": "UIJ_JOB_NOT_FOUND", "message": "UI job not found"}}
    if guard.reason == "not_allowed":
        return {"job_id": str(job_id), "error": {"code": "UIJ_RENDER_NOT_ALLOWED", "message": "Status not allowed"}}
    if guard.reason == "already_in_progress":
        return {"job_id": str(job_id), "enqueued": False, "message": "Already in progress"}
    return {"job_id": str(job_id), "enqueued": True}


def _summary_from_enqueue_results(results: list[dict[str, Any]], requested: int) -> dict[str, int]:
    return {
        "requested": requested,
        "enqueued": sum(1 for item in results if item.get("enqueued") is True),
        "noop": sum(1 for item in results if item.get("enqueued") is False),
        "failed": sum(1 for item in results if item.get("error")),
    }


def _bulk_render_selected_item(job_id_text: str) -> dict[str, Any]:
    try:
        return _render_selected_item(job_id_text)
    except Exception:
        return {
            "job_id": str(job_id_text),
            "error": {"code": "UIJ_INTERNAL", "message": "Internal error"},
        }


def _parse_bulk_payload(payload: UiJobsBulkJsonPayload) -> tuple[str | None, JSONResponse | None]:
    mode = str(payload.mode or "").strip()
    if mode not in {"create_draft_jobs", "create_and_enqueue", "enqueue_existing_jobs"}:
        return None, _bulk_json_invalid("mode is invalid")
    if not payload.items:
        return None, _bulk_json_invalid("items is required and must be non-empty")
    return mode, None


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        jobs = dbm.list_jobs(conn, limit=200)
        jobs_total = int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
        failed_total = int(conn.execute("SELECT COUNT(*) AS c FROM jobs WHERE UPPER(COALESCE(state,''))='FAILED'").fetchone()["c"])
        channels_total = int(conn.execute("SELECT COUNT(*) AS c FROM channels").fetchone()["c"])
        batch_month_total = int(conn.execute("SELECT COUNT(DISTINCT strftime('%Y-%m', COALESCE(planned_at,''))) AS c FROM releases WHERE planned_at IS NOT NULL").fetchone()["c"])
        control_center = build_control_center_contract_skeleton(
            factory_summary={"jobs_total": jobs_total},
            attention_summary={"failed_jobs": failed_total},
            channel_summary={"channels_total": channels_total, "active_channels": channels_total},
            batch_month_summary={"batch_month_total": batch_month_total},
            task_routing=default_task_routing_contract(),
        )
        attention_routes = [
            {
                "why": "Failed jobs require operator triage",
                "scope": "publish_failed",
                "urgency": "HIGH" if failed_total > 0 else "NORMAL",
                "next": "/ui/publish/failed",
            },
            {
                "why": "Planning drift can block upcoming batches",
                "scope": "planning",
                "urgency": "NORMAL",
                "next": "/ui/planner",
            },
        ]
        recent_changes = [
            {
                "job_id": int(j.get("id") or 0),
                "state": str(j.get("state") or ""),
                "updated_at": str(j.get("updated_at") or ""),
                "route": f"/jobs/{int(j.get('id') or 0)}",
            }
            for j in jobs[:5]
            if j.get("id") is not None
        ]
        token = str(request.query_params.get("ctx") or "").strip() or None
        incoming = resolve_incoming_context(token=token, known_paths=set(route_ownership_map().keys()))
        return_to_context = None
        if incoming is not None:
            return_to_context = {
                "path": incoming.current_path,
                "token": token,
                "label": "Return to previous context",
            }
    finally:
        conn.close()
    return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs, "control_center": control_center, "attention_routes": attention_routes, "recent_changes": recent_changes, "return_to_context": return_to_context})


@app.get("/ui/ops/recovery", response_class=HTMLResponse)
def recovery_console(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("recovery_console.html", {"request": request})


@app.get("/ui/db-viewer", response_class=HTMLResponse)
def ui_db_viewer_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("db_viewer.html", {"request": request})


@app.get("/ui/planner", response_class=HTMLResponse)
def ui_planner_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("planner_bulk_releases.html", {"request": request})


@app.get("/ui/publish/queue", response_class=HTMLResponse)
def ui_publish_queue_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("publish_queue.html", {"request": request, "view": "queue"})


@app.get("/ui/publish/blocked", response_class=HTMLResponse)
def ui_publish_blocked_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("publish_queue.html", {"request": request, "view": "blocked"})


@app.get("/ui/publish/failed", response_class=HTMLResponse)
def ui_publish_failed_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("publish_queue.html", {"request": request, "view": "failed"})


@app.get("/ui/publish/manual", response_class=HTMLResponse)
def ui_publish_manual_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("publish_queue.html", {"request": request, "view": "manual"})


@app.get("/ui/publish/health", response_class=HTMLResponse)
def ui_publish_health_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("publish_queue.html", {"request": request, "view": "health"})


@app.get("/ui/publish/jobs/{job_id}", response_class=HTMLResponse)
def ui_publish_job_detail_page(job_id: int, request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("publish_job_detail.html", {"request": request, "job_id": job_id})


@app.get("/ui/metadata/title-templates", response_class=HTMLResponse)
def ui_metadata_title_templates_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("metadata_title_templates.html", {"request": request})


@app.get("/ui/channels/{channel_slug}/metadata-defaults", response_class=HTMLResponse)
def ui_channel_metadata_defaults_page(channel_slug: str, request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("metadata_channel_defaults.html", {"request": request, "channel_slug": channel_slug})


@app.get("/ui/track-catalog/analysis-report", response_class=HTMLResponse)
def ui_track_analysis_report_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("track_analysis_report.html", {"request": request})


@app.get("/ui/tags", response_class=HTMLResponse)
@app.get("/ui/track-catalog/custom-tags", response_class=HTMLResponse)
def ui_tags_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("tags.html", {"request": request})


@app.get("/ui/track-catalog/custom-tags/dashboard", response_class=HTMLResponse)
def ui_tags_channel_dashboard_root_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("tags_channel_dashboard.html", {"request": request, "channel_slug": ""})


@app.get("/ui/track-catalog/custom-tags/dashboard/{channel_slug}", response_class=HTMLResponse)
def ui_tags_channel_dashboard_page(channel_slug: str, request: Request, _: bool = Depends(require_basic_auth(env))):
    return templates.TemplateResponse("tags_channel_dashboard.html", {"request": request, "channel_slug": channel_slug})


def _all_channels(conn) -> list:
    return conn.execute("SELECT id, slug, display_name FROM channels ORDER BY display_name ASC").fetchall()


def _build_ui_payload(
    *,
    channel_id: int,
    title: str,
    description: str,
    tags_csv: str,
    cover_name: str,
    cover_ext: str,
    background_name: str,
    background_ext: str,
    audio_ids_text: str,
) -> UiJobDraftPayload:
    return UiJobDraftPayload(
        channel_id=channel_id,
        title=title,
        description=description,
        tags_csv=tags_csv,
        cover_name=cover_name,
        cover_ext=cover_ext,
        background_name=background_name,
        background_ext=background_ext,
        audio_ids_text=audio_ids_text,
    )


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_page(job_id: int, request: Request, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        if not job:
            raise HTTPException(404)
        qa = conn.execute("SELECT * FROM qa_reports WHERE job_id = ?", (job_id,)).fetchone()
        yt = conn.execute("SELECT * FROM youtube_uploads WHERE job_id = ?", (job_id,)).fetchone()
    finally:
        conn.close()
    return templates.TemplateResponse("job.html", {"request": request, "job": job, "qa": qa, "yt": yt})


@app.get("/v1/jobs")
def api_jobs(state: Optional[str] = None, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        jobs = dbm.list_jobs(conn, state=state, limit=500)
        job_ids = [int(job["id"]) for job in jobs]
        retry_child_by_parent_id: dict[int, int] = {}
        if job_ids:
            placeholders = ",".join("?" for _ in job_ids)
            rows = conn.execute(
                f"SELECT id, retry_of_job_id FROM jobs WHERE retry_of_job_id IN ({placeholders})",
                job_ids,
            ).fetchall()
            retry_child_by_parent_id = {
                int(row["retry_of_job_id"]): int(row["id"])
                for row in rows
                if row.get("retry_of_job_id") is not None
            }

        for job in jobs:
            retry_child_job_id = retry_child_by_parent_id.get(int(job["id"]))
            status = str(job.get("state") or "")
            job["status"] = status
            job["attempt_no"] = int(job.get("attempt_no") or 1)
            job["retry_child_job_id"] = retry_child_job_id
            actions = dict(job.get("actions") or {})
            actions["retry_allowed"] = bool(status == "FAILED" and retry_child_job_id is None)
            job["actions"] = actions
    finally:
        conn.close()
    return {"jobs": jobs}


@app.get("/v1/ops/recovery/jobs")
def api_recovery_jobs(
    channel: Optional[str] = None,
    state: Optional[str] = None,
    actionability: Optional[str] = None,
    _: bool = Depends(require_basic_auth(env)),
):
    actionability_normalized = (actionability or "").strip().lower()
    if actionability_normalized and actionability_normalized not in _RECOVERY_ACTIONABLE:
        raise HTTPException(422, "invalid actionability")

    conn = dbm.connect(env)
    try:
        jobs = _list_recovery_jobs(conn)
    finally:
        conn.close()

    if channel:
        jobs = [job for job in jobs if str(job.get("channel_slug") or "") == channel]
    if state:
        jobs = [job for job in jobs if str(job.get("state") or "") == state]
    if actionability_normalized:
        jobs = [job for job in jobs if bool(job.get("actions", {}).get(actionability_normalized))]

    summary = {
        "total": len(jobs),
        "failed": sum(1 for job in jobs if bool(job.get("issue_flags", {}).get("failed"))),
        "stale_or_stuck": sum(1 for job in jobs if bool(job.get("issue_flags", {}).get("stale_or_stuck"))),
        "cleanup_pending": sum(1 for job in jobs if bool(job.get("issue_flags", {}).get("cleanup_pending"))),
        "artifact_issue": sum(1 for job in jobs if bool(job.get("issue_flags", {}).get("artifact_issue"))),
        "retryable": sum(1 for job in jobs if bool(job.get("actions", {}).get("retryable"))),
        "cancellable": sum(1 for job in jobs if bool(job.get("actions", {}).get("cancellable"))),
        "reclaimable": sum(1 for job in jobs if bool(job.get("actions", {}).get("reclaimable"))),
        "cleanupable": sum(1 for job in jobs if bool(job.get("actions", {}).get("cleanupable"))),
        "restartable": sum(1 for job in jobs if bool(job.get("actions", {}).get("restartable"))),
    }
    channels = sorted({str(job.get("channel_slug") or "") for job in jobs if str(job.get("channel_slug") or "")})
    states = sorted({str(job.get("state") or "") for job in jobs if str(job.get("state") or "")})
    return {"summary": summary, "jobs": jobs, "filters": {"channels": channels, "states": states}}


@app.get("/v1/ops/recovery/audit")
def api_recovery_audit(limit: int = 50, _: bool = Depends(require_basic_auth(env))):
    safe_limit = max(1, min(int(limit), 200))
    return {"items": _load_recovery_audit(limit=safe_limit)}


def _require_recovery_confirm(payload: RecoveryActionPayload) -> str:
    if not payload.confirm:
        raise HTTPException(409, "confirmation is required")
    return (payload.reason or "operator action").strip() or "operator action"


@app.post("/v1/ops/recovery/jobs/{job_id}/retry")
def api_recovery_retry(job_id: int, payload: RecoveryActionPayload, _: bool = Depends(require_basic_auth(env))):
    reason = _require_recovery_confirm(payload)
    result = api_ui_job_retry(job_id, _=True)
    if isinstance(result, JSONResponse):
        _record_recovery_action(job_id=job_id, action="retry", reason=reason, result="failed", details={"status": result.status_code})
        return result
    _record_recovery_action(job_id=job_id, action="retry", reason=reason, result="ok", details=result)
    return {"ok": True, "action": "retry", "result": result}


@app.post("/v1/ops/recovery/jobs/{job_id}/cancel")
def api_recovery_cancel(job_id: int, payload: RecoveryActionPayload, _: bool = Depends(require_basic_auth(env))):
    reason = _require_recovery_confirm(payload)
    result = api_cancel(job_id, CancelPayload(reason=reason), _=True)
    _record_recovery_action(job_id=job_id, action="cancel", reason=reason, result="ok", details=result)
    return {"ok": True, "action": "cancel", "result": result}


@app.post("/v1/ops/recovery/jobs/{job_id}/reclaim")
def api_recovery_reclaim(job_id: int, payload: RecoveryActionPayload, _: bool = Depends(require_basic_auth(env))):
    reason = _require_recovery_confirm(payload)
    conn = dbm.connect(env)
    try:
        details = _manual_reclaim_job(conn, job_id=job_id)
    finally:
        conn.close()

    _record_recovery_action(job_id=job_id, action="reclaim", reason=reason, result="ok", details=details)
    return {"ok": True, "action": "reclaim", "result": details}


@app.post("/v1/ops/recovery/jobs/{job_id}/cleanup")
def api_recovery_cleanup(job_id: int, payload: RecoveryActionPayload, _: bool = Depends(require_basic_auth(env))):
    reason = _require_recovery_confirm(payload)
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        recovery_job = _build_recovery_job(job, now_ts=dbm.now_ts(), lock_ttl_sec=env.job_lock_ttl_sec)
    finally:
        conn.close()

    if not bool(recovery_job.get("actions", {}).get("cleanupable")):
        details = {
            "message": "job is not cleanupable",
            "state": str(job.get("state") or ""),
            "issue_flags": recovery_job.get("issue_flags") or {},
        }
        _record_recovery_action(job_id=job_id, action="cleanup", reason=reason, result="rejected", details=details)
        raise HTTPException(409, "job is not cleanupable")

    try:
        details = _force_cleanup_job_artifacts(job_id)
    except Exception as exc:
        _record_recovery_action(
            job_id=job_id,
            action="cleanup",
            reason=reason,
            result="failed",
            details={"error": str(exc)},
        )
        raise

    _record_recovery_action(job_id=job_id, action="cleanup", reason=reason, result="ok", details=details)
    return {"ok": True, "action": "cleanup", "result": details}


@app.post("/v1/ops/recovery/jobs/{job_id}/restart")
def api_recovery_restart(job_id: int, payload: RecoveryActionPayload, _: bool = Depends(require_basic_auth(env))):
    reason = _require_recovery_confirm(payload)
    conn = dbm.connect(env)
    try:
        guard = check_ui_render_guard(conn, job_id=job_id)
    finally:
        conn.close()
    if not guard.eligible:
        raise HTTPException(409, "controlled restart is supported only for Draft jobs with no active inputs")
    result = api_ui_job_render(job_id, _=True)
    if isinstance(result, JSONResponse):
        _record_recovery_action(job_id=job_id, action="restart", reason=reason, result="failed", details={"status": result.status_code})
        return result
    _record_recovery_action(job_id=job_id, action="restart", reason=reason, result="ok", details=result)
    return {"ok": True, "action": "restart", "result": result}


@app.get("/v1/jobs/{job_id}")
def api_job(job_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        if not job:
            raise HTTPException(404)
        qa = conn.execute("SELECT * FROM qa_reports WHERE job_id = ?", (job_id,)).fetchone()
        yt = conn.execute("SELECT * FROM youtube_uploads WHERE job_id = ?", (job_id,)).fetchone()
    finally:
        conn.close()
    return {"job": job, "qa": qa, "youtube": yt}


@app.get("/v1/jobs/{job_id}/logs", response_class=PlainTextResponse)
def api_job_logs(job_id: int, tail: int = 200, _: bool = Depends(require_basic_auth(env))):
    p = logs_path(env, job_id)
    if not p.exists():
        return ""
    lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    return "\n".join(lines[-tail:]) + "\n"


@app.get("/v1/jobs/{job_id}/qa")
def api_job_qa(job_id: int, _: bool = Depends(require_basic_auth(env))):
    p = qa_path(env, job_id)
    if not p.exists():
        return {"qa": None}
    return {"qa": json.loads(p.read_text(encoding="utf-8"))}


@app.post("/v1/jobs/{job_id}/approve")
def api_approve(job_id: int, payload: ApprovePayload, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        return approve_job(conn, job_id=job_id, comment=(payload.comment or "approved"))
    finally:
        conn.close()


@app.post("/v1/jobs/{job_id}/reject")
def api_reject(job_id: int, payload: RejectPayload, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        return reject_job(conn, job_id=job_id, comment=payload.comment)
    finally:
        conn.close()




@app.post("/v1/jobs/{job_id}/cancel")
def api_cancel(job_id: int, payload: CancelPayload, _: bool = Depends(require_basic_auth(env))):
    reason = (payload.reason or "cancelled by user").strip() or "cancelled by user"
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        if not job:
            raise HTTPException(404)
        state = str(job.get("state") or "")
        if state in ("PUBLISHED", "REJECTED", "APPROVED", "CANCELLED"):
            raise HTTPException(409, f"job is already terminal: {state}")

        # create cancellation marker (best-effort)
        try:
            from services.common.paths import cancel_flag_path

            flag = cancel_flag_path(env, job_id)
            flag.parent.mkdir(parents=True, exist_ok=True)
            flag.write_text(reason, encoding="utf-8")
        except Exception:
            pass

        dbm.cancel_job(conn, job_id, reason=reason)
    finally:
        conn.close()
    return {"ok": True}
@app.post("/v1/jobs/{job_id}/mark_published")
def api_mark_published(job_id: int, payload: dict, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        result = mark_job_published(conn, job_id=job_id)
        if result.get("history_id") is not None:
            logger.info("playlist_builder.history.committed_written", extra={"job_id": job_id, "history_id": result["history_id"]})
        return {"ok": True, "delete_mp4_at": result["delete_mp4_at"]}
    except PlaylistBuilderApiError as exc:
        status = {
            "PLB_COMMITTED_HISTORY_MISSING_DRAFT": 409,
            "PLB_COMMITTED_HISTORY_MISSING_ITEMS": 409,
            "PLB_COMMITTED_HISTORY_PLAYLIST_MISMATCH": 409,
        }.get(exc.code, 409)
        return _plb_error(status, exc.code, exc.message)
    finally:
        conn.close()


@app.get("/v1/problems/readiness/grouped")
def api_problem_readiness_grouped(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        jobs = dbm.list_jobs(conn, limit=500)
    finally:
        conn.close()
    return build_grouped_problem_surface(jobs=jobs)


@app.get("/ui/problems/readiness", response_class=HTMLResponse)
def ui_problem_readiness_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        jobs = dbm.list_jobs(conn, limit=500)
    finally:
        conn.close()
    surface = build_grouped_problem_surface(jobs=jobs)
    return templates.TemplateResponse("ui_problems_readiness.html", {"request": request, "surface": surface})


@app.get("/v1/workspaces/catalog")
def api_workspace_catalog(_: bool = Depends(require_basic_auth(env))):
    from services.factory_api.operator_workspaces import workspace_family_catalog

    return workspace_family_catalog()


@app.get("/v1/workspaces/{family}/{entity_id}/drilldown")
def api_workspace_drilldown(family: str, entity_id: str, request: Request, _: bool = Depends(require_basic_auth(env))):
    from services.factory_api.operator_workspaces import entity_drilldown_contract

    return entity_drilldown_contract(
        entry_context=request.query_params.get("entry", "control_center"),
        scope=f"{family}:{entity_id}",
        related_context_links=[
            {"kind": "channel_workspace", "href": "/ui/workspaces/channel/1"},
            {"kind": "batch_workspace", "href": "/ui/workspaces/batch/2026-04"},
        ],
        return_path=request.query_params.get("return_path", "/"),
        open_full_context_path=f"/ui/workspaces/{family}/{entity_id}",
    )


@app.get("/v1/workspaces/task-continuity")
def api_workspace_task_continuity(request: Request, _: bool = Depends(require_basic_auth(env))):
    from services.factory_api.operator_workspaces import task_continuity_contract

    return task_continuity_contract(
        parent_context_ref=request.query_params.get("parent", "control_center"),
        filters={"status": request.query_params.get("status", "all")},
        scope=request.query_params.get("scope", "job"),
        result_return_path=request.query_params.get("return_path", "/"),
    )


@app.get("/v1/workspaces/result-return")
def api_workspace_result_return(request: Request, _: bool = Depends(require_basic_auth(env))):
    from services.factory_api.operator_workspaces import result_return_contract

    return result_return_contract(
        from_action=request.query_params.get("action", "mutate"),
        return_path=request.query_params.get("return_path", "/"),
        open_full_context_path=request.query_params.get("open_full", request.query_params.get("return_path", "/")),
    )


@app.get("/v1/actions/contracts/preview-apply")
def api_action_contract_preview_apply(request: Request, _: bool = Depends(require_basic_auth(env))):
    from services.factory_api.shared_action_flows import preview_to_apply_contract

    return preview_to_apply_contract(action=request.query_params.get("action", "retry"), preview_scope=request.query_params.get("scope", "job"))


@app.get("/v1/actions/contracts/preview-confirm-execute")
def api_action_contract_preview_confirm_execute(request: Request, _: bool = Depends(require_basic_auth(env))):
    from services.factory_api.shared_action_flows import preview_confirm_execute_contract

    return preview_confirm_execute_contract(action=request.query_params.get("action", "cancel"), preview_scope=request.query_params.get("scope", "job"))


@app.get("/v1/actions/contracts/stale-refresh")
def api_action_contract_stale_refresh(request: Request, _: bool = Depends(require_basic_auth(env))):
    from services.factory_api.shared_action_flows import stale_refusal_or_refresh_contract

    return stale_refusal_or_refresh_contract(expected_version=request.query_params.get("expected", "v1"), actual_version=request.query_params.get("actual", "v1"))


@app.get("/v1/actions/contracts/partial-result")
def api_action_contract_partial_result(request: Request, _: bool = Depends(require_basic_auth(env))):
    from services.factory_api.shared_action_flows import partial_result_summary_contract

    return partial_result_summary_contract(
        succeeded=[v for v in request.query_params.get("succeeded", "").split(",") if v],
        failed=[v for v in request.query_params.get("failed", "").split(",") if v],
        unresolved=[v for v in request.query_params.get("unresolved", "").split(",") if v],
    )


@app.get("/v1/actions/contracts/batch-preview-execute")
def api_action_contract_batch_preview_execute(request: Request, _: bool = Depends(require_basic_auth(env))):
    from services.factory_api.shared_action_flows import batch_preview_execute_contract

    return batch_preview_execute_contract(
        targets=[v for v in request.query_params.get("targets", "").split(",") if v],
        action=request.query_params.get("action", "batch_execute"),
        requires_preview=request.query_params.get("requires_preview", "1") != "0",
    )


@app.get("/v1/problems/readiness/contract")
def api_problem_readiness_contract(_: bool = Depends(require_basic_auth(env))):
    return {
        "catalog": problem_readiness_contract_catalog(),
        "sample": problem_readiness_item_contract(
            state="FAILED",
            severity="HIGH",
            primary_reason="render watchdog detected non-growing output",
            supporting_signals=["ffmpeg stderr anomaly", "output size unchanged"],
            next_direction="open recovery workspace",
        ),
    }


@app.get("/v1/control-center/contract-skeleton")
def api_control_center_contract_skeleton(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        jobs_total = int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
        failed_total = int(conn.execute("SELECT COUNT(*) AS c FROM jobs WHERE UPPER(COALESCE(state,''))='FAILED'").fetchone()["c"])
        channels_total = int(conn.execute("SELECT COUNT(*) AS c FROM channels").fetchone()["c"])
        active_channels = channels_total
        batch_month_total = int(conn.execute("SELECT COUNT(DISTINCT strftime('%Y-%m', COALESCE(planned_at,''))) AS c FROM releases WHERE planned_at IS NOT NULL").fetchone()["c"])
    finally:
        conn.close()
    return build_control_center_contract_skeleton(
        factory_summary={"jobs_total": jobs_total},
        attention_summary={"failed_jobs": failed_total},
        channel_summary={"channels_total": channels_total, "active_channels": active_channels},
        batch_month_summary={"batch_month_total": batch_month_total},
        task_routing=default_task_routing_contract(),
    )


@app.get("/v1/ui/jobs/statuses")
def api_ui_jobs_statuses(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        ordered_statuses = dbm.list_jobs_state_domain(conn)
    finally:
        conn.close()
    return {"statuses": ordered_statuses}


@app.get("/v1/ui/jobs/render_allowed_statuses")
def api_ui_jobs_render_allowed_statuses(_: bool = Depends(require_basic_auth(env))):
    return {"render_allowed_statuses": ["Draft"]}


@app.post("/v1/ui/jobs")
def api_create_ui_job(payload: UiJobDraftPayload, _: bool = Depends(require_basic_auth(env))):
    errors = _ui_validate(payload)
    if errors:
        raise HTTPException(422, {"field_errors": errors})

    conn = dbm.connect(env)
    try:
        ch = dbm.get_channel_by_id(conn, payload.channel_id)
        if not ch:
            raise HTTPException(422, {"field_errors": {"project": ["project does not exist"]}})
        job_id = dbm.create_ui_job_draft(
            conn,
            channel_id=payload.channel_id,
            title=payload.title.strip(),
            description=payload.description.strip(),
            tags_csv=payload.tags_csv.strip(),
            cover_name=payload.cover_name.strip() or None,
            cover_ext=payload.cover_ext.strip() or None,
            background_name=payload.background_name.strip(),
            background_ext=payload.background_ext.strip(),
            audio_ids_text=payload.audio_ids_text.strip(),
            job_type="UI",
        )
    finally:
        conn.close()
    return {"ok": True, "job_id": job_id}


@app.post("/v1/ui/jobs/playlist-builder-draft")
def api_create_ui_job_for_playlist_builder(payload: UiPlaylistBuilderDraftPayload, _: bool = Depends(require_basic_auth(env))):
    errors = _ui_validate_playlist_builder_draft(payload)
    if errors:
        raise HTTPException(422, {"field_errors": errors})

    conn = dbm.connect(env)
    try:
        ch = dbm.get_channel_by_id(conn, payload.channel_id)
        if not ch:
            raise HTTPException(422, {"field_errors": {"project": ["project does not exist"]}})
        job_id = dbm.create_ui_job_draft(
            conn,
            channel_id=payload.channel_id,
            title=payload.title.strip(),
            description=payload.description.strip(),
            tags_csv=payload.tags_csv.strip(),
            cover_name=payload.cover_name.strip() or None,
            cover_ext=payload.cover_ext.strip() or None,
            background_name=payload.background_name.strip(),
            background_ext=payload.background_ext.strip(),
            audio_ids_text="",
            job_type="UI",
        )
    finally:
        conn.close()
    return {"ok": True, "job_id": job_id}


@app.post("/v1/ui/jobs/bulk-json/preview")
def api_ui_jobs_bulk_json_preview(payload: UiJobsBulkJsonPayload, _: bool = Depends(require_basic_auth(env))):
    mode, error = _parse_bulk_payload(payload)
    if error:
        return error

    conn = dbm.connect(env)
    try:
        results: list[dict[str, Any]] = []
        if mode in {"create_draft_jobs", "create_and_enqueue"}:
            for index, item in enumerate(payload.items):
                parsed, playlist_overrides, item_error = _validate_create_item_with_playlist(conn, item)
                if item_error:
                    results.append({"index": index, "error": item_error})
                    continue
                assert parsed is not None
                playlist_preview, playlist_error = _bulk_preview_playlist_builder(
                    conn,
                    channel_id=parsed.channel_id,
                    overrides=playlist_overrides,
                )
                if playlist_error:
                    results.append({"index": index, "error": playlist_error})
                else:
                    result_item = {"index": index, "valid": True}
                    if playlist_preview is not None:
                        result_item["playlist_builder"] = playlist_preview
                    results.append(result_item)
        else:
            for item in payload.items:
                if set(item.keys()) != {"job_id"}:
                    results.append({"job_id": str(item.get("job_id")), "error": {"code": "UIJ_INVALID_INPUT", "message": "item must contain only job_id"}})
                else:
                    results.append(_preview_enqueue_existing_item(conn, item))
        summary = {
            "requested": len(payload.items),
            "valid": sum(1 for item in results if item.get("valid") is True or item.get("enqueued") in {True, False}),
            "failed": sum(1 for item in results if item.get("error")),
        }
        return {"mode": mode, "summary": summary, "results": results}
    finally:
        conn.close()


@app.post("/v1/ui/jobs/bulk-json/execute")
def api_ui_jobs_bulk_json_execute(payload: UiJobsBulkJsonPayload, _: bool = Depends(require_basic_auth(env))):
    mode, error = _parse_bulk_payload(payload)
    if error:
        return error

    if mode == "enqueue_existing_jobs":
        results = [_bulk_render_selected_item(str(item.get("job_id"))) for item in payload.items]
        return {"mode": mode, "summary": _summary_from_enqueue_results(results, len(payload.items)), "results": results}

    conn = dbm.connect(env)
    created_job_ids: list[int] = []
    tx_started = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        tx_started = True
        validated: list[tuple[UiJobDraftPayload, dict[str, Any] | None]] = []
        for index, item in enumerate(payload.items):
            parsed, playlist_overrides, item_error = _validate_create_item_with_playlist(conn, item)
            if item_error:
                conn.execute("ROLLBACK")
                tx_started = False
                return {
                    "mode": mode,
                    "summary": {"requested": len(payload.items), "created": 0, "failed": 1},
                    "results": [{"index": index, "error": item_error}],
                }
            assert parsed is not None
            validated.append((parsed, playlist_overrides))

        playlist_meta_by_job: dict[int, dict[str, Any]] = {}
        for index, (item, playlist_overrides) in enumerate(validated):
            job_id = dbm.create_ui_job_draft(
                conn,
                channel_id=item.channel_id,
                title=item.title.strip(),
                description=item.description.strip(),
                tags_csv=item.tags_csv.strip(),
                cover_name=item.cover_name.strip() or None,
                cover_ext=item.cover_ext.strip() or None,
                background_name=item.background_name.strip(),
                background_ext=item.background_ext.strip(),
                audio_ids_text=item.audio_ids_text.strip(),
                job_type="UI",
            )
            playlist_meta, playlist_error = _bulk_apply_playlist_builder_for_job(
                conn,
                job_id=job_id,
                overrides=playlist_overrides,
            )
            if playlist_error:
                conn.execute("ROLLBACK")
                tx_started = False
                return {
                    "mode": mode,
                    "summary": {"requested": len(payload.items), "created": 0, "failed": 1},
                    "results": [{"index": index, "job_id": str(job_id), "error": playlist_error}],
                }
            created_job_ids.append(job_id)
            playlist_meta_by_job[job_id] = playlist_meta

        conn.execute("COMMIT")
        tx_started = False
    except Exception:
        if tx_started:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    create_results = []
    for idx, job_id in enumerate(created_job_ids):
        item_result = {"index": idx, "job_id": str(job_id), "created": True}
        playlist_meta = playlist_meta_by_job.get(job_id)
        if playlist_meta is not None:
            item_result["playlist_builder"] = playlist_meta
        create_results.append(item_result)
    if mode == "create_draft_jobs":
        return {
            "mode": mode,
            "summary": {"requested": len(payload.items), "created": len(created_job_ids), "failed": 0},
            "results": create_results,
        }

    enqueue_results = [_bulk_render_selected_item(str(job_id)) for job_id in created_job_ids]
    merged_results = [
        {**create_result, "enqueue": enqueue_result}
        for create_result, enqueue_result in zip(create_results, enqueue_results)
    ]
    enqueue_summary = _summary_from_enqueue_results(enqueue_results, len(payload.items))
    return {
        "mode": mode,
        "summary": {
            "requested": len(payload.items),
            "created": len(created_job_ids),
            "enqueued": enqueue_summary["enqueued"],
            "noop": enqueue_summary["noop"],
            "failed": enqueue_summary["failed"],
        },
        "results": merged_results,
    }


def _uij_error(status_code: int, code: str, message: str, *, details: dict[str, Any] | None = None) -> JSONResponse:
    error: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        error["details"] = details
    return JSONResponse(status_code=status_code, content={"error": error})


def _disk_pressure_error_details(*, operation: str, snapshot: Any, reason: str) -> dict[str, Any]:
    threshold_percent = float(snapshot.thresholds.fail_percent)
    threshold_gb = float(snapshot.thresholds.fail_gib)
    return {
        "operation": operation,
        "checked_path": snapshot.checked_path,
        "resolved_mount_or_anchor": snapshot.resolved_mount_or_anchor,
        "free_gb": round(float(snapshot.free_gib), 2),
        "free_percent": round(float(snapshot.free_percent), 2),
        "threshold_gb": round(threshold_gb, 2),
        "threshold_percent": round(threshold_percent, 2),
        "reason": reason,
    }


def _disk_guard_write_heavy(*, operation: str) -> JSONResponse | None:
    snapshot = evaluate_disk_pressure_for_env(env=env)
    emit_disk_pressure_event(logger=logger, snapshot=snapshot, stage=operation)
    decision = classify_write_block(snapshot)
    if not decision.blocked:
        return None
    details = _disk_pressure_error_details(operation=operation, snapshot=snapshot, reason=decision.reason)
    logger.warning(
        "disk.write_blocked",
        extra={
            "disk_block": {
                "operation": operation,
                "reason": decision.reason,
                "checked_path": snapshot.checked_path,
                "resolved_mount_or_anchor": snapshot.resolved_mount_or_anchor,
                "total_bytes": snapshot.total_bytes,
                "used_bytes": snapshot.used_bytes,
                "free_bytes": snapshot.free_bytes,
                "free_percent": snapshot.free_percent,
                "threshold_percent": snapshot.thresholds.fail_percent,
                "threshold_bytes": int(snapshot.thresholds.fail_gib * (1024**3)),
                "threshold_gib": snapshot.thresholds.fail_gib,
            }
        },
    )
    return _uij_error(503, "DISK_CRITICAL_WRITE_BLOCKED", "Operation blocked due to critical disk pressure", details=details)


def _render_selected_item(job_id_text: str) -> dict[str, Any]:
    blocked = _disk_guard_write_heavy(operation="ui_jobs_render_selected")
    if blocked is not None:
        return {"job_id": str(job_id_text), **json.loads(blocked.body.decode("utf-8"))}
    try:
        job_id = int(job_id_text)
    except (TypeError, ValueError):
        return {
            "job_id": str(job_id_text),
            "error": {"code": "UIJ_JOB_NOT_FOUND", "message": "UI job not found"},
        }

    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        guard = check_ui_render_guard(conn, job_id=job_id)
        if guard.reason == "not_found":
            return {"job_id": str(job_id), "error": {"code": "UIJ_JOB_NOT_FOUND", "message": "UI job not found"}}
        if guard.reason == "not_allowed":
            return {
                "job_id": str(job_id),
                "error": {"code": "UIJ_RENDER_NOT_ALLOWED", "message": "Status not allowed"},
            }
        if guard.reason == "already_in_progress":
            return {"job_id": str(job_id), "enqueued": False, "message": "Already in progress"}

        if not job:
            return {"job_id": str(job_id), "error": {"code": "UIJ_JOB_NOT_FOUND", "message": "UI job not found"}}

        channel_slug = str(job.get("channel_slug") or "")
        token_path = oauth_token_path(base_dir=env.gdrive_tokens_dir, channel_slug=channel_slug)
        if (not channel_slug) or (not token_path.is_file()):
            raise RuntimeError(f"missing gdrive token for channel '{channel_slug}'")

        token = _render_all_channel_slug.set(channel_slug)
        try:
            drive = _create_drive_client(env)
        finally:
            _render_all_channel_slug.reset(token)

        result = run_preflight_for_job(conn, env, job_id, drive=drive)
        if not result.ok:
            raise RuntimeError("ui render preflight failed")

        draft = dbm.get_ui_job_draft(conn, job_id)
        if not draft:
            return {"job_id": str(job_id), "error": {"code": "UIJ_JOB_NOT_FOUND", "message": "UI job not found"}}

        enqueue_result = enqueue_ui_render_job(
            conn,
            job_id=job_id,
            channel_id=int(draft["channel_id"]),
            tracks=list(result.resolved.get("tracks") or []),
            background_file_id=str(result.resolved.get("background_file_id") or ""),
            background_filename=str(result.resolved.get("background_filename") or ""),
            cover_file_id=str(result.resolved.get("cover_file_id") or ""),
            cover_filename=str(result.resolved.get("cover_filename") or ""),
        )

        if enqueue_result.reason == "already_in_progress":
            return {"job_id": str(job_id), "enqueued": False, "message": "Already in progress"}

        if enqueue_result.reason == "not_allowed":
            return {
                "job_id": str(job_id),
                "error": {"code": "UIJ_RENDER_NOT_ALLOWED", "message": "Status not allowed"},
            }

        if enqueue_result.reason == "not_found":
            return {"job_id": str(job_id), "error": {"code": "UIJ_JOB_NOT_FOUND", "message": "UI job not found"}}

        if enqueue_result.enqueued:
            return {"job_id": str(job_id), "enqueued": True}

        raise RuntimeError(f"unexpected enqueue result: {enqueue_result.reason}")
    except Exception:
        logger.exception("ui render selected item failed", extra={"job_id": job_id_text, "stage": "enqueue_selected"})
        raise
    finally:
        conn.close()


@app.post("/v1/ui/jobs/render_selected")
def api_ui_jobs_render_selected(payload: UiJobsRenderSelectedPayload, _: bool = Depends(require_basic_auth(env))):
    try:
        if not payload.job_ids:
            return _uij_error(400, "UIJ_INVALID_INPUT", "job_ids is required and must be non-empty")

        results = [_render_selected_item(job_id_text) for job_id_text in payload.job_ids]
        enqueued_count = sum(1 for item in results if item.get("enqueued") is True)
        noop_count = sum(1 for item in results if item.get("enqueued") is False)
        failed_count = sum(1 for item in results if item.get("error"))
        return {
            "results": results,
            "summary": {
                "requested": len(payload.job_ids),
                "enqueued": enqueued_count,
                "noop": noop_count,
                "failed": failed_count,
            },
        }
    except Exception:
        logger.exception("render_selected internal error", extra={"stage": "enqueue_selected_batch"})
        return _uij_error(500, "UIJ_INTERNAL", "Internal error")


@app.post("/v1/ui/jobs/render_all")
def api_ui_jobs_render_all(_: bool = Depends(require_basic_auth(env))):
    blocked = _disk_guard_write_heavy(operation="ui_jobs_render_all")
    if blocked is not None:
        return blocked
    conn = dbm.connect(env)
    try:
        rows = conn.execute(
            """
            SELECT j.id
            FROM jobs j
            WHERE j.job_type='UI' AND j.state='DRAFT'
            ORDER BY j.created_at ASC
            """
        ).fetchall()
        # drive = _create_drive_client(env)
        enqueued = 0
        failed = 0
        skipped_jobs: list[dict[str, Any]] = []
        for r in rows:
            job_id = int(r["id"])
            try:
                job = dbm.get_job(conn, job_id)
                channel_slug = str(job.get("channel_slug") or "") if job else ""
                token_path = oauth_token_path(base_dir=env.gdrive_tokens_dir, channel_slug=channel_slug)
                if (not channel_slug) or (not token_path.is_file()):
                    skipped_jobs.append(
                        {
                            "job_id": job_id,
                            "channel_slug": channel_slug,
                            "reason": (
                                f"GDrive token missing for channel '{channel_slug}'. "
                                "Generate/Regenerate Drive Token in dashboard."
                            ),
                        }
                    )
                    continue
                token = _render_all_channel_slug.set(channel_slug)
                try:
                    drive = _create_drive_client(env)
                finally:
                    _render_all_channel_slug.reset(token)

                result = run_preflight_for_job(conn, env, job_id, drive=drive)
                if not result.ok:
                    failed += 1
                    continue

                draft = dbm.get_ui_job_draft(conn, job_id)
                if not draft:
                    failed += 1
                    continue

                channel_id = int(draft["channel_id"])
                tracks = list(result.resolved.get("tracks") or [])
                bg_id = str(result.resolved.get("background_file_id") or "")
                bg_name = str(result.resolved.get("background_filename") or "")
                cover_id = str(result.resolved.get("cover_file_id") or "")
                cover_name = str(result.resolved.get("cover_filename") or "")

                enqueue_result = enqueue_ui_render_job(
                    conn,
                    job_id=job_id,
                    channel_id=channel_id,
                    tracks=tracks,
                    background_file_id=bg_id,
                    background_filename=bg_name,
                    cover_file_id=cover_id,
                    cover_filename=cover_name,
                )
                if enqueue_result.enqueued:
                    enqueued += 1
            except Exception as exc:
                failed += 1
                error_reason = f"render_all: {exc}".strip()[:500]
                dbm.update_job_state(conn, job_id, state="DRAFT", stage="DRAFT", error_reason=error_reason)
    finally:
        conn.close()
    return {
        "enqueued_count": enqueued,
        "failed_count": failed,
        "skipped_count": len(skipped_jobs),
        "skipped_jobs": skipped_jobs,
    }


@app.post("/v1/ui/jobs/{job_id}/render")
def api_ui_job_render(job_id: int, _: bool = Depends(require_basic_auth(env))):
    blocked = _disk_guard_write_heavy(operation="ui_jobs_render")
    if blocked is not None:
        return blocked
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        guard = check_ui_render_guard(conn, job_id=job_id)
        if guard.reason == "not_found":
            return _uij_error(404, "UIJ_JOB_NOT_FOUND", "UI job not found")
        if guard.reason == "not_allowed":
            return _uij_error(409, "UIJ_RENDER_NOT_ALLOWED", "Render is allowed only for Draft jobs")
        if guard.reason == "already_in_progress":
            return {"job_id": str(job_id), "enqueued": False, "message": "Already in progress"}

        if not job:
            return _uij_error(404, "UIJ_JOB_NOT_FOUND", "UI job not found")

        channel_slug = str(job.get("channel_slug") or "")
        token_path = oauth_token_path(base_dir=env.gdrive_tokens_dir, channel_slug=channel_slug)
        if (not channel_slug) or (not token_path.is_file()):
            raise RuntimeError(f"missing gdrive token for channel '{channel_slug}'")

        token = _render_all_channel_slug.set(channel_slug)
        try:
            drive = _create_drive_client(env)
        finally:
            _render_all_channel_slug.reset(token)

        result = run_preflight_for_job(conn, env, job_id, drive=drive)
        if not result.ok:
            raise RuntimeError("ui render preflight failed")

        draft = dbm.get_ui_job_draft(conn, job_id)
        if not draft:
            return _uij_error(404, "UIJ_JOB_NOT_FOUND", "UI job not found")

        enqueue_result = enqueue_ui_render_job(
            conn,
            job_id=job_id,
            channel_id=int(draft["channel_id"]),
            tracks=list(result.resolved.get("tracks") or []),
            background_file_id=str(result.resolved.get("background_file_id") or ""),
            background_filename=str(result.resolved.get("background_filename") or ""),
            cover_file_id=str(result.resolved.get("cover_file_id") or ""),
            cover_filename=str(result.resolved.get("cover_filename") or ""),
        )

        if enqueue_result.reason == "already_in_progress":
            return {"job_id": str(job_id), "enqueued": False, "message": "Already in progress"}

        if not enqueue_result.enqueued:
            if enqueue_result.reason == "not_allowed":
                return _uij_error(409, "UIJ_RENDER_NOT_ALLOWED", "Render is allowed only for Draft jobs")
            if enqueue_result.reason == "not_found":
                return _uij_error(404, "UIJ_JOB_NOT_FOUND", "UI job not found")
            raise RuntimeError(f"unexpected enqueue result: {enqueue_result.reason}")

        return {"job_id": str(job_id), "enqueued": True, "message": "Render enqueued"}
    except Exception:
        logger.exception("ui render enqueue failed", extra={"job_id": job_id, "stage": "enqueue"})
        return _uij_error(500, "UIJ_ENQUEUE_FAILED", "Failed to enqueue render")
    finally:
        conn.close()


@app.post("/v1/ui/jobs/{job_id}/retry")
def api_ui_job_retry(job_id: int, _: bool = Depends(require_basic_auth(env))):
    blocked = _disk_guard_write_heavy(operation="ui_jobs_retry")
    if blocked is not None:
        return blocked
    logger.info("ui.jobs.retry.request", extra={"job_id": job_id, "stage": "request"})
    conn = dbm.connect(env)
    try:
        result = retry_failed_ui_job(conn, source_job_id=job_id)
        retry_job = dbm.get_job(conn, result.retry_job_id)
        attempt_no = int(retry_job.get("attempt_no") or 1) if retry_job else 1
    except UiJobRetryNotFoundError:
        logger.info("ui.jobs.retry.error", extra={"job_id": job_id, "stage": "not_found"})
        return _uij_error(404, "UIJ_JOB_NOT_FOUND", "UI job not found")
    except UiJobRetryStatusError:
        logger.info("ui.jobs.retry.error", extra={"job_id": job_id, "stage": "not_allowed"})
        return _uij_error(409, "UIJ_RETRY_NOT_ALLOWED", "Retry is allowed only for Failed jobs")
    except Exception as exc:
        message = str(exc)
        if "retry enqueue integration failed" in message or message.lower() == "enqueue failed":
            logger.exception("ui.jobs.retry.error", extra={"job_id": job_id, "stage": "enqueue"})
            return _uij_error(500, "UIJ_RETRY_ENQUEUE_FAILED", "Failed to enqueue retry")
        logger.exception("ui.jobs.retry.error", extra={"job_id": job_id, "stage": "internal"})
        return _uij_error(500, "UIJ_RETRY_INTERNAL", "Internal error")
    finally:
        conn.close()

    payload = {
        "source_job_id": str(job_id),
        "retry_job_id": str(result.retry_job_id),
        "attempt_no": attempt_no,
        "enqueued": bool(result.created),
        "message": "Retry enqueued" if result.created else "Retry already created",
    }
    if result.created:
        logger.info(
            "ui.jobs.retry.created",
            extra={"job_id": job_id, "retry_job_id": result.retry_job_id, "stage": "created"},
        )
    else:
        logger.info(
            "ui.jobs.retry.noop",
            extra={"job_id": job_id, "retry_job_id": result.retry_job_id, "stage": "noop"},
        )
    return payload


@app.get("/v1/ui/jobs/{job_id}")
def api_get_ui_job(job_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        d = dbm.get_ui_job_draft(conn, job_id)
        if not d:
            raise HTTPException(404)
    finally:
        conn.close()
    return {"draft": d}


@app.post("/v1/ui/jobs/{job_id}")
def api_update_ui_job(job_id: int, payload: UiJobDraftPayload, _: bool = Depends(require_basic_auth(env))):
    errors = _ui_validate(payload)
    if errors:
        raise HTTPException(422, {"field_errors": errors})

    conn = dbm.connect(env)
    try:
        d = dbm.get_ui_job_draft(conn, job_id)
        if not d:
            raise HTTPException(404)

        job = dbm.get_job(conn, job_id)
        if not job:
            raise HTTPException(404)
        if str(job.get("state") or "") != "DRAFT":
            raise HTTPException(409, "only DRAFT jobs can be edited")

        if int(d["channel_id"]) != payload.channel_id:
            raise HTTPException(409, "project/channel_id is immutable")

        dbm.update_ui_job_draft(
            conn,
            job_id=job_id,
            title=payload.title.strip(),
            description=payload.description.strip(),
            tags_csv=payload.tags_csv.strip(),
            cover_name=payload.cover_name.strip() or None,
            cover_ext=payload.cover_ext.strip() or None,
            background_name=payload.background_name.strip(),
            background_ext=payload.background_ext.strip(),
            audio_ids_text=payload.audio_ids_text.strip(),
        )
    finally:
        conn.close()
    return {"ok": True}


@app.post("/v1/ui/jobs/{job_id}/preflight")
def api_ui_job_preflight(job_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        result = run_preflight_for_job(conn, env, job_id)
    finally:
        conn.close()
    return {
        "ok": result.ok,
        "field_errors": result.field_errors,
        "resolved": result.resolved,
    }


@app.get("/ui/jobs/create", response_class=HTMLResponse)
def ui_jobs_create_page(request: Request, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        channels = _all_channels(conn)
    finally:
        conn.close()
    return templates.TemplateResponse(
        "ui_job_form.html",
        {
            "request": request,
            "mode": "create",
            "channels": channels,
            "field_errors": {},
            "form": {},
            "job_id": None,
            "release_id": None,
            "locked": False,
        },
    )


@app.get("/ui/jobs/create/")
def ui_jobs_create_page_trailing_slash(_: bool = Depends(require_basic_auth(env))):
    return RedirectResponse(url="/ui/jobs/create", status_code=307)


@app.post("/ui/jobs/create")
async def ui_jobs_create_submit(
    request: Request,
    _: bool = Depends(require_basic_auth(env)),
):
    # parse x-www-form-urlencoded without python-multipart dependency
    import urllib.parse

    raw_body = (await request.body()).decode("utf-8")
    raw = urllib.parse.parse_qs(raw_body)
    getv = lambda k: (raw.get(k, [""])[0] if raw.get(k) else "")

    raw_channel_id = getv("channel_id")
    try:
        channel_id = int(raw_channel_id or "0")
    except (TypeError, ValueError):
        channel_id = 0
    title = getv("title")
    description = getv("description")
    tags_csv = getv("tags_csv")
    cover_name = getv("cover_name")
    cover_ext = getv("cover_ext")
    background_name = getv("background_name")
    background_ext = getv("background_ext")
    audio_ids_text = getv("audio_ids_text")

    payload = _build_ui_payload(
        channel_id=channel_id,
        title=title,
        description=description,
        tags_csv=tags_csv,
        cover_name=cover_name,
        cover_ext=cover_ext,
        background_name=background_name,
        background_ext=background_ext,
        audio_ids_text=audio_ids_text,
    )
    errors = _ui_validate(payload)
    conn = dbm.connect(env)
    try:
        channels = _all_channels(conn)
        if errors:
            return templates.TemplateResponse(
                "ui_job_form.html",
                {
                    "request": request,
                    "mode": "create",
                    "channels": channels,
                    "field_errors": errors,
                    "form": payload.model_dump(),
                    "job_id": None,
                    "release_id": None,
                    "locked": False,
                },
                status_code=422,
            )

        channel = dbm.get_channel_by_id(conn, payload.channel_id)
        if not channel:
            return templates.TemplateResponse(
                "ui_job_form.html",
                {
                    "request": request,
                    "mode": "create",
                    "channels": channels,
                    "field_errors": {"project": ["project is invalid"]},
                    "form": payload.model_dump(),
                    "job_id": None,
                    "release_id": None,
                    "locked": False,
                },
                status_code=422,
            )

        job_id = dbm.create_ui_job_draft(
            conn,
            channel_id=payload.channel_id,
            title=payload.title.strip(),
            description=payload.description.strip(),
            tags_csv=payload.tags_csv.strip(),
            cover_name=payload.cover_name.strip() or None,
            cover_ext=payload.cover_ext.strip() or None,
            background_name=payload.background_name.strip(),
            background_ext=payload.background_ext.strip(),
            audio_ids_text=payload.audio_ids_text.strip(),
        )
        created_job = dbm.get_job(conn, job_id)
        release_id = int(created_job["release_id"]) if created_job and created_job.get("release_id") is not None else None
        preflight = run_preflight_for_job(conn, env, job_id)
    finally:
        conn.close()

    return templates.TemplateResponse(
        "ui_job_form.html",
        {
            "request": request,
            "mode": "edit",
            "channels": channels,
            "field_errors": preflight.field_errors,
            "form": payload.model_dump(),
            "job_id": job_id,
            "release_id": release_id,
            "locked": False,
        },
    )


@app.get("/ui/jobs/{job_id}/edit", response_class=HTMLResponse)
def ui_jobs_edit_page(job_id: int, request: Request, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        draft = dbm.get_ui_job_draft(conn, job_id)
        job = dbm.get_job(conn, job_id)
        channels = _all_channels(conn)
        if not draft or not job:
            raise HTTPException(404)
        release_id = int(job["release_id"]) if job.get("release_id") is not None else None
        locked = str(job.get("state") or "") != "DRAFT"
    finally:
        conn.close()
    return templates.TemplateResponse(
        "ui_job_form.html",
        {
            "request": request,
            "mode": "edit",
            "channels": channels,
            "field_errors": {},
            "form": draft,
            "job_id": job_id,
            "release_id": release_id,
            "locked": locked,
        },
    )


@app.get("/ui/jobs/{job_id}/edit/")
def ui_jobs_edit_page_trailing_slash(job_id: int, _: bool = Depends(require_basic_auth(env))):
    return RedirectResponse(url=f"/ui/jobs/{job_id}/edit", status_code=307)


@app.post("/ui/jobs/{job_id}/edit")
async def ui_jobs_edit_submit(
    job_id: int,
    request: Request,
    _: bool = Depends(require_basic_auth(env)),
):
    import urllib.parse

    raw_body = (await request.body()).decode("utf-8")
    raw = urllib.parse.parse_qs(raw_body)
    getv = lambda k: (raw.get(k, [""])[0] if raw.get(k) else "")
    raw_channel_id = getv("channel_id")
    try:
        channel_id = int(raw_channel_id or "0")
    except (TypeError, ValueError):
        channel_id = 0
    title = getv("title")
    description = getv("description")
    tags_csv = getv("tags_csv")
    cover_name = getv("cover_name")
    cover_ext = getv("cover_ext")
    background_name = getv("background_name")
    background_ext = getv("background_ext")
    audio_ids_text = getv("audio_ids_text")
    payload = _build_ui_payload(
        channel_id=channel_id,
        title=title,
        description=description,
        tags_csv=tags_csv,
        cover_name=cover_name,
        cover_ext=cover_ext,
        background_name=background_name,
        background_ext=background_ext,
        audio_ids_text=audio_ids_text,
    )
    conn = dbm.connect(env)
    try:
        channels = _all_channels(conn)
        draft = dbm.get_ui_job_draft(conn, job_id)
        job = dbm.get_job(conn, job_id)
        if not draft or not job:
            raise HTTPException(404)
        if str(job.get("state") or "") != "DRAFT":
            raise HTTPException(409, "only DRAFT jobs can be edited")
        errors = _ui_validate(payload)
        if errors:
            return templates.TemplateResponse(
                "ui_job_form.html",
                {
                    "request": request,
                    "mode": "edit",
                    "channels": channels,
                    "field_errors": errors,
                    "form": payload.model_dump(),
                    "job_id": job_id,
                    "release_id": int(job["release_id"]) if job.get("release_id") is not None else None,
                    "locked": False,
                },
                status_code=422,
            )

        channel = dbm.get_channel_by_id(conn, payload.channel_id)
        if not channel:
            return templates.TemplateResponse(
                "ui_job_form.html",
                {
                    "request": request,
                    "mode": "edit",
                    "channels": channels,
                    "field_errors": {"project": ["project is invalid"]},
                    "form": payload.model_dump(),
                    "job_id": job_id,
                    "release_id": int(job["release_id"]) if job.get("release_id") is not None else None,
                    "locked": False,
                },
                status_code=422,
            )

        if int(draft["channel_id"]) != payload.channel_id:
            raise HTTPException(409, "project/channel_id is immutable")

        dbm.update_ui_job_draft(
            conn,
            job_id=job_id,
            title=payload.title.strip(),
            description=payload.description.strip(),
            tags_csv=payload.tags_csv.strip(),
            cover_name=payload.cover_name.strip() or None,
            cover_ext=payload.cover_ext.strip() or None,
            background_name=payload.background_name.strip(),
            background_ext=payload.background_ext.strip(),
            audio_ids_text=payload.audio_ids_text.strip(),
        )
        preflight = run_preflight_for_job(conn, env, job_id)
        release_id = int(job["release_id"]) if job.get("release_id") is not None else None
    finally:
        conn.close()

    return templates.TemplateResponse(
        "ui_job_form.html",
        {
            "request": request,
            "mode": "edit",
            "channels": channels,
            "field_errors": preflight.field_errors,
            "form": payload.model_dump(),
            "job_id": job_id,
            "release_id": release_id,
            "locked": False,
        },
    )


@app.post("/ui/jobs/render_all")
def ui_jobs_render_all(_: bool = Depends(require_basic_auth(env))):
    api_ui_jobs_render_all(True)
    return RedirectResponse(url="/", status_code=303)

# --- MF6 analytics center pages/contracts (Slices 1-2) ---


def _analytics_nav_spine() -> list[dict[str, str]]:
    return [
        {"key": "OVERVIEW", "label": "Overview", "path": "/v1/analytics/overview"},
        {"key": "CHANNELS", "label": "Channels", "path": "/v1/analytics/channels"},
        {"key": "RELEASES_VIDEOS", "label": "Releases/Videos", "path": "/v1/analytics/releases"},
        {"key": "BATCH_MONTH", "label": "Batch/Month", "path": "/v1/analytics/batches"},
        {"key": "ANOMALIES", "label": "Anomalies", "path": "/v1/analytics/anomalies"},
        {"key": "RECOMMENDATIONS", "label": "Recommendations", "path": "/v1/analytics/recommendations"},
        {"key": "REPORTS_EXPORTS", "label": "Reports/Exports", "path": "/v1/analytics/reports"},
    ]


def _build_page(conn: Any, scope: str, raw_filters: dict[str, Any], *, scope_ref: str | None = None, summary_cards: list[dict[str, Any]], detail_blocks: list[dict[str, Any]], anomaly_markers: list[dict[str, Any]], recommendation_summary: list[dict[str, Any]], available_actions: list[dict[str, Any]], export_actions: list[dict[str, Any]]) -> dict[str, Any]:
    from services.analytics_center.ui_contracts import (
        build_analytics_page_contract,
        normalize_analytics_filters,
    )
    from services.analytics_center.page_aggregations import compute_page_freshness

    applied = normalize_analytics_filters(raw_filters)
    freshness, coverage = compute_page_freshness(conn, page_scope=scope, scope_ref=scope_ref)
    page = build_analytics_page_contract(
        page_scope=scope,
        applied_filters=applied,
        freshness_summary=freshness,
        source_coverage_summary=coverage,
        summary_cards=summary_cards,
        detail_blocks=detail_blocks,
        anomaly_risk_markers=anomaly_markers,
        recommendation_summary=recommendation_summary,
        available_actions=available_actions,
        export_report_actions=export_actions,
    )
    page["navigation"] = _analytics_nav_spine()
    return page


def _compute_freshness_summary(conn: Any, *, page_scope: str, scope_ref: Any | None = None) -> dict[str, Any]:
    from services.analytics_center.page_aggregations import compute_page_freshness

    normalized_scope = {"BATCH_MONTH": "BATCH_MONTH", "OVERVIEW": "OVERVIEW", "CHANNEL": "CHANNEL", "RELEASE": "RELEASE", "ANOMALIES": "ANOMALIES", "RECOMMENDATIONS": "RECOMMENDATIONS", "REPORTS_EXPORTS": "REPORTS_EXPORTS"}.get(str(page_scope or "").upper(), "OVERVIEW")
    freshness, _ = compute_page_freshness(conn, page_scope=normalized_scope, scope_ref=None if scope_ref is None else str(scope_ref))
    return freshness




def _analytics_filter_error(code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=422, content={"error": {"code": code, "message": message}})

@app.get("/v1/analytics/filter-contract")
def api_analytics_filter_contract(_: bool = Depends(require_basic_auth(env))):
    return {
        "shared_filters": [
            "channel", "release_video", "batch_month", "time_window", "anomaly_risk_status",
            "recommendation_family", "severity", "confidence", "freshness", "source_family",
            "target_domain", "report_export_type",
        ],
        "navigation": _analytics_nav_spine(),
        "restorable": True,
    }


@app.get("/v1/analytics/channels")
def api_analytics_channels_index(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        rows = conn.execute("SELECT slug FROM channels ORDER BY slug").fetchall()
        return {"items": [str(r["slug"]) for r in rows], "path_template": "/v1/analytics/channels/{channel_slug}"}
    finally:
        conn.close()


@app.get("/v1/analytics/releases")
def api_analytics_releases_index(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        rows = conn.execute("SELECT id FROM releases ORDER BY id DESC LIMIT 50").fetchall()
        return {"items": [int(r["id"]) for r in rows], "path_template": "/v1/analytics/releases/{release_id}"}
    finally:
        conn.close()


@app.get("/v1/analytics/batches")
def api_analytics_batches_index(_: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        rows = conn.execute("SELECT DISTINCT strftime('%Y-%m', COALESCE(planned_at, '')) AS batch_month FROM releases WHERE planned_at IS NOT NULL ORDER BY batch_month DESC LIMIT 24").fetchall()
        return {"items": [str(r["batch_month"]) for r in rows if str(r["batch_month"] or "")], "path_template": "/v1/analytics/batches/{batch_month}"}
    finally:
        conn.close()


@app.get("/v1/analytics/overview")
def api_analytics_overview(_: bool = Depends(require_basic_auth(env)), time_window: str | None = None, freshness: str | None = None):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.page_aggregations import aggregate_overview

        aggregated = aggregate_overview(conn, time_window=time_window, freshness=freshness)
        page = _build_page(
            conn,
            "OVERVIEW",
            {"time_window": time_window, "freshness": freshness},
            summary_cards=aggregated["summary_cards"],
            detail_blocks=aggregated["detail_blocks"],
            anomaly_markers=aggregated["anomaly_risk_markers"],
            recommendation_summary=aggregated["recommendation_summary"],
            available_actions=[{"action": "refresh"}, {"action": "recompute"}],
            export_actions=[{"action": "export_overview", "artifact_types": ["XLSX", "STRUCTURED_REPORT", "API_REPORT"]}],
        )
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="ANALYTICS_PAGE_VIEWED", page_scope="OVERVIEW", action_type="VIEW", filter_payload=page["applied_filters"], freshness_summary=page["freshness_summary"])
    return page


@app.get("/v1/analytics/channels/{channel_slug}")
def api_analytics_channels(channel_slug: str, _: bool = Depends(require_basic_auth(env)), time_window: str | None = None, recommendation_family: str | None = None, severity: str | None = None, confidence: str | None = None):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.page_aggregations import aggregate_scope

        aggregated = aggregate_scope(conn, scope_type="CHANNEL", scope_ref=channel_slug, filters={"time_window": time_window, "recommendation_family": recommendation_family, "severity": severity, "confidence": confidence})
        page = _build_page(
            conn,
            "CHANNEL",
            {"channel": channel_slug, "time_window": time_window, "recommendation_family": recommendation_family, "severity": severity, "confidence": confidence},
            scope_ref=channel_slug,
            summary_cards=aggregated["summary_cards"],
            detail_blocks=aggregated["detail_blocks"],
            anomaly_markers=aggregated["anomaly_risk_markers"],
            recommendation_summary=aggregated["recommendation_summary"],
            available_actions=[{"action": "refresh"}, {"action": "open_anomaly"}, {"action": "open_recommendation"}],
            export_actions=[{"action": "export_channel", "artifact_types": ["XLSX", "STRUCTURED_REPORT", "API_REPORT"]}],
        )
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="ANALYTICS_PAGE_VIEWED", page_scope="CHANNEL", action_type="VIEW", filter_payload=page["applied_filters"], freshness_summary=page["freshness_summary"])
    return page


@app.get("/v1/analytics/releases/{release_id}")
def api_analytics_releases(release_id: int, _: bool = Depends(require_basic_auth(env)), time_window: str | None = None, anomaly_risk_status: str | None = None, recommendation_family: str | None = None, source_family: str | None = None):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.page_aggregations import aggregate_scope
        from services.analytics_center.reporting import validate_mf6_source_family_filter
        from services.analytics_center.errors import AnalyticsDomainError

        try:
            validate_mf6_source_family_filter(context_scope="RELEASE", source_family=source_family)
        except AnalyticsDomainError as exc:
            return _analytics_filter_error(exc.code, exc.message)

        aggregated = aggregate_scope(conn, scope_type="RELEASE", scope_ref=str(release_id), filters={"time_window": time_window, "anomaly_risk_status": anomaly_risk_status, "recommendation_family": recommendation_family, "source_family": source_family})
        page = _build_page(
            conn,
            "RELEASE",
            {"release_video": str(release_id), "time_window": time_window, "anomaly_risk_status": anomaly_risk_status, "recommendation_family": recommendation_family, "source_family": source_family},
            scope_ref=str(release_id),
            summary_cards=aggregated["summary_cards"],
            detail_blocks=aggregated["detail_blocks"],
            anomaly_markers=aggregated["anomaly_risk_markers"],
            recommendation_summary=aggregated["recommendation_summary"],
            available_actions=[{"action": "inspect_anomaly"}, {"action": "open_related_domain"}],
            export_actions=[{"action": "export_release", "artifact_types": ["XLSX", "STRUCTURED_REPORT", "API_REPORT"]}],
        )
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="ANALYTICS_PAGE_VIEWED", page_scope="RELEASE", action_type="VIEW", filter_payload=page["applied_filters"], freshness_summary=page["freshness_summary"])
    return page


@app.get("/v1/analytics/batches/{batch_month}")
def api_analytics_batches(batch_month: str, _: bool = Depends(require_basic_auth(env)), channel: str | None = None, time_window: str | None = None, anomaly_risk_status: str | None = None, recommendation_family: str | None = None):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.page_aggregations import aggregate_scope

        aggregated = aggregate_scope(conn, scope_type="BATCH_MONTH", scope_ref=batch_month, filters={"time_window": time_window, "anomaly_risk_status": anomaly_risk_status, "recommendation_family": recommendation_family, "channel": channel})
        page = _build_page(
            conn,
            "BATCH_MONTH",
            {"batch_month": batch_month, "channel": channel, "time_window": time_window, "anomaly_risk_status": anomaly_risk_status, "recommendation_family": recommendation_family},
            scope_ref=batch_month,
            summary_cards=aggregated["summary_cards"],
            detail_blocks=aggregated["detail_blocks"],
            anomaly_markers=aggregated["anomaly_risk_markers"],
            recommendation_summary=aggregated["recommendation_summary"],
            available_actions=[{"action": "refresh"}, {"action": "recompute"}],
            export_actions=[{"action": "export_batch", "artifact_types": ["XLSX", "STRUCTURED_REPORT", "API_REPORT"]}],
        )
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="ANALYTICS_PAGE_VIEWED", page_scope="BATCH_MONTH", action_type="VIEW", filter_payload=page["applied_filters"], freshness_summary=page["freshness_summary"])
    return page


@app.get("/v1/analytics/anomalies")
def api_analytics_anomalies(_: bool = Depends(require_basic_auth(env)), scope_type: str | None = None, severity: str | None = None, confidence: str | None = None, recommendation_family: str | None = None, target_domain: str | None = None):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.page_aggregations import aggregate_anomalies

        aggregated = aggregate_anomalies(conn, filters={"scope_type": scope_type, "severity": severity, "confidence": confidence, "recommendation_family": recommendation_family, "target_domain": target_domain})
        page = _build_page(
            conn,
            "ANOMALIES",
            {"scope_type": scope_type, "severity": severity, "confidence": confidence, "recommendation_family": recommendation_family, "target_domain": target_domain},
            summary_cards=aggregated["summary_cards"],
            detail_blocks=aggregated["detail_blocks"],
            anomaly_markers=aggregated["anomaly_risk_markers"],
            recommendation_summary=aggregated["recommendation_summary"],
            available_actions=[{"action": "inspect_anomaly"}, {"action": "open_related_domain"}],
            export_actions=[],
        )
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="ANOMALY_INSPECTED", page_scope="ANOMALIES", action_type="INSPECT_ANOMALY", filter_payload=page["applied_filters"], freshness_summary=page["freshness_summary"])
    return page


@app.get("/v1/analytics/recommendations")
def api_analytics_recommendations(_: bool = Depends(require_basic_auth(env)), scope_type: str | None = None, recommendation_family: str | None = None, target_domain: str | None = None, severity: str | None = None, confidence: str | None = None, lifecycle_status: str | None = None):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.page_aggregations import aggregate_recommendations

        aggregated = aggregate_recommendations(conn, filters={"scope_type": scope_type, "recommendation_family": recommendation_family, "target_domain": target_domain, "severity": severity, "confidence": confidence, "lifecycle_status": lifecycle_status})
        page = _build_page(
            conn,
            "RECOMMENDATIONS",
            {"scope_type": scope_type, "recommendation_family": recommendation_family, "target_domain": target_domain, "severity": severity, "confidence": confidence, "lifecycle_status": lifecycle_status},
            summary_cards=aggregated["summary_cards"],
            detail_blocks=aggregated["detail_blocks"],
            anomaly_markers=aggregated["anomaly_risk_markers"],
            recommendation_summary=aggregated["recommendation_summary"],
            available_actions=[{"action": "open_next_action_surface"}, {"action": "acknowledge_recommendation"}],
            export_actions=[],
        )
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="RECOMMENDATION_OPENED", page_scope="RECOMMENDATIONS", action_type="OPEN_RECOMMENDATION", filter_payload=page["applied_filters"], freshness_summary=page["freshness_summary"])
    return page


@app.get("/v1/analytics/reports")
def api_analytics_reports(_: bool = Depends(require_basic_auth(env)), report_export_type: str | None = None):
    conn = dbm.connect(env)
    try:
        page = _build_page(
            conn,
            "REPORTS_EXPORTS",
            {"report_export_type": report_export_type},
            summary_cards=[{"card": "reports_exports"}],
            detail_blocks=[{"table": "saved_reports", "rows": []}],
            anomaly_markers=[],
            recommendation_summary=[],
            available_actions=[{"action": "export_download_report"}],
            export_actions=[{"action": "create_report_record"}],
        )
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="ANALYTICS_PAGE_VIEWED", page_scope="REPORTS_EXPORTS", action_type="VIEW_REPORTS", filter_payload=page["applied_filters"], freshness_summary=page["freshness_summary"])
    return page


@app.post("/v1/analytics/reports/request")
def api_analytics_report_request(
    payload: Dict[str, Any],
    _: bool = Depends(require_basic_auth(env)),
):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.reporting import create_report_record

        try:
            report_id = create_report_record(
                conn,
                report_scope_type=str(payload.get("report_scope_type", "")),
                report_scope_ref=payload.get("report_scope_ref"),
                report_family=str(payload.get("report_family", "DEFAULT")),
                filter_payload=dict(payload.get("filter_payload", {})),
                artifact_type=str(payload.get("artifact_type", "")),
                created_by=env.basic_user,
            )
        except Exception as exc:
            _record_analytics_ui_event(event_type="ANALYTICS_REPORT_FAILED", page_scope=str(payload.get("report_scope_type", "OVERVIEW")), action_type="REPORT_FAILED", scope_ref=payload.get("report_scope_ref"), filter_payload=dict(payload.get("filter_payload", {})), artifact_type=str(payload.get("artifact_type", "")), freshness_summary=_compute_freshness_summary(conn, page_scope=str(payload.get("report_scope_type", "OVERVIEW")), scope_ref=payload.get("report_scope_ref")))
            from services.analytics_center.errors import AnalyticsDomainError
            if isinstance(exc, AnalyticsDomainError) and exc.code == "E5A_INVALID_ANALYTICS_FILTER_COMBINATION":
                return _analytics_filter_error(exc.code, exc.message)
            raise HTTPException(422, f"report generation failed: {exc}")
        row = conn.execute("SELECT * FROM analytics_report_records WHERE id = ?", (int(report_id),)).fetchone()
        event_freshness = _compute_freshness_summary(conn, page_scope=str(payload.get("report_scope_type", "OVERVIEW")), scope_ref=payload.get("report_scope_ref"))
        _record_analytics_ui_event(event_type="ANALYTICS_REPORT_REQUESTED", page_scope=str(payload.get("report_scope_type", "OVERVIEW")), action_type="REPORT_REQUEST", scope_ref=payload.get("report_scope_ref"), filter_payload=dict(payload.get("filter_payload", {})), artifact_type=str(payload.get("artifact_type", "")), report_record_id=int(report_id), freshness_summary=event_freshness)
        if row is not None and str(row["generation_status"]) == "READY":
            _record_analytics_ui_event(event_type="ANALYTICS_REPORT_GENERATED", page_scope=str(payload.get("report_scope_type", "OVERVIEW")), action_type="REPORT_GENERATED", scope_ref=payload.get("report_scope_ref"), filter_payload=dict(payload.get("filter_payload", {})), artifact_type=str(payload.get("artifact_type", "")), report_record_id=int(report_id), freshness_summary=event_freshness)
        else:
            _record_analytics_ui_event(event_type="ANALYTICS_REPORT_FAILED", page_scope=str(payload.get("report_scope_type", "OVERVIEW")), action_type="REPORT_FAILED", scope_ref=payload.get("report_scope_ref"), filter_payload=dict(payload.get("filter_payload", {})), artifact_type=str(payload.get("artifact_type", "")), report_record_id=int(report_id), freshness_summary=event_freshness)
        return {"report_record": dict(row), "deduped_or_created_id": int(report_id)}
    finally:
        conn.close()


@app.get("/v1/analytics/reports/records")
def api_analytics_report_records(
    _: bool = Depends(require_basic_auth(env)),
    report_scope_type: str | None = None,
    generation_status: str | None = None,
):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.reporting import list_report_records

        return {"items": list_report_records(conn, report_scope_type=report_scope_type, generation_status=generation_status)}
    finally:
        conn.close()


@app.get("/v1/analytics/reports/{report_record_id}/download")
def api_analytics_report_download(report_record_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        row = conn.execute("SELECT * FROM analytics_report_records WHERE id = ?", (int(report_record_id),)).fetchone()
        if row is None:
            raise HTTPException(404, "report not found")
        if str(row["generation_status"]) != "READY":
            raise HTTPException(422, "report not ready")
        artifact_ref = str(row["artifact_ref"] or "")
        if not artifact_ref or not os.path.exists(artifact_ref):
            raise HTTPException(422, "report artifact missing")
        _record_analytics_ui_event(event_type="EXPORT_DOWNLOADED", page_scope=str(row["report_scope_type"]), action_type="DOWNLOAD", scope_ref=row["report_scope_ref"], artifact_type=str(row["artifact_type"]), report_record_id=int(report_record_id), freshness_summary=_compute_freshness_summary(conn, page_scope=str(row["report_scope_type"]), scope_ref=row["report_scope_ref"]))
        return {"download": True, "report_record_id": int(report_record_id), "artifact_ref": artifact_ref, "artifact_type": row["artifact_type"]}
    finally:
        conn.close()




def _external_sync_http_status(code: str) -> int:
    if code in {"E5A_INVALID_EXTERNAL_SCOPE", "E5A_INVALID_REFRESH_MODE"}:
        return 422
    if code in {"E5A_SYNC_RUN_CONFLICT"}:
        return 409
    return 500


@app.post("/v1/analytics/external/manual-refresh")
def api_analytics_external_manual_refresh(payload: Dict[str, Any], _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.external_sync import request_manual_refresh
        from services.analytics_center.errors import AnalyticsDomainError

        provider_name = "YOUTUBE"
        target_scope_type = str(payload.get("target_scope_type") or "CHANNEL")
        target_scope_ref = str(payload.get("target_scope_ref") or "")
        refresh_mode = str(payload.get("refresh_mode") or "MANUAL_REFRESH")
        force = bool(payload.get("force", False))
        metrics_subset = payload.get("metrics_subset")
        run_id = request_manual_refresh(
            conn,
            target_scope_type=target_scope_type,
            target_scope_ref=target_scope_ref,
            refresh_mode=refresh_mode,
            force=force,
            metrics_subset=metrics_subset if isinstance(metrics_subset, list) else None,
        )
        row = conn.execute("SELECT * FROM analytics_external_sync_runs WHERE id = ?", (int(run_id),)).fetchone()
        return {
            "run_id": int(run_id),
            "provider_name": provider_name,
            "target_scope_type": target_scope_type,
            "target_scope_ref": target_scope_ref,
            "run_mode": str(row["run_mode"]),
            "sync_state": str(row["sync_state"]),
        }
    except Exception as exc:
        from services.analytics_center.errors import AnalyticsDomainError
        if isinstance(exc, AnalyticsDomainError):
            raise HTTPException(status_code=_external_sync_http_status(exc.code), detail={"code": exc.code, "message": exc.message})
        raise
    finally:
        conn.close()


@app.get("/v1/analytics/external/status")
def api_analytics_external_status(target_scope_type: str, target_scope_ref: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.external_sync import get_sync_status
        return get_sync_status(conn, target_scope_type=target_scope_type, target_scope_ref=target_scope_ref)
    finally:
        conn.close()


@app.get("/v1/analytics/external/coverage")
def api_analytics_external_coverage(target_scope_type: str, target_scope_ref: str, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.external_sync import get_coverage_report
        return get_coverage_report(conn, target_scope_type=target_scope_type, target_scope_ref=target_scope_ref)
    finally:
        conn.close()


@app.get("/v1/analytics/external/runs")
def api_analytics_external_runs(target_scope_type: str | None = None, target_scope_ref: str | None = None, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.external_sync import list_sync_runs
        return {"items": list_sync_runs(conn, target_scope_type=target_scope_type, target_scope_ref=target_scope_ref)}
    finally:
        conn.close()


@app.get("/v1/analytics/external/runs/{run_id}")
def api_analytics_external_run_detail(run_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.external_sync import get_sync_run_detail
        row = get_sync_run_detail(conn, run_id=int(run_id))
        if row is None:
            raise HTTPException(status_code=404, detail={"code": "E5A_INVALID_EXTERNAL_SCOPE", "message": "sync run not found"})
        return row
    finally:
        conn.close()

@app.post("/v1/analytics/actions/refresh")
def api_analytics_action_refresh(payload: Dict[str, Any], _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        freshness_summary = _compute_freshness_summary(conn, page_scope=str(payload.get("scope", "OVERVIEW")), scope_ref=payload.get("scope_ref"))
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="REFRESH_TRIGGERED", page_scope=str(payload.get("scope", "OVERVIEW")), action_type="REFRESH", filter_payload=payload, freshness_summary=freshness_summary)
    return {"action": "refresh", "delegated": True, "scope": payload, "mutation": False}


@app.post("/v1/analytics/actions/recompute")
def api_analytics_action_recompute(payload: Dict[str, Any], _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        freshness_summary = _compute_freshness_summary(conn, page_scope=str(payload.get("scope", "OVERVIEW")), scope_ref=payload.get("scope_ref"))
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="RECOMPUTE_TRIGGERED", page_scope=str(payload.get("scope", "OVERVIEW")), action_type="RECOMPUTE", filter_payload=payload, freshness_summary=freshness_summary)
    return {"action": "recompute", "delegated": True, "scope": payload, "mutation": False}


@app.get("/v1/analytics/actions/related-domain-jump")
def api_analytics_related_domain_jump(
    target_domain: str,
    scope_ref: str,
    next_action: str,
    _: bool = Depends(require_basic_auth(env)),
):
    from services.analytics_center.reporting import build_related_domain_jump

    jump = build_related_domain_jump(target_domain=target_domain, scope_ref=scope_ref, next_action=next_action)
    conn = dbm.connect(env)
    try:
        freshness_summary = _compute_freshness_summary(conn, page_scope="RECOMMENDATIONS", scope_ref=scope_ref)
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="RELATED_DOMAIN_JUMP_OPENED", page_scope="RECOMMENDATIONS", action_type="RELATED_DOMAIN_JUMP", scope_ref=scope_ref, filter_payload={"target_domain": target_domain, "next_action": next_action}, freshness_summary=freshness_summary)
    return {"jump": jump, "mutation": False}


@app.post("/v1/analytics/actions/recommendations/{recommendation_id}/acknowledge")
def api_analytics_ack_recommendation(recommendation_id: int, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        from services.analytics_center.recommendation_runtime import update_recommendation_lifecycle

        update_recommendation_lifecycle(conn, recommendation_id=int(recommendation_id), target_status="ACKNOWLEDGED")
        return {"recommendation_id": int(recommendation_id), "status": "ACKNOWLEDGED", "mutation": False}
    finally:
        conn.close()


@app.post("/v1/analytics/actions/anomaly/inspect")
def api_analytics_inspect_anomaly(payload: Dict[str, Any], _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        freshness_summary = _compute_freshness_summary(conn, page_scope="ANOMALIES", scope_ref=None)
    finally:
        conn.close()
    _record_analytics_ui_event(event_type="ANOMALY_INSPECTED", page_scope="ANOMALIES", action_type="INSPECT_ANOMALY", scope_ref=str(payload.get("id") or ""), filter_payload=payload, freshness_summary=freshness_summary)
    return {"action": "inspect_anomaly", "payload": payload, "mutation": False}


def _record_analytics_ui_event(*, event_type: str, page_scope: str, action_type: str, scope_ref: str | None = None, filter_payload: dict[str, Any] | None = None, artifact_type: str | None = None, report_record_id: int | None = None, freshness_summary: dict[str, Any] | None = None) -> None:
    conn = dbm.connect(env)
    try:
        conn.execute(
            """
            INSERT INTO analytics_ui_events(event_type, page_scope, scope_ref, filter_payload_json, artifact_type, report_record_id, action_type, actor, freshness_summary_json, created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event_type,
                page_scope,
                scope_ref,
                json.dumps(filter_payload or {}, sort_keys=True),
                artifact_type,
                report_record_id,
                action_type,
                env.basic_user,
                json.dumps(freshness_summary or {}, sort_keys=True),
                dbm.now_ts(),
            ),
        )
        logger.info(
            "analytics_ui_event page_scope=%s scope_ref=%s filter_payload=%s artifact_type=%s report_record_id=%s action_type=%s actor=%s upstream_freshness_summary=%s",
            page_scope,
            scope_ref or "-",
            json.dumps(filter_payload or {}, sort_keys=True),
            artifact_type or "-",
            report_record_id if report_record_id is not None else -1,
            action_type,
            env.basic_user,
            json.dumps(freshness_summary or {}, sort_keys=True),
        )
    finally:
        conn.close()
