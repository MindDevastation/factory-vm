from __future__ import annotations

import html
import json
import re
import secrets
import time
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from services.common.env import Env
from services.common import db as dbm
from services.factory_api.security import require_basic_auth
from services.common.paths import logs_path, qa_path
from services.factory_api.ui_gdrive import run_preflight_for_job
from services.integrations.gdrive import DriveClient
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
app = FastAPI(title="Factory VM API", version="0.0.1")
_render_all_channel_slug: ContextVar[Optional[str]] = ContextVar("render_all_channel_slug", default=None)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


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


def _normalize_display_name(value: str) -> str:
    return value.strip()



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


class ApprovePayload(BaseModel):
    comment: str = Field(default="approved", max_length=500)


class RejectPayload(BaseModel):
    comment: str = Field(min_length=1, max_length=1000)




class CancelPayload(BaseModel):
    reason: str = Field(default='cancelled by user', max_length=500)


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


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        jobs = dbm.list_jobs(conn, limit=200)
    finally:
        conn.close()
    return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})


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
    finally:
        conn.close()
    return {"jobs": jobs}


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
    comment = (payload.comment or "approved").strip() or "approved"
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        if not job:
            raise HTTPException(404)
        if str(job.get("state")) != "WAIT_APPROVAL":
            raise HTTPException(409, "job is not in WAIT_APPROVAL")
        dbm.set_approval(conn, job_id, "APPROVE", comment)
        dbm.update_job_state(conn, job_id, state="APPROVED", stage="APPROVAL")
    finally:
        conn.close()
    return {"ok": True}


@app.post("/v1/jobs/{job_id}/reject")
def api_reject(job_id: int, payload: RejectPayload, _: bool = Depends(require_basic_auth(env))):
    comment = payload.comment.strip()
    conn = dbm.connect(env)
    try:
        job = dbm.get_job(conn, job_id)
        if not job:
            raise HTTPException(404)
        if str(job.get("state")) != "WAIT_APPROVAL":
            raise HTTPException(409, "job is not in WAIT_APPROVAL")
        dbm.set_approval(conn, job_id, "REJECT", comment)
        dbm.update_job_state(conn, job_id, state="REJECTED", stage="APPROVAL")
    finally:
        conn.close()
    return {"ok": True}




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
        job = dbm.get_job(conn, job_id)
        if not job:
            raise HTTPException(404)
        if str(job.get("state")) not in ("APPROVED", "WAIT_APPROVAL"):
            raise HTTPException(409, "job is not in APPROVED/WAIT_APPROVAL")
        ts = dbm.now_ts()
        delete_at = ts + 48 * 3600
        dbm.update_job_state(conn, job_id, state="PUBLISHED", stage="APPROVAL", published_at=ts, delete_mp4_at=delete_at)
    finally:
        conn.close()
    return {"ok": True, "delete_mp4_at": delete_at}


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


@app.post("/v1/ui/jobs/render_all")
def api_ui_jobs_render_all(_: bool = Depends(require_basic_auth(env))):
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
        for r in rows:
            job_id = int(r["id"])
            try:
                job = dbm.get_job(conn, job_id)
                token = _render_all_channel_slug.set(str(job.get("channel_slug") or "") if job else "")
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

                tx_started = False
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    tx_started = True

                    job = conn.execute("SELECT job_type, state FROM jobs WHERE id=?", (job_id,)).fetchone()
                    if not job or str(job.get("job_type") or "") != "UI" or str(job.get("state") or "") != "DRAFT":
                        conn.execute("ROLLBACK")
                        tx_started = False
                        continue

                    has_inputs = conn.execute("SELECT 1 FROM job_inputs WHERE job_id=? LIMIT 1", (job_id,)).fetchone()
                    if has_inputs:
                        conn.execute("ROLLBACK")
                        tx_started = False
                        continue

                    for idx, track in enumerate(tracks):
                        fid = str(track.get("file_id") or "")
                        fname = str(track.get("filename") or "")
                        aid = dbm.create_asset(conn, channel_id=channel_id, kind="AUDIO", origin="GDRIVE", origin_id=fid, name=fname, path=f"gdrive:{fid}")
                        dbm.link_job_input(conn, job_id, aid, "TRACK", idx)

                    bg_aid = dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="GDRIVE", origin_id=bg_id, name=bg_name, path=f"gdrive:{bg_id}")
                    dbm.link_job_input(conn, job_id, bg_aid, "BACKGROUND", 0)

                    if cover_id:
                        c_aid = dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="GDRIVE", origin_id=cover_id, name=cover_name, path=f"gdrive:{cover_id}")
                        dbm.link_job_input(conn, job_id, c_aid, "COVER", 0)

                    dbm.update_job_state(conn, job_id, state="READY_FOR_RENDER", stage="FETCH", error_reason="")
                    conn.execute("COMMIT")
                    tx_started = False
                    enqueued += 1
                except Exception:
                    if tx_started:
                        conn.execute("ROLLBACK")
                    raise
            except Exception as exc:
                failed += 1
                error_reason = f"render_all: {exc}".strip()[:500]
                dbm.update_job_state(conn, job_id, state="DRAFT", stage="DRAFT", error_reason=error_reason)
    finally:
        conn.close()
    return {"enqueued_count": enqueued, "failed_count": failed}


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
            "locked": False,
        },
    )


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
            "locked": locked,
        },
    )


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
            "locked": False,
        },
    )


@app.post("/ui/jobs/render_all")
def ui_jobs_render_all(_: bool = Depends(require_basic_auth(env))):
    api_ui_jobs_render_all(True)
    return RedirectResponse(url="/", status_code=303)
