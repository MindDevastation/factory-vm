from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

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


env = Env.load()
app = FastAPI(title="Factory VM API", version="0.0.1")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


def _create_drive_client(_env: Env) -> DriveClient:
    return DriveClient(
        service_account_json=_env.gdrive_sa_json,
        oauth_client_json=_env.gdrive_oauth_client_json,
        oauth_token_json=_env.gdrive_oauth_token_json,
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
        drive = _create_drive_client(env)
        enqueued = 0
        failed = 0
        for r in rows:
            job_id = int(r["id"])
            try:
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
        result = run_preflight_for_job(conn, env, job_id, drive=_create_drive_client(env))
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
        preflight = run_preflight_for_job(conn, env, job_id, drive=_create_drive_client(env))
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
        preflight = run_preflight_for_job(conn, env, job_id, drive=_create_drive_client(env))
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
