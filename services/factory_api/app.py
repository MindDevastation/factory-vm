from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from services.common.env import Env
from services.common import db as dbm
from services.factory_api.security import require_basic_auth
from services.common.paths import logs_path, qa_path


env = Env.load()
app = FastAPI(title="Factory VM API", version="0.0.1")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


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


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, _: bool = Depends(require_basic_auth(env))):
    conn = dbm.connect(env)
    try:
        jobs = dbm.list_jobs(conn, limit=200)
    finally:
        conn.close()
    return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})


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
