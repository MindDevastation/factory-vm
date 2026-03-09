from __future__ import annotations

import base64
import secrets
import time
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from services.common.env import Env


def planner_error(
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    *,
    status_code: int,
    request_id: str | None = None,
) -> JSONResponse:
    payload: dict[str, Any] = {"error": {"code": code, "message": message}}
    if request_id:
        payload["error"]["request_id"] = request_id
    if details is not None:
        payload["error"]["details"] = details
    return JSONResponse(status_code=status_code, content=payload)


def planner_auth_username(request: Request, env: Env) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return ""
    try:
        raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        user, pwd = raw.split(":", 1)
    except Exception:
        return ""
    if secrets.compare_digest(user, env.basic_user) and secrets.compare_digest(pwd, env.basic_pass):
        return user
    return ""


def planner_request_id(request: Request) -> str | None:
    header_rid = request.headers.get("X-Request-Id", "").strip()
    if header_rid:
        return header_rid
    rid = getattr(request.state, "planner_request_id", "")
    if isinstance(rid, str) and rid:
        return rid
    return None


def log_planner_event(
    logger: Any,
    *,
    event_name: str,
    username: str,
    started_at: float,
    status_code: int,
    request_id: str | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> None:
    fields: dict[str, Any] = {
        "event_name": event_name,
        "username": username,
        "duration_ms": int((time.perf_counter() - started_at) * 1000),
        "status_code": status_code,
    }
    if request_id:
        fields["request_id"] = request_id
    if extra_fields:
        fields.update(extra_fields)
    logger.info("planner_event %s", fields)
