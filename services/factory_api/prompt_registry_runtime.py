from __future__ import annotations

import base64
import sqlite3
from typing import Any
from urllib.parse import parse_qs

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from services.common.env import Env
from services.factory_api.security import require_basic_auth
from services.prompt_registry.runtime_adapters import RuntimeAdapterRegistry
from services.prompt_registry.runtime_execution import (
    admit_due_prompt_execution_retries,
    cancel_prompt_execution,
    confirm_prompt_execution,
    dispatch_prompt_execution,
    get_prompt_execution_status,
    list_prompt_execution_timeline,
    prepare_prompt_execution_preflight,
    recover_stale_runtime_executions,
    schedule_prompt_execution_retry,
)

RUNTIME_ADAPTER_REGISTRY = RuntimeAdapterRegistry()


_SECRET_KEYS = ("token", "secret", "password", "api_key", "apikey", "authorization", "bearer", "credential", "private_key")


def _error(code: str, message: str, *, status_code: int) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _actor_from_request(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return "unknown"
    try:
        raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        user, _pwd = raw.split(":", 1)
        return user.strip() or "unknown"
    except Exception:
        return "unknown"


def _status_code_for_value_error(exc: ValueError) -> int:
    text = str(exc)
    if "not found" in text.lower():
        return 404
    if "CONFLICT" in text:
        return 409
    return 422


def _sanitize_response(value: Any, *, allow_confirmation_token: bool = False) -> Any:
    if isinstance(value, dict):
        clean: dict[str, Any] = {}
        for key, item in value.items():
            normalized = str(key).lower()
            if normalized == "confirmation_token" and allow_confirmation_token:
                clean[key] = item
            elif normalized == "secret_safe_message":
                clean[key] = _sanitize_response(item, allow_confirmation_token=False)
            elif any(secret_key in normalized for secret_key in _SECRET_KEYS):
                clean[key] = "[redacted]"
            else:
                clean[key] = _sanitize_response(item, allow_confirmation_token=False)
        return clean
    if isinstance(value, list):
        return [_sanitize_response(item, allow_confirmation_token=False) for item in value]
    if isinstance(value, str) and any(secret_key in value.lower() for secret_key in _SECRET_KEYS):
        return "[redacted]"
    return value


def _as_int(payload: dict[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if value is None or str(value).strip() == "":
        raise ValueError(f"missing {field_name}")
    return int(value)


def _connect_runtime_db(env: Env) -> sqlite3.Connection:
    conn = sqlite3.connect(env.db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _redirect_to_runtime_detail(execution_group_id: int) -> RedirectResponse:
    return RedirectResponse(f"/ui/prompt-registry/runtime/{execution_group_id}", status_code=303)


async def _form_payload(request: Request) -> dict[str, Any]:
    parsed = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def create_prompt_registry_runtime_router(env: Env, templates: Jinja2Templates) -> APIRouter:
    router = APIRouter(tags=["prompt-registry-runtime"])

    @router.post("/v1/prompt-registry/runtime/preflight")
    def api_preflight(payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            body = dict(payload or {})
            body["operator_id_or_system_actor"] = _actor_from_request(request)
            out = prepare_prompt_execution_preflight(conn, **body)
            return _sanitize_response(out, allow_confirmation_token=out.get("state") == "CONFIRMATION_REQUIRED")
        except ValueError as exc:
            return _error("PROMPT_RUNTIME_PREFLIGHT_REJECTED", str(exc), status_code=_status_code_for_value_error(exc))
        finally:
            conn.close()

    @router.post("/v1/prompt-registry/runtime/confirm")
    def api_confirm(payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            token = str((payload or {}).get("confirmation_token") or "").strip()
            if not token:
                return _error("PROMPT_RUNTIME_CONFIRM_REJECTED", "confirmation_token is required", status_code=422)
            out = confirm_prompt_execution(
                conn,
                execution_attempt_id=_as_int(payload, "execution_attempt_id"),
                confirmation_token=token,
                operator_id_or_system_actor=_actor_from_request(request),
                reviewed_target_state_hash=str((payload or {}).get("reviewed_target_state_hash") or ""),
            )
            return _sanitize_response(out)
        except ValueError as exc:
            return _error("PROMPT_RUNTIME_CONFIRM_REJECTED", str(exc), status_code=_status_code_for_value_error(exc))
        finally:
            conn.close()

    @router.post("/v1/prompt-registry/runtime/dispatch")
    def api_dispatch(payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            out = dispatch_prompt_execution(
                conn,
                execution_attempt_id=_as_int(payload, "execution_attempt_id"),
                adapter_registry=RUNTIME_ADAPTER_REGISTRY,
                payload=(payload or {}).get("payload") if isinstance((payload or {}).get("payload"), dict) else None,
            )
            return _sanitize_response(out)
        except ValueError as exc:
            return _error("PROMPT_RUNTIME_DISPATCH_REJECTED", str(exc), status_code=_status_code_for_value_error(exc))
        finally:
            conn.close()

    @router.get("/v1/prompt-registry/runtime/status/{execution_group_id}")
    def api_status(execution_group_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            return _sanitize_response(get_prompt_execution_status(conn, execution_group_id=execution_group_id))
        except ValueError as exc:
            return _error("PROMPT_RUNTIME_STATUS_NOT_FOUND", str(exc), status_code=_status_code_for_value_error(exc))
        finally:
            conn.close()

    @router.get("/v1/prompt-registry/runtime/timeline/{execution_group_id}")
    def api_timeline(execution_group_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            return {"items": _sanitize_response(list_prompt_execution_timeline(conn, execution_group_id=execution_group_id))}
        finally:
            conn.close()

    @router.post("/v1/prompt-registry/runtime/retry")
    def api_retry(payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            out = schedule_prompt_execution_retry(
                conn,
                execution_attempt_id=_as_int(payload, "execution_attempt_id"),
                actor=_actor_from_request(request),
                retry_after=(payload or {}).get("retry_after"),
            )
            return _sanitize_response(out)
        except ValueError as exc:
            return _error("PROMPT_RUNTIME_RETRY_REJECTED", str(exc), status_code=_status_code_for_value_error(exc))
        finally:
            conn.close()

    @router.post("/v1/prompt-registry/runtime/admit-due-retries")
    def api_admit_due_retries(payload: dict[str, Any] | None = None, request: Request = None, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            actor = _actor_from_request(request) if request is not None else "system"
            out = admit_due_prompt_execution_retries(conn, now=(payload or {}).get("now"), actor=actor)
            return {"items": _sanitize_response(out)}
        finally:
            conn.close()

    @router.post("/v1/prompt-registry/runtime/cancel")
    def api_cancel(payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            out = cancel_prompt_execution(conn, execution_attempt_id=_as_int(payload, "execution_attempt_id"), actor=_actor_from_request(request))
            return _sanitize_response(out)
        except ValueError as exc:
            return _error("PROMPT_RUNTIME_CANCEL_REJECTED", str(exc), status_code=_status_code_for_value_error(exc))
        finally:
            conn.close()

    @router.post("/v1/prompt-registry/runtime/recover")
    def api_recover(payload: dict[str, Any] | None = None, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            out = recover_stale_runtime_executions(conn, now=(payload or {}).get("now"))
            return {"items": _sanitize_response(out)}
        finally:
            conn.close()

    @router.post("/ui/prompt-registry/runtime/retry")
    async def ui_retry(request: Request, _: bool = Depends(require_basic_auth(env))):
        payload = await _form_payload(request)
        conn = _connect_runtime_db(env)
        try:
            out = schedule_prompt_execution_retry(
                conn,
                execution_attempt_id=_as_int(payload, "execution_attempt_id"),
                actor=_actor_from_request(request),
                retry_after=payload.get("retry_after") or None,
            )
            return _redirect_to_runtime_detail(int(out["execution_group_id"]))
        except ValueError as exc:
            return templates.TemplateResponse(
                "prompt_registry_runtime.html",
                {"request": request, "status": None, "timeline": [], "error": str(exc), "execution_group_id": None},
                status_code=_status_code_for_value_error(exc),
            )
        finally:
            conn.close()

    @router.post("/ui/prompt-registry/runtime/cancel")
    async def ui_cancel(request: Request, _: bool = Depends(require_basic_auth(env))):
        payload = await _form_payload(request)
        conn = _connect_runtime_db(env)
        try:
            out = cancel_prompt_execution(conn, execution_attempt_id=_as_int(payload, "execution_attempt_id"), actor=_actor_from_request(request))
            return _redirect_to_runtime_detail(int(out["execution_group_id"]))
        except ValueError as exc:
            return templates.TemplateResponse(
                "prompt_registry_runtime.html",
                {"request": request, "status": None, "timeline": [], "error": str(exc), "execution_group_id": None},
                status_code=_status_code_for_value_error(exc),
            )
        finally:
            conn.close()

    @router.get("/ui/prompt-registry/runtime", name="prompt_registry_runtime_index")
    def ui_runtime_index(request: Request, execution_group_id: int | None = None, _: bool = Depends(require_basic_auth(env))):
        status = None
        timeline: list[dict[str, Any]] = []
        error = None
        conn = _connect_runtime_db(env)
        try:
            if execution_group_id is not None:
                try:
                    status = _sanitize_response(get_prompt_execution_status(conn, execution_group_id=execution_group_id))
                    timeline = _sanitize_response(list_prompt_execution_timeline(conn, execution_group_id=execution_group_id))
                except ValueError as exc:
                    error = str(exc)
        finally:
            conn.close()
        return templates.TemplateResponse(
            "prompt_registry_runtime.html",
            {"request": request, "status": status, "timeline": timeline, "error": error, "execution_group_id": execution_group_id},
        )

    @router.get("/ui/prompt-registry/runtime/{execution_group_id}", name="prompt_registry_runtime_detail")
    def ui_runtime_detail(request: Request, execution_group_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = _connect_runtime_db(env)
        try:
            try:
                status = _sanitize_response(get_prompt_execution_status(conn, execution_group_id=execution_group_id))
                timeline = _sanitize_response(list_prompt_execution_timeline(conn, execution_group_id=execution_group_id))
                error = None
            except ValueError as exc:
                status = None
                timeline = []
                error = str(exc)
        finally:
            conn.close()
        return templates.TemplateResponse(
            "prompt_registry_runtime.html",
            {"request": request, "status": status, "timeline": timeline, "error": error, "execution_group_id": execution_group_id},
        )

    return router
