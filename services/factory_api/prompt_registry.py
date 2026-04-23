from __future__ import annotations

import base64
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.security import require_basic_auth
from services.prompt_registry.contracts import bridge_policy_payload, contracts_payload
from services.prompt_registry.errors import (
    PromptRegistryConflictError,
    PromptRegistryNotFoundError,
    PromptRegistryValidationError,
)
from services.prompt_registry.registry_service import PromptRegistryService


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


def create_prompt_registry_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/prompt-registry", tags=["prompt-registry"])

    @router.get("/contracts")
    def get_contracts(_: bool = Depends(require_basic_auth(env))):
        return contracts_payload()

    @router.get("/bridge-policy")
    def get_bridge_policy(_: bool = Depends(require_basic_auth(env))):
        return bridge_policy_payload()

    @router.get("/records")
    def list_records(_: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return {"items": PromptRegistryService(conn).list_records()}
        finally:
            conn.close()

    @router.post("/records")
    def create_record(payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).create_record(payload, actor=_actor_from_request(request))
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        except PromptRegistryConflictError as exc:
            return _error("PROMPT_REGISTRY_CONFLICT", str(exc), status_code=409)
        except PromptRegistryValidationError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        except ValueError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/records/{prompt_id}")
    def get_record(prompt_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).get_record(prompt_id)
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        finally:
            conn.close()

    @router.get("/records/{prompt_id}/audit")
    def get_record_audit(prompt_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).get_audit_diagnostics(prompt_id)
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        finally:
            conn.close()

    @router.patch("/records/{prompt_id}")
    def patch_record(prompt_id: int, payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).update_record(prompt_id, payload, actor=_actor_from_request(request))
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        except PromptRegistryConflictError as exc:
            return _error("PROMPT_REGISTRY_CONFLICT", str(exc), status_code=409)
        except PromptRegistryValidationError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        except ValueError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/records/{prompt_id}/versions")
    def list_versions(prompt_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return {"items": PromptRegistryService(conn).list_versions(prompt_id)}
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        finally:
            conn.close()

    @router.post("/records/{prompt_id}/versions")
    def create_version(prompt_id: int, payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).create_version(prompt_id, payload, actor=_actor_from_request(request))
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        except PromptRegistryValidationError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        except ValueError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/versions/{version_id}")
    def get_version(version_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).get_version(version_id)
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        finally:
            conn.close()

    @router.post("/versions/{version_id}/activate")
    def activate_version(version_id: int, request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).activate_version(version_id, actor=_actor_from_request(request))
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        except PromptRegistryValidationError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/bindings")
    def list_bindings(_: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return {"items": PromptRegistryService(conn).list_bindings()}
        finally:
            conn.close()

    @router.post("/bindings")
    def create_binding(payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).create_binding(payload, actor=_actor_from_request(request))
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        except PromptRegistryConflictError as exc:
            return _error("PROMPT_REGISTRY_CONFLICT", str(exc), status_code=409)
        except PromptRegistryValidationError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        except ValueError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.patch("/bindings/{binding_id}")
    def patch_binding(binding_id: int, payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).update_binding_status(binding_id, payload, actor=_actor_from_request(request))
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        except PromptRegistryConflictError as exc:
            return _error("PROMPT_REGISTRY_CONFLICT", str(exc), status_code=409)
        except PromptRegistryValidationError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        except ValueError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.post("/resolve")
    def resolve_prompt(payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).resolve_effective_prompt(payload)
        finally:
            conn.close()

    return router
