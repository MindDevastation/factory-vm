from __future__ import annotations

import base64
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.security import require_basic_auth
from services.prompt_registry.contracts import (
    bridge_policy_payload,
    contracts_payload,
    ensure_binding_scope,
    ensure_binding_status,
    ensure_import_mode,
    ensure_usage_event_status,
    ensure_usage_event_type,
)
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


def _parse_bool_query(value: str | None, *, field_name: str, default: bool) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{field_name} must be a boolean")


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

    @router.post("/versions/{version_id}/preview")
    def preview_version(version_id: int, payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).preview_version(version_id, payload)
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        except PromptRegistryValidationError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        except ValueError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/bindings")
    def list_bindings(
        prompt_id: str | None = None,
        binding_scope: str | None = None,
        binding_status: str | None = None,
        _: bool = Depends(require_basic_auth(env)),
    ):
        conn = dbm.connect(env)
        try:
            validated_prompt_id: int | None = None
            if prompt_id is not None:
                try:
                    validated_prompt_id = int(str(prompt_id).strip())
                except (TypeError, ValueError) as exc:
                    raise ValueError("prompt_id must be an integer") from exc
            validated_scope = ensure_binding_scope(binding_scope) if binding_scope is not None else None
            validated_status = ensure_binding_status(binding_status) if binding_status is not None else None
            return {
                "items": PromptRegistryService(conn).list_bindings(
                    prompt_id=validated_prompt_id,
                    binding_scope=validated_scope,
                    binding_status=validated_status,
                )
            }
        except (TypeError, ValueError) as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
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


    @router.post("/resolve-preview")
    def resolve_preview(payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return PromptRegistryService(conn).preview_resolved_prompt(payload)
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
        except PromptRegistryValidationError as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/usage-events")
    def list_usage_events(
        prompt_id: str | None = None,
        version_id: str | None = None,
        event_type: str | None = None,
        status: str | None = None,
        limit: str | None = None,
        _: bool = Depends(require_basic_auth(env)),
    ):
        conn = dbm.connect(env)
        try:
            parsed_prompt_id: int | None = None
            parsed_version_id: int | None = None
            if prompt_id is not None:
                parsed_prompt_id = int(str(prompt_id).strip())
            if version_id is not None:
                parsed_version_id = int(str(version_id).strip())
            parsed_limit = 50
            if limit is not None:
                parsed_limit = int(str(limit).strip())
            if parsed_limit <= 0 or parsed_limit > 200:
                raise ValueError("limit must be between 1 and 200")
            parsed_event_type = ensure_usage_event_type(event_type) if event_type is not None else None
            parsed_status = ensure_usage_event_status(status) if status is not None else None
            return {
                "items": PromptRegistryService(conn).list_usage_events(
                    prompt_id=parsed_prompt_id,
                    version_id=parsed_version_id,
                    event_type=parsed_event_type,
                    status=parsed_status,
                    limit=parsed_limit,
                )
            }
        except (TypeError, ValueError, PromptRegistryValidationError) as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/usage-summary")
    def usage_summary(
        prompt_id: str | None = None,
        version_id: str | None = None,
        event_type: str | None = None,
        _: bool = Depends(require_basic_auth(env)),
    ):
        conn = dbm.connect(env)
        try:
            parsed_prompt_id: int | None = None
            parsed_version_id: int | None = None
            if prompt_id is not None:
                parsed_prompt_id = int(str(prompt_id).strip())
            if version_id is not None:
                parsed_version_id = int(str(version_id).strip())
            parsed_event_type = ensure_usage_event_type(event_type) if event_type is not None else None
            return PromptRegistryService(conn).usage_summary(
                prompt_id=parsed_prompt_id,
                version_id=parsed_version_id,
                event_type=parsed_event_type,
            )
        except (TypeError, ValueError, PromptRegistryValidationError) as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/export")
    def export_registry(
        prompt_id: str | None = None,
        include_inactive: str | None = None,
        include_usage: str | None = None,
        _: bool = Depends(require_basic_auth(env)),
    ):
        conn = dbm.connect(env)
        try:
            parsed_prompt_id: int | None = None
            if prompt_id is not None:
                parsed_prompt_id = int(str(prompt_id).strip())
            parsed_include_inactive = _parse_bool_query(
                include_inactive, field_name="include_inactive", default=True
            )
            parsed_include_usage = _parse_bool_query(include_usage, field_name="include_usage", default=False)
            return PromptRegistryService(conn).export_registry(
                prompt_id=parsed_prompt_id,
                include_inactive=parsed_include_inactive,
                include_usage=parsed_include_usage,
            )
        except PromptRegistryNotFoundError as exc:
            return _error("PROMPT_REGISTRY_NOT_FOUND", str(exc), status_code=404)
        except (TypeError, ValueError, PromptRegistryValidationError) as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.post("/import/preview")
    def import_preview(payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            mode = ensure_import_mode(payload.get("mode"))
            return PromptRegistryService(conn).preview_import(payload.get("payload"), mode=mode)
        except (TypeError, ValueError, PromptRegistryValidationError) as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    @router.post("/import/confirm")
    def import_confirm(payload: dict[str, Any], request: Request, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            mode = ensure_import_mode(payload.get("mode"))
            dry_run = bool(payload.get("dry_run", False))
            return PromptRegistryService(conn).confirm_import(
                payload.get("payload"),
                mode=mode,
                dry_run=dry_run,
                actor=_actor_from_request(request),
            )
        except PromptRegistryConflictError as exc:
            return _error("PROMPT_REGISTRY_CONFLICT", str(exc), status_code=409)
        except (TypeError, ValueError, PromptRegistryValidationError) as exc:
            return _error("PROMPT_REGISTRY_VALIDATION_ERROR", str(exc), status_code=422)
        finally:
            conn.close()

    return router
