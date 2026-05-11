from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from services.common.env import Env
from services.factory_api.security import require_basic_auth_subject
from services.prompt_registry.authoritative_gate import CapabilityGateService, OperatorPermissionService, RenderValidationService, TargetCompatibilityService, TargetResolverRegistryService

_SECRET_KEYS = ("token", "secret", "password", "api_key", "apikey", "authorization", "bearer", "credential", "private_key")


def _connect(env: Env) -> sqlite3.Connection:
    conn = sqlite3.connect(env.db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): ("[redacted]" if any(secret in str(k).lower() for secret in _SECRET_KEYS) else _sanitize(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, str) and any(secret in value.lower() for secret in _SECRET_KEYS):
        return "[redacted]"
    return value


def _error(code: str, message: str, *, status_code: int) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def _not_found(code: str, subject: str) -> JSONResponse:
    return _error("PROMPT_RUNTIME_AUTHORITY_NOT_FOUND", f"{code} not found: {subject}", status_code=404)


def create_prompt_runtime_authority_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/prompt-runtime", tags=["prompt-runtime-authority"])

    @router.get("/resolvers")
    def list_resolvers(
        capability_code: str | None = None,
        target_type: str | None = None,
        is_enabled: bool | None = None,
        limit: int = 100,
        _: str = Depends(require_basic_auth_subject(env)),
    ):
        conn = _connect(env)
        try:
            rows = TargetResolverRegistryService(conn).list_rows(
                capability_code=capability_code,
                target_type=target_type,
                is_enabled=is_enabled,
                limit=limit,
            )
            return {"items": _sanitize(rows)}
        finally:
            conn.close()

    @router.get("/resolvers/{capability_code}/{target_type}")
    def get_resolver(capability_code: str, target_type: str, _: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            result = TargetResolverRegistryService(conn).evaluate(capability_code, target_type).as_dict()
            if not result["exists"]:
                return _not_found("target_resolver", f"{capability_code}/{target_type}")
            return _sanitize(result)
        finally:
            conn.close()

    @router.put("/resolvers/{capability_code}/{target_type}")
    def put_resolver(capability_code: str, target_type: str, payload: dict[str, Any], operator_subject: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            row = TargetResolverRegistryService(conn).upsert(capability_code, target_type, dict(payload or {}), updated_by_operator=operator_subject)
            conn.commit()
            return _sanitize(row)
        except ValueError as exc:
            conn.rollback()
            return _error("PROMPT_RUNTIME_AUTHORITY_INVALID", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/compatibility")
    def list_compatibility(
        capability_code: str | None = None,
        target_type: str | None = None,
        compatibility_status: str | None = None,
        limit: int = 100,
        _: str = Depends(require_basic_auth_subject(env)),
    ):
        conn = _connect(env)
        try:
            rows = TargetCompatibilityService(conn).list_rows(
                capability_code=capability_code,
                target_type=target_type,
                compatibility_status=compatibility_status,
                limit=limit,
            )
            return {"items": _sanitize(rows)}
        except ValueError as exc:
            return _error("PROMPT_RUNTIME_AUTHORITY_INVALID", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/compatibility/{capability_code}/{target_type}")
    def get_compatibility(capability_code: str, target_type: str, _: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            result = TargetCompatibilityService(conn).evaluate(capability_code, target_type).as_dict()
            if not result["exists"]:
                return _not_found("target_compatibility", f"{capability_code}/{target_type}")
            return _sanitize(result)
        finally:
            conn.close()

    @router.put("/compatibility/{capability_code}/{target_type}")
    def put_compatibility(capability_code: str, target_type: str, payload: dict[str, Any], operator_subject: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            row = TargetCompatibilityService(conn).upsert(capability_code, target_type, dict(payload or {}), updated_by_operator=operator_subject)
            conn.commit()
            return _sanitize(row)
        except ValueError as exc:
            conn.rollback()
            return _error("PROMPT_RUNTIME_AUTHORITY_INVALID", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/render-validations")
    def list_render_validations(
        prompt_version_id: int | None = None,
        binding_fingerprint: str | None = None,
        render_result_hash: str | None = None,
        validation_status: str | None = None,
        limit: int = 100,
        _: str = Depends(require_basic_auth_subject(env)),
    ):
        conn = _connect(env)
        try:
            rows = RenderValidationService(conn).list_validations(
                prompt_version_id=prompt_version_id,
                binding_fingerprint=binding_fingerprint,
                render_result_hash=render_result_hash,
                validation_status=validation_status,
                limit=limit,
            )
            return {"items": _sanitize(rows)}
        except ValueError as exc:
            return _error("PROMPT_RUNTIME_AUTHORITY_INVALID", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/render-validations/latest")
    def get_latest_render_validation(
        prompt_version_id: int,
        binding_fingerprint: str,
        render_result_hash: str,
        _: str = Depends(require_basic_auth_subject(env)),
    ):
        conn = _connect(env)
        try:
            result = RenderValidationService(conn).evaluate(
                prompt_version_id=prompt_version_id,
                binding_fingerprint=binding_fingerprint,
                render_result_hash=render_result_hash,
            )
            return _sanitize(result.as_dict())
        finally:
            conn.close()

    @router.get("/capabilities")
    def list_capabilities(_: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            return {"items": _sanitize(CapabilityGateService(conn).list_rows())}
        finally:
            conn.close()

    @router.get("/capabilities/{capability_code}")
    def get_capability(capability_code: str, _: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            result = CapabilityGateService(conn).evaluate(capability_code).as_dict()
            if not result["exists"]:
                return _not_found("capability", capability_code)
            return _sanitize(result)
        finally:
            conn.close()

    @router.put("/capabilities/{capability_code}")
    def put_capability(capability_code: str, payload: dict[str, Any], operator_subject: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            row = CapabilityGateService(conn).upsert(capability_code, dict(payload or {}), updated_by_operator=operator_subject)
            conn.commit()
            return _sanitize(row)
        except ValueError as exc:
            conn.rollback()
            return _error("PROMPT_RUNTIME_AUTHORITY_INVALID", str(exc), status_code=422)
        finally:
            conn.close()

    @router.get("/operators/{operator_subject}/permissions")
    def get_operator_permission(operator_subject: str, _: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            result = OperatorPermissionService(conn).evaluate(operator_subject).as_dict()
            if not result["exists"]:
                return _not_found("operator_permission", operator_subject)
            return _sanitize(result)
        finally:
            conn.close()

    @router.put("/operators/{operator_subject}/permissions")
    def put_operator_permission(operator_subject: str, payload: dict[str, Any], basic_auth_subject: str = Depends(require_basic_auth_subject(env))):
        conn = _connect(env)
        try:
            row = OperatorPermissionService(conn).upsert(operator_subject, dict(payload or {}), updated_by_operator=basic_auth_subject)
            conn.commit()
            return _sanitize(row)
        except ValueError as exc:
            conn.rollback()
            return _error("PROMPT_RUNTIME_AUTHORITY_INVALID", str(exc), status_code=422)
        finally:
            conn.close()

    return router
