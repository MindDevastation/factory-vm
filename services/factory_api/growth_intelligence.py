from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.security import require_basic_auth
from services.growth_intelligence.contracts import contracts_payload
from services.growth_intelligence.registry_service import GrowthRegistryService


class FeatureFlagsPayload(BaseModel):
    growth_intelligence_enabled: bool
    planning_digest_enabled: bool
    planner_handoff_enabled: bool
    export_enabled: bool
    assisted_planning_enabled: bool


class BootstrapImportPayload(BaseModel):
    import_source: str = "curated"
    import_mode: str = "upsert"
    actor: str | None = None
    notes_json: dict[str, Any] = Field(default_factory=dict)
    items: list[dict[str, Any]]


def _error(code: str, message: str, status_code: int = 422) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def create_growth_intelligence_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/growth-intelligence", tags=["growth-intelligence"])

    @router.get("/contracts")
    def get_contracts(_: bool = Depends(require_basic_auth(env))):
        return contracts_payload()

    @router.get("/knowledge-items")
    def list_knowledge_items(
        source_class: str | None = None,
        source_trust: str | None = None,
        status: str | None = None,
        q: str | None = None,
        _: bool = Depends(require_basic_auth(env)),
    ):
        conn = dbm.connect(env)
        try:
            svc = GrowthRegistryService(conn)
            return {"items": svc.list_knowledge_items(source_class=source_class, source_trust=source_trust, status=status, q=q)}
        except ValueError as exc:
            return _error("GI_VALIDATION_ERROR", str(exc))
        finally:
            conn.close()

    @router.post("/knowledge-items")
    def create_knowledge_item(payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            svc = GrowthRegistryService(conn)
            return svc.create_knowledge_item(payload)
        except ValueError as exc:
            return _error("GI_VALIDATION_ERROR", str(exc))
        finally:
            conn.close()

    @router.patch("/knowledge-items/{item_id}")
    def patch_knowledge_item(item_id: int, payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            svc = GrowthRegistryService(conn)
            return svc.update_knowledge_item(item_id, payload)
        except ValueError as exc:
            return _error("GI_VALIDATION_ERROR", str(exc), status_code=404 if "not found" in str(exc) else 422)
        finally:
            conn.close()

    @router.get("/playbooks")
    def list_playbooks(_: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return {"items": GrowthRegistryService(conn).list_playbooks()}
        finally:
            conn.close()

    @router.post("/playbooks")
    def create_playbook(payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return GrowthRegistryService(conn).create_playbook(payload)
        except ValueError as exc:
            return _error("GI_VALIDATION_ERROR", str(exc))
        finally:
            conn.close()

    @router.patch("/playbooks/{playbook_id}")
    def patch_playbook(playbook_id: int, payload: dict[str, Any], _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return GrowthRegistryService(conn).update_playbook(playbook_id, payload)
        except ValueError as exc:
            return _error("GI_VALIDATION_ERROR", str(exc), status_code=404 if "not found" in str(exc) else 422)
        finally:
            conn.close()

    @router.get("/channels/{channel_slug}/feature-flags")
    def get_feature_flags(channel_slug: str, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return GrowthRegistryService(conn).get_channel_feature_flags(channel_slug)
        finally:
            conn.close()

    @router.put("/channels/{channel_slug}/feature-flags")
    def put_feature_flags(channel_slug: str, payload: FeatureFlagsPayload, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return GrowthRegistryService(conn).set_channel_feature_flags(channel_slug, payload.model_dump())
        except ValueError as exc:
            return _error("GI_VALIDATION_ERROR", str(exc))
        finally:
            conn.close()

    @router.post("/bootstrap/import")
    def bootstrap_import(payload: BootstrapImportPayload, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            return GrowthRegistryService(conn).bootstrap_import(payload.model_dump())
        except ValueError as exc:
            return _error("GI_BOOTSTRAP_VALIDATION_ERROR", str(exc))
        finally:
            conn.close()

    return router
