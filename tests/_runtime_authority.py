from __future__ import annotations

import sqlite3

from services.prompt_registry.authoritative_gate import (
    CapabilityGateService,
    OperatorPermissionService,
    RenderValidationService,
    TARGET_SNAPSHOT_RESOLVER_REGISTRY,
    TargetCompatibilityService,
    TargetResolverRegistryService,
)


def runtime_snapshot_payload(*, target_type: str, target_ref: str, state: str = "ready") -> dict:
    return {
        "target_type": target_type,
        "target_ref": target_ref,
        "target_display_label": f"{target_type}:{target_ref}",
        "target_state_code": state,
        "target_exists": True,
        "target_updated_at": "2026-01-01T00:00:00Z",
        "compatibility_inputs": {"state": state},
        "resolver_metadata": {"resolver": "unit_runtime_resolver"},
    }


def register_runtime_resolver(state: str = "ready") -> None:
    TARGET_SNAPSHOT_RESOLVER_REGISTRY.register(
        "unit_runtime_resolver",
        lambda **kwargs: runtime_snapshot_payload(target_type=kwargs["target_type"], target_ref=kwargs["target_ref"], state=state),
    )


def seed_runtime_authorities(
    conn: sqlite3.Connection,
    *,
    operator: str = "operator-1",
    capability: str = "CREATE_BULK_JSON_DRAFT",
    target_type: str = "workflow",
    prompt_record_id: int = 1,
    prompt_version_id: int = 1,
    binding_fingerprint: str = "bf",
    render_hash: str = "rh",
    permission_class: str = "runtime_execute",
    capability_enabled: bool = True,
    capability_status: str = "active",
    operator_enabled: bool = True,
    render_status: str = "passed",
    resolver_enabled: bool = True,
    compatibility_status: str = "allowed",
    register_resolver: bool = True,
) -> None:
    if register_resolver:
        register_runtime_resolver()
    CapabilityGateService(conn).upsert(
        capability,
        {"execution_enabled": capability_enabled, "required_permission_class": "runtime_execute", "status": capability_status},
        updated_by_operator="test",
    )
    OperatorPermissionService(conn).upsert(
        operator,
        {"permission_class": permission_class, "is_enabled": operator_enabled},
        updated_by_operator="test",
    )
    RenderValidationService(conn).record_validation(
        prompt_record_id=prompt_record_id,
        prompt_version_id=prompt_version_id,
        binding_fingerprint=binding_fingerprint,
        render_result_hash=render_hash,
        validation_status=render_status,
        validation_schema_version="v1",
        validator_code="test",
    )
    TargetResolverRegistryService(conn).upsert(
        capability,
        target_type,
        {"resolver_code": "unit_runtime_resolver", "snapshot_schema_version": "v1", "is_enabled": resolver_enabled},
        updated_by_operator="test",
    )
    TargetCompatibilityService(conn).upsert(
        capability,
        target_type,
        {"compatibility_status": compatibility_status, "policy_code": f"test_{compatibility_status}"},
        updated_by_operator="test",
    )
    conn.commit()
