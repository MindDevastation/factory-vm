from __future__ import annotations

import json
import sqlite3
from typing import Callable

AdapterFn = Callable[[dict], dict]


class RuntimeAdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, AdapterFn] = {}

    def register(self, capability_code: str, adapter: AdapterFn) -> None:
        self._adapters[str(capability_code)] = adapter

    def get(self, capability_code: str) -> AdapterFn | None:
        return self._adapters.get(str(capability_code))


_SYNC_RESULT_CODE = {
    "CREATE_BULK_JSON_DRAFT": "BULK_JSON_DRAFT_TARGET_UPDATED",
    "CREATE_METADATA_REQUEST": "METADATA_TARGET_UPDATED",
    "CREATE_VISUAL_REQUEST": "VISUAL_TARGET_UPDATED",
    "CREATE_ANALYTICS_REQUEST": "ANALYTICS_TARGET_UPDATED",
}

_TARGET_KIND = {
    "CREATE_BULK_JSON_DRAFT": "bulk_json_draft",
    "CREATE_METADATA_REQUEST": "metadata_request",
    "CREATE_VISUAL_REQUEST": "visual_request",
    "CREATE_ANALYTICS_REQUEST": "analytics_request",
}


def _controlled_internal_request_adapter(capability_code: str) -> AdapterFn:
    result_code = _SYNC_RESULT_CODE[capability_code]
    target_kind = _TARGET_KIND[capability_code]

    def _adapter(payload: dict) -> dict:
        if not isinstance(payload, dict):
            raise ValueError("Runtime payload must be an object.")
        conn = payload.get("_runtime_conn")
        execution_group_id = int(payload.get("_execution_group_id") or 0)
        execution_attempt_id = int(payload.get("_execution_attempt_id") or 0)
        if not isinstance(conn, sqlite3.Connection) or execution_group_id <= 0 or execution_attempt_id <= 0:
            raise RuntimeError("Runtime adapter missing controlled execution context.")
        artifact_ref = f"prompt-runtime:{target_kind}:{execution_group_id}:{execution_attempt_id}"
        product_target = {
            "artifact_ref": artifact_ref,
            "target_kind": target_kind,
            "capability_code": capability_code,
            "execution_group_id": execution_group_id,
            "execution_attempt_id": execution_attempt_id,
            "status": "RECORDED",
        }
        cur = conn.execute(
            """
            UPDATE prompt_execution_usage
            SET artifact_ref=?, usage_payload_json=json_set(COALESCE(usage_payload_json,'{}'),'$.internal_product_target',json(?)), updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            WHERE execution_group_id=? AND latest_attempt_id=?
            """,
            (artifact_ref, json.dumps(product_target, separators=(",", ":"), sort_keys=True), execution_group_id, execution_attempt_id),
        )
        if cur.rowcount != 1:
            raise RuntimeError("Runtime adapter could not update internal product target.")
        return {
            "result_code": result_code,
            "secret_safe_message": f"{capability_code} internal target recorded.",
            "artifact_ref": artifact_ref,
        }

    return _adapter


def build_default_runtime_adapter_registry() -> RuntimeAdapterRegistry:
    registry = RuntimeAdapterRegistry()
    for capability_code in _SYNC_RESULT_CODE:
        registry.register(capability_code, _controlled_internal_request_adapter(capability_code))
    return registry
