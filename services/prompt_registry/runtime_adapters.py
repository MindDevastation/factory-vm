from __future__ import annotations

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
    "CREATE_BULK_JSON_DRAFT": "BULK_JSON_DRAFT_REQUEST_RECORDED",
    "CREATE_METADATA_REQUEST": "METADATA_REQUEST_RECORDED",
    "CREATE_VISUAL_REQUEST": "VISUAL_REQUEST_RECORDED",
    "CREATE_ANALYTICS_REQUEST": "ANALYTICS_REQUEST_RECORDED",
}


def _controlled_internal_request_adapter(capability_code: str) -> AdapterFn:
    result_code = _SYNC_RESULT_CODE[capability_code]

    def _adapter(payload: dict) -> dict:
        if not isinstance(payload, dict):
            return {"result_code": "INVALID_RUNTIME_PAYLOAD", "secret_safe_message": "Runtime payload must be an object."}
        # These v1 adapters deliberately perform only internal request construction.
        # They do not execute shell commands, call providers, upload to YouTube, or use the network.
        return {
            "result_code": result_code,
            "secret_safe_message": f"{capability_code} internal runtime request completed.",
        }

    return _adapter


def build_default_runtime_adapter_registry() -> RuntimeAdapterRegistry:
    registry = RuntimeAdapterRegistry()
    for capability_code in _SYNC_RESULT_CODE:
        registry.register(capability_code, _controlled_internal_request_adapter(capability_code))
    return registry
