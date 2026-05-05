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
