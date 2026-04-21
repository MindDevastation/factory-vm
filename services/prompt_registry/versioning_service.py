from __future__ import annotations

from typing import Any

from services.prompt_registry.registry_service import PromptRegistryService


class PromptVersioningService:
    """Narrow foundation wrapper for version operations."""

    def __init__(self, registry_service: PromptRegistryService) -> None:
        self._registry_service = registry_service

    def create_version(self, prompt_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        return self._registry_service.create_version(prompt_id, payload)

    def list_versions(self, prompt_id: int) -> list[dict[str, Any]]:
        return self._registry_service.list_versions(prompt_id)

    def get_version(self, version_id: int) -> dict[str, Any]:
        return self._registry_service.get_version(version_id)

    def activate_version(self, version_id: int) -> dict[str, Any]:
        return self._registry_service.activate_version(version_id)
