from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Callable

from services.common import db as dbm


ALLOWED_BACKGROUND_SOURCE_FAMILIES: tuple[str, ...] = (
    "managed_library",
    "channel_source",
    "operator_imported",
    "known_resolved",
)


class BackgroundSourceAdapterError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class BackgroundCandidate:
    asset_id: int
    source_family: str
    source_reference: str | None
    display_name: str
    selection_mode_prefill: str
    template_assisted: bool
    warnings: list[str]


AdapterFn = Callable[[sqlite3.Connection, int, int], list[BackgroundCandidate]]


class BackgroundSourceAdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, AdapterFn] = {}

    def register(self, source_family: str, adapter: AdapterFn) -> None:
        if source_family not in ALLOWED_BACKGROUND_SOURCE_FAMILIES:
            raise BackgroundSourceAdapterError(
                code="VBG_UNSUPPORTED_SOURCE_FAMILY",
                message=f"Unsupported background source family: {source_family}",
            )
        self._adapters[source_family] = adapter

    def get(self, source_family: str) -> AdapterFn:
        adapter = self._adapters.get(source_family)
        if adapter is None:
            raise BackgroundSourceAdapterError(
                code="VBG_UNSUPPORTED_SOURCE_FAMILY",
                message=f"Unsupported background source family: {source_family}",
            )
        return adapter

    def list_families(self) -> list[str]:
        return [family for family in ALLOWED_BACKGROUND_SOURCE_FAMILIES if family in self._adapters]


def _db_adapter_for_family(source_family: str) -> AdapterFn:
    def _adapter(conn: sqlite3.Connection, release_id: int, channel_id: int) -> list[BackgroundCandidate]:
        rows = dbm.list_background_candidate_assets(
            conn,
            release_id=release_id,
            channel_id=channel_id,
            source_family=source_family,
        )
        out: list[BackgroundCandidate] = []
        for row in rows:
            asset_id = int(row["id"])
            display_name = str(row.get("name") or f"asset-{asset_id}")
            source_reference = str(row.get("origin_id") or f"asset:{asset_id}")
            out.append(
                BackgroundCandidate(
                    asset_id=asset_id,
                    source_family=source_family,
                    source_reference=source_reference,
                    display_name=display_name,
                    selection_mode_prefill="manual",
                    template_assisted=False,
                    warnings=[],
                )
            )
        return out

    return _adapter


def build_default_background_source_adapter_registry() -> BackgroundSourceAdapterRegistry:
    registry = BackgroundSourceAdapterRegistry()
    for source_family in ALLOWED_BACKGROUND_SOURCE_FAMILIES:
        registry.register(source_family, _db_adapter_for_family(source_family))
    return registry
