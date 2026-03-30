from __future__ import annotations

from typing import Any, Mapping

VISUAL_LIFECYCLE_INTENT_CONFIG = "intent_config"
VISUAL_LIFECYCLE_PREVIEW_SNAPSHOT = "preview_snapshot"
VISUAL_LIFECYCLE_APPROVED_PREVIEW = "approved_preview"
VISUAL_LIFECYCLE_APPLIED_PACKAGE = "applied_package"

_APPLIED_PACKAGE_KEYS: frozenset[str] = frozenset({"background_asset_id", "cover_asset_id"})


def validate_applied_package_shape(package: Mapping[str, Any]) -> None:
    keys = set(package.keys())
    if "background_asset_id" not in keys:
        raise ValueError("background_asset_id is required")
    if "cover_asset_id" not in keys:
        raise ValueError("cover_asset_id is required")
    if any("thumbnail" in key.lower() for key in keys):
        raise ValueError("thumbnail fields are not allowed")
    if keys != _APPLIED_PACKAGE_KEYS:
        extra = sorted(keys - _APPLIED_PACKAGE_KEYS)
        raise ValueError(f"unexpected applied package keys: {extra}")

