from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence

_REQUIRED_STRING_FIELDS: tuple[str, ...] = (
    "palette_guidance",
    "typography_rules",
    "text_layout_rules",
    "composition_framing_rules",
    "branding_rules",
    "output_profile_guidance",
    "background_compatibility_guidance",
    "cover_composition_guidance",
)

_REQUIRED_LIST_FIELDS: tuple[str, ...] = (
    "allowed_motifs",
    "banned_motifs",
)

REQUIRED_KEYS: tuple[str, ...] = _REQUIRED_STRING_FIELDS + _REQUIRED_LIST_FIELDS
_OPTIONAL_DEFAULT_BACKGROUND_ASSET_ID = "default_background_asset_id"


@dataclass(frozen=True)
class PayloadValidationResult:
    is_valid: bool
    normalized_payload: Dict[str, Any] | None
    errors: Sequence[Dict[str, str]]


def validate_template_payload(payload: Any) -> PayloadValidationResult:
    errors: List[Dict[str, str]] = []
    if not isinstance(payload, dict):
        errors.append({"code": "CVST_PAYLOAD_TYPE", "message": "template_payload must be an object"})
        return PayloadValidationResult(is_valid=False, normalized_payload=None, errors=errors)

    normalized: Dict[str, Any] = {}

    for key in REQUIRED_KEYS:
        if key not in payload:
            errors.append({"code": "CVST_PAYLOAD_REQUIRED_KEY", "message": f"template_payload missing required key: {key}"})

    for key in _REQUIRED_STRING_FIELDS:
        if key not in payload:
            continue
        value = payload.get(key)
        if not isinstance(value, str):
            errors.append({"code": "CVST_PAYLOAD_STRING_TYPE", "message": f"template_payload.{key} must be a string"})
            continue
        trimmed = value.strip()
        if not trimmed:
            errors.append({"code": "CVST_PAYLOAD_STRING_EMPTY", "message": f"template_payload.{key} must be non-empty"})
            continue
        normalized[key] = trimmed

    for key in _REQUIRED_LIST_FIELDS:
        if key not in payload:
            continue
        value = payload.get(key)
        if not isinstance(value, list):
            errors.append({"code": "CVST_PAYLOAD_LIST_TYPE", "message": f"template_payload.{key} must be an array"})
            continue
        normalized_items: List[str] = []
        for idx, item in enumerate(value):
            if not isinstance(item, str):
                errors.append({"code": "CVST_PAYLOAD_LIST_ITEM_TYPE", "message": f"template_payload.{key}[{idx}] must be a string"})
                continue
            trimmed = item.strip()
            if not trimmed:
                errors.append({"code": "CVST_PAYLOAD_LIST_ITEM_EMPTY", "message": f"template_payload.{key}[{idx}] must be non-empty"})
                continue
            normalized_items.append(trimmed)
        normalized[key] = normalized_items

    if _OPTIONAL_DEFAULT_BACKGROUND_ASSET_ID in payload:
        raw_default_background_asset_id = payload.get(_OPTIONAL_DEFAULT_BACKGROUND_ASSET_ID)
        try:
            parsed_default_background_asset_id = int(raw_default_background_asset_id)
        except (TypeError, ValueError):
            errors.append(
                {
                    "code": "CVST_PAYLOAD_DEFAULT_BACKGROUND_ASSET_ID_TYPE",
                    "message": "template_payload.default_background_asset_id must be an integer",
                }
            )
        else:
            if parsed_default_background_asset_id <= 0:
                errors.append(
                    {
                        "code": "CVST_PAYLOAD_DEFAULT_BACKGROUND_ASSET_ID_RANGE",
                        "message": "template_payload.default_background_asset_id must be > 0",
                    }
                )
            else:
                normalized[_OPTIONAL_DEFAULT_BACKGROUND_ASSET_ID] = parsed_default_background_asset_id

    if errors:
        return PayloadValidationResult(is_valid=False, normalized_payload=None, errors=errors)

    return PayloadValidationResult(is_valid=True, normalized_payload=normalized, errors=[])
