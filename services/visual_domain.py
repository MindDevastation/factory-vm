from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Mapping, Sequence

VISUAL_LIFECYCLE_INTENT_CONFIG = "intent_config"
VISUAL_LIFECYCLE_PREVIEW_SNAPSHOT = "preview_snapshot"
VISUAL_LIFECYCLE_APPROVED_PREVIEW = "approved_preview"
VISUAL_LIFECYCLE_APPLIED_PACKAGE = "applied_package"

_APPLIED_PACKAGE_KEYS: frozenset[str] = frozenset({"background_asset_id", "cover_asset_id"})


class VisualLifecycleError(Exception):
    def __init__(self, *, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class VisualApplyTokens:
    preview_id: str
    stale_token: str
    conflict_token: str


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


def preview_identity_from_snapshot(snapshot_row: Mapping[str, Any]) -> str:
    preview_id = str(snapshot_row.get("id") or "").strip()
    if not preview_id:
        raise VisualLifecycleError(code="VISUAL_PREVIEW_REQUIRED", message="Preview snapshot id is required before apply")
    return preview_id


def stale_token_from_snapshot(*, release_id: int, snapshot_row: Mapping[str, Any]) -> str:
    preview_id = preview_identity_from_snapshot(snapshot_row)
    intent_snapshot_json = _normalized_json_text(snapshot_row.get("intent_snapshot_json"))
    preview_package_json = _normalized_json_text(snapshot_row.get("preview_package_json"))
    payload = f"{int(release_id)}|{preview_id}|{intent_snapshot_json}|{preview_package_json}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def conflict_token_from_intent(*, release_id: int, intent_config_json: Any) -> str:
    payload = f"{int(release_id)}|{_normalized_json_text(intent_config_json)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_apply_tokens(
    *,
    release_id: int,
    snapshot_row: Mapping[str, Any],
    current_intent_config_json: Any,
) -> VisualApplyTokens:
    return VisualApplyTokens(
        preview_id=preview_identity_from_snapshot(snapshot_row),
        stale_token=stale_token_from_snapshot(release_id=release_id, snapshot_row=snapshot_row),
        conflict_token=conflict_token_from_intent(release_id=release_id, intent_config_json=current_intent_config_json),
    )


def forbid_apply_without_preview(*, snapshot_row: Mapping[str, Any] | None) -> None:
    if snapshot_row is None:
        raise VisualLifecycleError(
            code="VISUAL_PREVIEW_REQUIRED",
            message="Preview is required before apply for visual package changes",
        )
    preview_identity_from_snapshot(snapshot_row)


def reject_stale_preview_apply(
    *,
    release_id: int,
    snapshot_row: Mapping[str, Any],
    provided_stale_token: str,
) -> None:
    expected_token = stale_token_from_snapshot(release_id=release_id, snapshot_row=snapshot_row)
    if provided_stale_token != expected_token:
        raise VisualLifecycleError(
            code="VISUAL_PREVIEW_STALE",
            message="Preview is stale; regenerate preview before apply",
        )


def reject_conflict_apply(
    *,
    release_id: int,
    current_intent_config_json: Any,
    provided_conflict_token: str,
) -> None:
    expected_token = conflict_token_from_intent(release_id=release_id, intent_config_json=current_intent_config_json)
    if provided_conflict_token != expected_token:
        raise VisualLifecycleError(
            code="VISUAL_APPLY_CONFLICT",
            message="Visual intent/config changed since preview; apply rejected",
        )


def ensure_preview_is_latest_approved(
    *,
    snapshot_row: Mapping[str, Any],
    approved_preview_row: Mapping[str, Any] | None,
) -> None:
    if approved_preview_row is None:
        raise VisualLifecycleError(
            code="VISUAL_APPROVAL_REQUIRED",
            message="Approved preview is required before apply",
        )
    preview_id = preview_identity_from_snapshot(snapshot_row)
    approved_preview_id = str(approved_preview_row.get("preview_id") or "")
    if preview_id != approved_preview_id:
        raise VisualLifecycleError(
            code="VISUAL_PREVIEW_OUTDATED",
            message="Approved preview does not match selected preview snapshot",
        )


def approved_preview_is_distinct_from_applied(
    *,
    approved_preview_row: Mapping[str, Any] | None,
    applied_package_row: Mapping[str, Any] | None,
) -> bool:
    if approved_preview_row is None:
        return True
    if applied_package_row is None:
        return True
    approved_preview_id = str(approved_preview_row.get("preview_id") or "")
    applied_source_preview_id = str(applied_package_row.get("source_preview_id") or "")
    return approved_preview_id != applied_source_preview_id


def preview_snapshot_is_non_live(*, preview_id: str, applied_package_row: Mapping[str, Any] | None) -> bool:
    if applied_package_row is None:
        return True
    return str(applied_package_row.get("source_preview_id") or "") != str(preview_id)


def validate_apply_safety(
    *,
    release_id: int,
    snapshot_row: Mapping[str, Any] | None,
    approved_preview_row: Mapping[str, Any] | None,
    applied_package_row: Mapping[str, Any] | None,
    current_intent_config_json: Any,
    provided_stale_token: str,
    provided_conflict_token: str,
) -> None:
    forbid_apply_without_preview(snapshot_row=snapshot_row)
    assert snapshot_row is not None
    ensure_preview_is_latest_approved(snapshot_row=snapshot_row, approved_preview_row=approved_preview_row)
    reject_stale_preview_apply(
        release_id=release_id,
        snapshot_row=snapshot_row,
        provided_stale_token=provided_stale_token,
    )
    reject_conflict_apply(
        release_id=release_id,
        current_intent_config_json=current_intent_config_json,
        provided_conflict_token=provided_conflict_token,
    )
    if not approved_preview_is_distinct_from_applied(
        approved_preview_row=approved_preview_row,
        applied_package_row=applied_package_row,
    ):
        raise VisualLifecycleError(
            code="VISUAL_ALREADY_APPLIED",
            message="Approved preview is already applied",
        )


def build_visual_package_summary(
    *,
    release_id: int,
    package: Mapping[str, Any],
    template_ref: Mapping[str, Any] | None = None,
    warning_messages: Sequence[str] | None = None,
) -> dict[str, Any]:
    background_asset_id = _as_int_or_none(package.get("background_asset_id"))
    cover_asset_id = _as_int_or_none(package.get("cover_asset_id"))
    operator_overrides = sorted({str(item) for item in package.get("operator_override_fields", [])})
    warnings = [str(item) for item in (warning_messages if warning_messages is not None else package.get("warnings", []))]

    return {
        "release": {"release_id": int(release_id)},
        "background_asset": {"asset_id": background_asset_id},
        "cover_asset": {"asset_id": cover_asset_id},
        "thumbnail_source": {
            "source_kind": "cover_asset",
            "asset_id": cover_asset_id,
        },
        "template_ref": dict(template_ref) if template_ref is not None else None,
        "markers": {
            "is_auto_assisted": bool(package.get("is_auto_assisted", False)),
            "operator_overrides": operator_overrides,
        },
        "warnings": warnings,
    }


def _normalized_json_text(value: Any) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return stripped
            return json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        return ""
    return json.dumps(value if value is not None else {}, sort_keys=True, separators=(",", ":"))


def _as_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
