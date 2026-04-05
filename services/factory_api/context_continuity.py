from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any


_CONTEXT_VERSION = 1
_ALLOWED_FILTER_KEYS = {
    "status",
    "statuses",
    "channel",
    "channel_slug",
    "view",
    "time_window",
    "severity",
    "confidence",
    "scope_type",
    "recommendation_family",
    "target_domain",
    "batch_month",
}


@dataclass(frozen=True)
class ContextEnvelope:
    version: int
    current_path: str
    parent_path: str | None
    filters: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "v": self.version,
            "current_path": self.current_path,
            "parent_path": self.parent_path,
            "filters": dict(self.filters),
        }


def _normalized_filters(raw_query: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in raw_query.items():
        if key not in _ALLOWED_FILTER_KEYS:
            continue
        text = str(value or "").strip()
        if not text:
            continue
        normalized[key] = text
    return normalized


def build_context_envelope(*, current_path: str, parent_path: str | None, raw_query: dict[str, str]) -> ContextEnvelope:
    return ContextEnvelope(
        version=_CONTEXT_VERSION,
        current_path=str(current_path or "/") or "/",
        parent_path=str(parent_path) if parent_path else None,
        filters=_normalized_filters(raw_query),
    )


def encode_context_token(envelope: ContextEnvelope) -> str:
    payload = json.dumps(envelope.as_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def decode_context_token(token: str) -> ContextEnvelope | None:
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("v") != _CONTEXT_VERSION:
        return None
    current_path = str(payload.get("current_path") or "").strip()
    if not current_path.startswith("/"):
        return None
    parent_path_raw = payload.get("parent_path")
    parent_path = str(parent_path_raw).strip() if parent_path_raw else None
    if parent_path is not None and not parent_path.startswith("/"):
        return None
    raw_filters = payload.get("filters")
    if not isinstance(raw_filters, dict):
        return None
    filters = _normalized_filters({str(k): str(v) for k, v in raw_filters.items()})
    return ContextEnvelope(version=_CONTEXT_VERSION, current_path=current_path, parent_path=parent_path, filters=filters)


def resolve_incoming_context(*, token: str | None, known_paths: set[str]) -> ContextEnvelope | None:
    if not token:
        return None
    envelope = decode_context_token(token)
    if envelope is None:
        return None
    if envelope.current_path not in known_paths:
        return None
    if envelope.parent_path and envelope.parent_path not in known_paths:
        return None
    return envelope
