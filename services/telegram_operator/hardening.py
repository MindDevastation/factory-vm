from __future__ import annotations

import hashlib
import json
from typing import Any


def build_idempotency_fingerprint(*, action_type: str, target_entity_type: str, target_entity_ref: str, request_id: str) -> str:
    base = {
        "action_type": action_type,
        "target_entity_type": target_entity_type,
        "target_entity_ref": target_entity_ref,
        "request_id": request_id,
    }
    return hashlib.sha256(json.dumps(base, sort_keys=True).encode("utf-8")).hexdigest()


def build_audit_correlation(*, telegram_user_id: int, chat_id: int, correlation_id: str, action_type: str, result: str) -> dict[str, Any]:
    return {
        "telegram_user_id": int(telegram_user_id),
        "chat_id": int(chat_id),
        "correlation_id": str(correlation_id),
        "action_type": str(action_type),
        "result": str(result),
    }

from datetime import datetime, timezone


def is_callback_expired(*, expires_at: str | None, now: datetime | None = None) -> bool:
    if not expires_at:
        return False
    ref = now or datetime.now(timezone.utc)
    exp = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
    return exp <= ref


def classify_stale_conflict(*, expected_state: str | None, current_state: str | None, already_applied: bool = False) -> str:
    if already_applied:
        return "ALREADY_APPLIED"
    if not expected_state or not current_state:
        return "UNKNOWN"
    if expected_state != current_state:
        return "STALE"
    return "CURRENT"


def render_operator_safe_result(*, code: str, message: str, detail: str | None = None) -> dict[str, str | None]:
    return {"code": code, "message": message, "detail": detail}
