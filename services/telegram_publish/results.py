from __future__ import annotations

from typing import Any


def render_publish_action_result(*, telegram_action: str, gateway_result: str, ok: bool, result: dict[str, Any] | None, error: dict[str, Any] | None) -> dict[str, Any]:
    status = "SUCCESS" if ok else ("STALE" if gateway_result == "STALE" else "DENIED" if gateway_result == "DENIED" else "FAILED")
    continuity = {
        "what_happened": status,
        "what_changed": result.get("result", {}).get("publish_state_after") if result else None,
        "what_failed": (error.get("code") if error else None),
        "result_kind": status,
    }
    return {
        "telegram_action": telegram_action,
        "status": status,
        "gateway_result": gateway_result,
        "continuity": continuity,
        "error": error,
        "raw_result": result,
    }
