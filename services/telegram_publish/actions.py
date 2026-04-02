from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi.responses import JSONResponse

from services.factory_api.publish_job_actions import execute_publish_job_action
from services.telegram_operator import TelegramActionGateway, build_action_envelope

from .results import render_publish_action_result


_ACTION_POLICY: dict[str, dict[str, Any]] = {
    "approve": {
        "canonical_action_type": "unblock",
        "action_class": "STANDARD_OPERATOR_MUTATE",
        "confirm_required": True,
        "allowed_states": {"policy_blocked", "manual_handoff_pending"},
    },
    "reject": {
        "canonical_action_type": "move_to_manual",
        "action_class": "STANDARD_OPERATOR_MUTATE",
        "confirm_required": True,
        "allowed_states": {"ready_to_publish", "retry_pending", "policy_blocked"},
    },
    "ack_manual_handoff": {
        "canonical_action_type": "acknowledge",
        "action_class": "STANDARD_OPERATOR_MUTATE",
        "confirm_required": False,
        "allowed_states": {"manual_handoff_pending"},
    },
    "confirm_manual_completion": {
        "canonical_action_type": "mark_completed",
        "action_class": "STANDARD_OPERATOR_MUTATE",
        "confirm_required": True,
        "allowed_states": {"manual_handoff_acknowledged"},
    },
}


def map_publish_action_policy(*, telegram_action: str) -> dict[str, Any]:
    policy = _ACTION_POLICY.get(str(telegram_action))
    if policy is None:
        raise ValueError("unsupported publish telegram action")
    return dict(policy)


def compare_publish_staleness(*, expected_publish_state: str | None, current_publish_state: str | None) -> dict[str, Any]:
    expected = str(expected_publish_state or "").strip() or None
    current = str(current_publish_state or "").strip() or None
    if expected is None or current is None:
        return {"result": "UNKNOWN", "expected_publish_state": expected, "current_publish_state": current}
    if expected != current:
        return {"result": "STALE", "expected_publish_state": expected, "current_publish_state": current}
    return {"result": "CURRENT", "expected_publish_state": expected, "current_publish_state": current}


def build_publish_confirmation_payload(*, telegram_action: str, confirm: bool, reason: str, request_id: str) -> dict[str, Any]:
    policy = map_publish_action_policy(telegram_action=telegram_action)
    if policy["confirm_required"] and not bool(confirm):
        raise ValueError("confirmation required")
    if not str(reason or "").strip():
        raise ValueError("reason required")
    if not str(request_id or "").strip():
        raise ValueError("request_id required")
    return {"confirm": bool(confirm), "reason": str(reason).strip(), "request_id": str(request_id).strip()}


def _load_job_state(conn: Any, *, job_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT id, publish_state FROM jobs WHERE id = ?", (int(job_id),)).fetchone()
    return dict(row) if row else None


def route_publish_action_via_gateway(
    conn: Any,
    *,
    telegram_user_id: int,
    chat_id: int,
    thread_id: int | None,
    telegram_action: str,
    job_id: int,
    expected_publish_state: str | None,
    confirm: bool,
    reason: str,
    request_id: str,
    correlation_id: str,
    actual_published_at: str | None = None,
    video_id: str | None = None,
    url: str | None = None,
) -> dict[str, Any]:
    policy = map_publish_action_policy(telegram_action=telegram_action)
    envelope = build_action_envelope(
        action_transport_type="CALLBACK",
        action_transport_id=f"publish:{telegram_action}:{int(job_id)}",
        telegram_user_id=int(telegram_user_id),
        chat_id=int(chat_id),
        thread_id=thread_id,
        action_type=f"PUBLISH_{telegram_action.upper()}",
        action_class=str(policy["action_class"]),
        target_entity_type="publish_job",
        target_entity_ref=str(int(job_id)),
        freshness_context={"expected_publish_state": expected_publish_state, "job_id": int(job_id)},
        correlation_id=str(correlation_id),
        idempotency_key=f"publish:{telegram_action}:{job_id}:{request_id}",
        created_at=datetime.now(timezone.utc).isoformat(),
    )

    gateway = TelegramActionGateway(conn)

    def _target_resolver(_envelope: dict[str, Any]) -> dict[str, Any]:
        row = _load_job_state(conn, job_id=job_id)
        return {"result": "FOUND" if row else "MISSING", "row": row}

    def _stale_hook(_envelope: dict[str, Any]) -> dict[str, Any]:
        row = _load_job_state(conn, job_id=job_id)
        if not row:
            return {"result": "STALE"}
        cmp = compare_publish_staleness(expected_publish_state=expected_publish_state, current_publish_state=str(row.get("publish_state") or ""))
        return {"result": cmp["result"]}

    def _idempotency(_envelope: dict[str, Any]) -> dict[str, Any]:
        return {"result": "OK"}

    gateway_result = gateway.evaluate(envelope, target_resolver=_target_resolver, stale_precheck_hook=_stale_hook, idempotency_hook=_idempotency)
    if not bool(gateway_result.get("allow")):
        return render_publish_action_result(
            telegram_action=telegram_action,
            gateway_result=str(gateway_result.get("gateway_result") or "DENIED"),
            ok=False,
            result=None,
            error=gateway_result.get("error"),
        )

    row = _load_job_state(conn, job_id=job_id)
    if row is None:
        return render_publish_action_result(
            telegram_action=telegram_action,
            gateway_result="DENIED",
            ok=False,
            result=None,
            error={"code": "E6A_TARGET_MISSING", "message": "target missing"},
        )
    if str(row.get("publish_state") or "") not in set(policy["allowed_states"]):
        return render_publish_action_result(
            telegram_action=telegram_action,
            gateway_result="STALE",
            ok=False,
            result=None,
            error={"code": "E6A_TARGET_STALE", "message": "publish state changed"},
        )

    payload = build_publish_confirmation_payload(
        telegram_action=telegram_action,
        confirm=confirm,
        reason=reason,
        request_id=request_id,
    )
    extra_payload: dict[str, Any] = {}
    if policy["canonical_action_type"] == "mark_completed":
        extra_payload = {
            "actual_published_at": actual_published_at,
            "video_id": video_id,
            "url": url,
        }

    out = execute_publish_job_action(
        conn,
        job_id=int(job_id),
        action_type=str(policy["canonical_action_type"]),
        actor=f"telegram:{telegram_user_id}",
        request_id=payload["request_id"],
        reason=payload["reason"],
        extra_payload=extra_payload or None,
    )
    if isinstance(out, JSONResponse):
        return render_publish_action_result(
            telegram_action=telegram_action,
            gateway_result="ALLOWED",
            ok=False,
            result=None,
            error={"code": "E3_ACTION_NOT_ALLOWED", "message": "action failed"},
        )
    return render_publish_action_result(
        telegram_action=telegram_action,
        gateway_result="ALLOWED",
        ok=True,
        result=out,
        error=None,
    )
